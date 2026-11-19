// Copyright 2026 LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testSchema = `{
  "type": "record",
  "name": "Msg",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "note", "type": ["null", "string"], "default": null}
  ]
}`

func testCodec(t *testing.T) *Codec {
	t.Helper()
	c, err := NewCodec(testSchema)
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	return c
}

func encodeDatum(t *testing.T, c *Codec, datum interface{}) []byte {
	t.Helper()
	buf, err := c.BinaryFromNative(nil, datum)
	if err != nil {
		t.Fatalf("BinaryFromNative: %v", err)
	}
	return buf
}

func validDatum() map[string]interface{} {
	return map[string]interface{}{"id": int64(42), "note": nil}
}

// scriptedHandler returns configured status codes (and headers) in order,
// then 200 with a valid Avro body once the script is exhausted.
type scriptedHandler struct {
	mu       sync.Mutex
	statuses []int
	retryHdr []string // optional Retry-After per scripted status
	calls    int32
	calledAt []time.Time
	body     []byte
	block    chan struct{} // optional: handler blocks until this closes
}

func (h *scriptedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.calledAt = append(h.calledAt, time.Now())
	idx := int(atomic.AddInt32(&h.calls, 1)) - 1
	var status int
	if idx < len(h.statuses) {
		status = h.statuses[idx]
	} else {
		status = http.StatusOK
	}
	if h.block != nil {
		<-h.block
	}
	if status != http.StatusOK && idx < len(h.retryHdr) && h.retryHdr[idx] != "" {
		w.Header().Set("Retry-After", h.retryHdr[idx])
	}
	h.mu.Unlock()

	w.WriteHeader(status)
	if status == http.StatusOK {
		_, _ = w.Write(h.body)
	}
}

func (h *scriptedHandler) times() []time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]time.Time, len(h.calledAt))
	copy(out, h.calledAt)
	return out
}

func newFetcherForTest(t *testing.T, cfg FetcherConfig) *Fetcher {
	t.Helper()
	f, err := NewFetcher(cfg)
	if err != nil {
		t.Fatalf("NewFetcher: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestFetchSuccessPreservesNativeAndNull(t *testing.T) {
	c := testCodec(t)
	body := encodeDatum(t, c, validDatum())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	f := newFetcherForTest(t, FetcherConfig{})
	res := f.Fetch(context.Background(), []*FetchRequest{{ID: "a", URL: srv.URL, Codec: c}})
	if len(res) != 1 {
		t.Fatalf("got %d results, want 1", len(res))
	}
	r := res[0]
	if !r.OK() {
		t.Fatalf("unexpected failure: %v", r.Err)
	}
	if r.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", r.Attempts)
	}
	native, ok := r.Native.(map[string]interface{})
	if !ok {
		t.Fatalf("native type %T, want map", r.Native)
	}
	if native["id"] != int64(42) {
		t.Errorf("id = %v (%T), want int64(42)", native["id"], native["id"])
	}
	if note, present := native["note"]; !present || note != nil {
		t.Errorf("null union must be preserved as nil; got %#v present=%v", note, present)
	}
}

func TestFetch429RetryAfterSecondsSucceeds(t *testing.T) {
	c := testCodec(t)
	h := &scriptedHandler{
		statuses: []int{429, 429},
		retryHdr: []string{"0", "0"},
		body:     encodeDatum(t, c, validDatum()),
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	f := newFetcherForTest(t, FetcherConfig{MaxAttempts: 3, InitialBackoff: time.Minute})
	res := f.Fetch(context.Background(), []*FetchRequest{{ID: "x", URL: srv.URL, Codec: c}})[0]
	if !res.OK() {
		t.Fatalf("want success after retry, got %v", res.Err)
	}
	if res.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3", res.Attempts)
	}
	if got := atomic.LoadInt32(&h.calls); got != 3 {
		t.Errorf("server calls = %d, want 3", got)
	}
}

func TestFetchExponentialBackoffWhenRetryAfterMissing(t *testing.T) {
	c := testCodec(t)
	h := &scriptedHandler{
		statuses: []int{503, 503}, // no Retry-After headers
		body:     encodeDatum(t, c, validDatum()),
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	const initial = 30 * time.Millisecond
	f := newFetcherForTest(t, FetcherConfig{
		MaxAttempts:    3,
		InitialBackoff: initial,
		MaxBackoff:     time.Second,
	})
	start := time.Now()
	res := f.Fetch(context.Background(), []*FetchRequest{{URL: srv.URL, Codec: c}})[0]
	elapsed := time.Since(start)
	if !res.OK() {
		t.Fatalf("want success, got %v", res.Err)
	}
	// Waits: initial + 2*initial = 3*initial before attempts 2 and 3.
	if elapsed < 3*initial-10*time.Millisecond {
		t.Errorf("elapsed %v, want at least ~%v of backoff", elapsed, 3*initial)
	}
}

func TestBackoffDoublesAndCaps(t *testing.T) {
	f := &Fetcher{cfg: FetcherConfig{InitialBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}}
	want := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		100 * time.Millisecond,
		100 * time.Millisecond,
		100 * time.Millisecond,
	}
	for attempt, w := range want {
		if got := f.backoff(attempt + 1); got != w {
			t.Errorf("backoff(attempt=%d) = %v, want %v", attempt+1, got, w)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Now()
	cases := []struct {
		in      string
		ok      bool
		want    time.Duration
		wantNeg bool
	}{
		{"", false, 0, false},
		{"garbage", false, 0, false},
		{"-5", false, 0, false},
		{"15", true, 15 * time.Second, false},
		{"0", true, 0, false},
		{now.Add(5 * time.Second).UTC().Format(time.RFC1123), true, 0, true},
		{now.Add(-time.Minute).UTC().Format(time.RFC1123), true, 0, false},
	}
	for _, tc := range cases {
		got, ok := parseRetryAfter(tc.in, now)
		if ok != tc.ok {
			t.Errorf("parseRetryAfter(%q) ok = %v, want %v", tc.in, ok, tc.ok)
			continue
		}
		if !ok {
			continue
		}
		if tc.wantNeg {
			if got < 4*time.Second || got > 6*time.Second {
				t.Errorf("date Retry-After = %v, want ~5s", got)
			}
		} else if got != tc.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestSharedOriginCooldownNotShortenedByLateShortWait(t *testing.T) {
	c := testCodec(t)
	body := encodeDatum(t, c, validDatum())

	// Two requests on the SAME origin. First one imposes a long cooldown
	// (150ms); the second then arrives with a much shorter Retry-After (10ms).
	// The shared cooldown must stay at ~150ms.
	var mu sync.Mutex
	var scripts = map[string][]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Req")
		mu.Lock()
		n := scripts[id]
		scripts[id] = append(scripts[id], 1)
		mu.Unlock()
		attempt := len(n) + 1
		if attempt == 1 {
			if id == "long" {
				w.Header().Set("Retry-After", "1")
			} else {
				w.Header().Set("Retry-After", "0")
			}
			w.WriteHeader(429)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	f := newFetcherForTest(t, FetcherConfig{
		MaxConcurrentPerOrigin: 2,
		MaxAttempts:            2,
		InitialBackoff:         time.Second,
	})

	// "long" fails first and establishes a 1s cooldown on the origin.
	longDone := make(chan FetchResult, 1)
	go func() {
		longDone <- f.Fetch(context.Background(), []*FetchRequest{
			{ID: "long", URL: srv.URL, Codec: c, Headers: http.Header{"X-Req": []string{"long"}}},
		})[0]
	}()
	time.Sleep(60 * time.Millisecond)

	// The second request's Retry-After: 0 must NOT shorten the 1s cooldown:
	// its retry still has to wait for the shared cooldown.
	start := time.Now()
	res := f.Fetch(context.Background(), []*FetchRequest{
		{ID: "late", URL: srv.URL, Codec: c, Headers: http.Header{"X-Req": []string{"late"}}},
	})[0]
	waited := time.Since(start)
	if !res.OK() {
		t.Fatalf("late request: %v", res.Err)
	}
	if waited < 900*time.Millisecond {
		t.Errorf("late request waited only %v; shared cooldown was shortened by a shorter Retry-After", waited)
	}
	if longRes := <-longDone; !longRes.OK() {
		t.Errorf("long request should eventually succeed: %v", longRes.Err)
	}
}

func TestOtherOriginsNotBlockedByCooldown(t *testing.T) {
	c := testCodec(t)
	body := encodeDatum(t, c, validDatum())

	slow := &scriptedHandler{statuses: []int{503}, retryHdr: []string{"1"}, body: body}
	fast := &scriptedHandler{body: body}
	srvA := httptest.NewServer(slow)
	srvB := httptest.NewServer(fast)
	defer srvA.Close()
	defer srvB.Close()

	f := newFetcherForTest(t, FetcherConfig{MaxConcurrent: 4, MaxAttempts: 2})
	start := time.Now()
	res := f.Fetch(context.Background(), []*FetchRequest{
		{ID: "A", URL: srvA.URL, Codec: c},
		{ID: "B1", URL: srvB.URL, Codec: c},
		{ID: "B2", URL: srvB.URL, Codec: c},
	})

	byID := map[string]FetchResult{}
	for _, r := range res {
		byID[r.ID] = r
	}
	for _, id := range []string{"A", "B1", "B2"} {
		if !byID[id].OK() {
			t.Fatalf("%s failed: %v", id, byID[id].Err)
		}
	}
	if byID["A"].Attempts != 2 {
		t.Errorf("A attempts = %d, want 2", byID["A"].Attempts)
	}
	// B requests never wait for A's cooldown.
	bTimes := fast.times()
	if len(bTimes) != 2 {
		t.Fatalf("B calls = %d, want 2", len(bTimes))
	}
	if bTimes[1].Sub(bTimes[0]) > 200*time.Millisecond {
		t.Errorf("both B calls should be served immediately, gap = %v", bTimes[1].Sub(bTimes[0]))
	}
	// B requests happen near t=0, well before A's retry at ~1s.
	if bTimes[0].Sub(start) > 300*time.Millisecond {
		t.Errorf("origin B was delayed by origin A cooldown: %v", bTimes[0].Sub(start))
	}
}

func TestRetryWaitDoesNotConsumeGlobalSlot(t *testing.T) {
	c := testCodec(t)
	body := encodeDatum(t, c, validDatum())

	// A occupies the cooldown path on origin A; B must still get global slots.
	aHandler := &scriptedHandler{statuses: []int{429}, retryHdr: []string{"1"}, body: body}
	var bCalls int32
	srvA := httptest.NewServer(aHandler)
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&bCalls, 1)
		_, _ = w.Write(body)
	}))
	defer srvA.Close()
	defer srvB.Close()

	f := newFetcherForTest(t, FetcherConfig{
		MaxConcurrent:          1,
		MaxConcurrentPerOrigin: 1,
		MaxAttempts:            2,
		InitialBackoff:         time.Second,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.Fetch(context.Background(), []*FetchRequest{{ID: "A", URL: srvA.URL, Codec: c}})
	}()

	// Wait until A's first attempt is in its 1s cooldown.
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	res := f.Fetch(context.Background(), []*FetchRequest{
		{ID: "B", URL: srvB.URL, Codec: c},
	})
	if !res[0].OK() {
		t.Fatalf("B failed: %v", res[0].Err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("B waited %v for a global slot held by A's retry sleep", d)
	}
	wg.Wait()
}

func TestPerOriginConcurrencyLimit(t *testing.T) {
	c := testCodec(t)
	body := encodeDatum(t, c, validDatum())

	var current int32
	var maxSeen int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&current, 1)
		for {
			seen := atomic.LoadInt32(&maxSeen)
			if cur <= seen || atomic.CompareAndSwapInt32(&maxSeen, seen, cur) {
				break
			}
		}
		time.Sleep(40 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	f := newFetcherForTest(t, FetcherConfig{
		MaxConcurrent:          8,
		MaxConcurrentPerOrigin: 2,
	})
	reqs := make([]*FetchRequest, 6)
	for i := range reqs {
		reqs[i] = &FetchRequest{ID: fmt.Sprint(i), URL: srv.URL, Codec: c}
	}
	for _, r := range f.Fetch(context.Background(), reqs) {
		if !r.OK() {
			t.Fatalf("req %s: %v", r.ID, r.Err)
		}
	}
	if got := atomic.LoadInt32(&maxSeen); got > 2 {
		t.Errorf("max concurrent same-origin requests = %d, want <= 2", got)
	}
}

func TestGlobalConcurrencyLimit(t *testing.T) {
	c := testCodec(t)
	body := encodeDatum(t, c, validDatum())

	var current int32
	var maxSeen int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&current, 1)
		for {
			seen := atomic.LoadInt32(&maxSeen)
			if cur <= seen || atomic.CompareAndSwapInt32(&maxSeen, seen, cur) {
				break
			}
		}
		time.Sleep(40 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		_, _ = w.Write(body)
	})
	// Different origins via two servers.
	srv1 := httptest.NewServer(mux)
	srv2 := httptest.NewServer(mux)
	defer srv1.Close()
	defer srv2.Close()

	f := newFetcherForTest(t, FetcherConfig{
		MaxConcurrent:          2,
		MaxConcurrentPerOrigin: 4,
	})
	reqs := []*FetchRequest{
		{ID: "1a", URL: srv1.URL, Codec: c},
		{ID: "1b", URL: srv1.URL, Codec: c},
		{ID: "2a", URL: srv2.URL, Codec: c},
		{ID: "2b", URL: srv2.URL, Codec: c},
	}
	for _, r := range f.Fetch(context.Background(), reqs) {
		if !r.OK() {
			t.Fatalf("req %s: %v", r.ID, r.Err)
		}
	}
	if got := atomic.LoadInt32(&maxSeen); got > 2 {
		t.Errorf("max global concurrent requests = %d, want <= 2", got)
	}
}

func TestMinIntervalBetweenSameOriginSends(t *testing.T) {
	c := testCodec(t)
	body := encodeDatum(t, c, validDatum())
	h := &scriptedHandler{body: body}
	srv := httptest.NewServer(h)
	defer srv.Close()

	const interval = 50 * time.Millisecond
	f := newFetcherForTest(t, FetcherConfig{
		MaxConcurrentPerOrigin: 4,
		MinInterval:            interval,
	})
	reqs := make([]*FetchRequest, 4)
	for i := range reqs {
		reqs[i] = &FetchRequest{ID: fmt.Sprint(i), URL: srv.URL, Codec: c}
	}
	start := time.Now()
	for _, r := range f.Fetch(context.Background(), reqs) {
		if !r.OK() {
			t.Fatalf("req %s: %v", r.ID, r.Err)
		}
	}
	total := time.Since(start)

	times := h.times()
	if len(times) != 4 {
		t.Fatalf("calls = %d, want 4", len(times))
	}
	for i := 1; i < len(times); i++ {
		if gap := times[i].Sub(times[i-1]); gap < interval-15*time.Millisecond {
			t.Errorf("gap %d->%d = %v, want >= %v", i-1, i, gap, interval)
		}
	}
	if total < 3*interval-15*time.Millisecond {
		t.Errorf("total %v, want at least 3 intervals (%v)", total, 3*interval)
	}
}

func TestPermanentHTTPErrorNotRetried(t *testing.T) {
	c := testCodec(t)
	h := &scriptedHandler{statuses: []int{http.StatusNotFound, http.StatusBadRequest}}
	srv := httptest.NewServer(h)
	defer srv.Close()

	f := newFetcherForTest(t, FetcherConfig{MaxAttempts: 5})
	res := f.Fetch(context.Background(), []*FetchRequest{{URL: srv.URL, Codec: c}})[0]
	if res.OK() {
		t.Fatal("404 must not be reported as success")
	}
	var statusErr *HTTPStatusError
	if !errors.As(res.Err, &statusErr) || statusErr.StatusCode != 404 {
		t.Fatalf("want HTTPStatusError 404, got %v", res.Err)
	}
	if res.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 (no retries on 404)", res.Attempts)
	}
	if got := atomic.LoadInt32(&h.calls); got != 1 {
		t.Errorf("server calls = %d, want 1", got)
	}
}

func TestRetryExhaustionIsExplicitFailureAndIsolated(t *testing.T) {
	c := testCodec(t)
	bad := &scriptedHandler{statuses: []int{500, 500, 500, 500}}
	good := &scriptedHandler{body: encodeDatum(t, c, validDatum())}
	srvBad := httptest.NewServer(bad)
	srvGood := httptest.NewServer(good)
	defer srvBad.Close()
	defer srvGood.Close()

	f := newFetcherForTest(t, FetcherConfig{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})
	res := f.Fetch(context.Background(), []*FetchRequest{
		{ID: "bad", URL: srvBad.URL, Codec: c},
		{ID: "good", URL: srvGood.URL, Codec: c},
	})
	if res[0].OK() {
		t.Fatal("exhausted retries must not be success")
	}
	if !errors.Is(res[0].Err, ErrRetriesExhausted) {
		t.Fatalf("want ErrRetriesExhausted wrapper, got %v", res[0].Err)
	}
	if res[0].Attempts != 3 {
		t.Errorf("bad Attempts = %d, want 3", res[0].Attempts)
	}
	if !res[1].OK() {
		t.Fatalf("good request dragged down by bad one: %v", res[1].Err)
	}
}

func TestMalformedAvroIsDefinitiveFailure(t *testing.T) {
	c := testCodec(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{0xff, 0xff, 0xff}) // not a valid encoding
	}))
	defer srv.Close()

	f := newFetcherForTest(t, FetcherConfig{MaxAttempts: 4, InitialBackoff: time.Millisecond})
	res := f.Fetch(context.Background(), []*FetchRequest{{URL: srv.URL, Codec: c}})[0]
	if res.OK() || res.Native != nil {
		t.Fatal("malformed avro must not be surfaced as success")
	}
	if res.Attempts != 1 {
		t.Errorf("malformed avro must not be retried; Attempts = %d", res.Attempts)
	}
}

func TestTrailingBytesRejected(t *testing.T) {
	c := testCodec(t)
	body := append(encodeDatum(t, c, validDatum()), 0x00)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	f := newFetcherForTest(t, FetcherConfig{})
	res := f.Fetch(context.Background(), []*FetchRequest{{URL: srv.URL, Codec: c}})[0]
	if res.OK() {
		t.Fatal("response with undecoded trailing bytes must fail")
	}
}

type flakyRoundTripper struct {
	failures int32
	body     []byte
	mu       sync.Mutex
	calls    int
}

func (rt *flakyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.mu.Lock()
	rt.calls++
	shouldFail := rt.calls <= int(atomic.LoadInt32(&rt.failures))
	rt.mu.Unlock()
	if shouldFail {
		return nil, errors.New("simulated connection reset")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(rt.body)),
	}, nil
}

func TestNetworkFailureRetriesThenSucceeds(t *testing.T) {
	c := testCodec(t)
	rt := &flakyRoundTripper{failures: 2, body: encodeDatum(t, c, validDatum())}
	f, err := NewFetcher(FetcherConfig{
		HTTPClient:     &http.Client{Transport: rt},
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	res := f.Fetch(context.Background(), []*FetchRequest{{ID: "net", URL: "http://example.test/data", Codec: c}})[0]
	if !res.OK() {
		t.Fatalf("want recovery after network errors, got %v", res.Err)
	}
	if res.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3", res.Attempts)
	}
}

func TestContextCancellationFailsRequestCleanly(t *testing.T) {
	c := testCodec(t)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	f := newFetcherForTest(t, FetcherConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	res := f.Fetch(context.Background(), []*FetchRequest{{URL: srv.URL, Codec: c, Context: ctx}})[0]
	if res.OK() {
		t.Fatal("canceled request must not succeed")
	}
	if !errors.Is(res.Err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", res.Err)
	}
	if res.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", res.Attempts)
	}
}

func TestCloseFailsQueuedRequestsAndAwaitsInflight(t *testing.T) {
	c := testCodec(t)
	body := encodeDatum(t, c, validDatum())
	release := make(chan struct{})
	var started int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&started, 1)
		<-release
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	f, err := NewFetcher(FetcherConfig{MaxConcurrentPerOrigin: 1})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan FetchResult, 3)
	go func() {
		for _, r := range f.Fetch(context.Background(), []*FetchRequest{
			{ID: "q1", URL: srv.URL, Codec: c},
			{ID: "q2", URL: srv.URL, Codec: c},
			{ID: "q3", URL: srv.URL, Codec: c},
		}) {
			done <- r
		}
	}()

	time.Sleep(80 * time.Millisecond) // let q1 start
	closeErr := make(chan error, 1)
	go func() { closeErr <- f.Close() }()

	// Close must block while q1 is in flight.
	select {
	case err := <-closeErr:
		t.Fatalf("Close returned before in-flight request finished: %v", err)
	case <-time.After(60 * time.Millisecond):
	}
	close(release)

	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after in-flight request finished")
	}

	results := map[string]FetchResult{}
	for i := 0; i < 3; i++ {
		r := <-done
		results[r.ID] = r
	}
	if !results["q1"].OK() {
		t.Errorf("in-flight q1 should finish successfully, got %v", results["q1"].Err)
	}
	for _, id := range []string{"q2", "q3"} {
		if !errors.Is(results[id].Err, ErrFetcherClosed) {
			t.Errorf("queued %s: want ErrFetcherClosed, got %v", id, results[id].Err)
		}
	}

	// Fetch after Close fails immediately.
	post := f.Fetch(context.Background(), []*FetchRequest{{URL: srv.URL, Codec: c}})
	if !errors.Is(post[0].Err, ErrFetcherClosed) {
		t.Errorf("post-close Fetch: want ErrFetcherClosed, got %v", post[0].Err)
	}
}

func TestRequestValidation(t *testing.T) {
	c := testCodec(t)
	f := newFetcherForTest(t, FetcherConfig{})
	res := f.Fetch(context.Background(), []*FetchRequest{
		nil,
		{ID: "nocodec", URL: "http://example.test"},
		{ID: "nourl", Codec: c},
		{ID: "badurl", URL: "http://[:::bad", Codec: c},
	})
	if len(res) != 4 {
		t.Fatalf("got %d results", len(res))
	}
	for i, r := range res {
		if r.OK() {
			t.Errorf("result %d should be a validation failure", i)
		}
	}
}

func TestConcurrentFetchFromManyOrigins(t *testing.T) {
	c := testCodec(t)
	body := encodeDatum(t, c, validDatum())

	var totalCalls int32
	servers := make([]*httptest.Server, 4)
	for i := range servers {
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&totalCalls, 1)
			time.Sleep(time.Duration(2+(int(r.URL.Path[1])%5)) * time.Millisecond)
			_, _ = w.Write(body)
		}))
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	f := newFetcherForTest(t, FetcherConfig{
		MaxConcurrent:          4,
		MaxConcurrentPerOrigin: 2,
		MinInterval:            time.Millisecond,
		MaxAttempts:            2,
	})

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			reqs := make([]*FetchRequest, 12)
			for i := range reqs {
				srv := servers[(g+i)%len(servers)]
				reqs[i] = &FetchRequest{
					ID:    fmt.Sprintf("g%d-r%d", g, i),
					URL:   srv.URL + "/" + fmt.Sprint(i),
					Codec: c,
				}
			}
			for _, r := range f.Fetch(context.Background(), reqs) {
				if !r.OK() {
					t.Errorf("req %s failed: %v", r.ID, r.Err)
				}
				if r.Attempts != 1 {
					t.Errorf("req %s attempts = %d, want 1", r.ID, r.Attempts)
				}
			}
		}(g)
	}
	wg.Wait()

	if got := atomic.LoadInt32(&totalCalls); got != 8*12 {
		t.Errorf("total server calls = %d, want %d", got, 8*12)
	}
}

func TestConfigDefaultsAndClamping(t *testing.T) {
	f, err := NewFetcher(FetcherConfig{MaxConcurrentPerOrigin: 99})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if f.cfg.MaxConcurrent != defaultMaxConcurrent {
		t.Errorf("MaxConcurrent default = %d", f.cfg.MaxConcurrent)
	}
	if f.cfg.MaxConcurrentPerOrigin != defaultMaxConcurrent {
		t.Errorf("per-origin should be clamped to global, got %d", f.cfg.MaxConcurrentPerOrigin)
	}
	if f.cfg.MaxAttempts != defaultMaxAttempts {
		t.Errorf("MaxAttempts default = %d", f.cfg.MaxAttempts)
	}
	if f.cfg.InitialBackoff != defaultInitialBackoff || f.cfg.MaxBackoff != defaultMaxBackoff {
		t.Errorf("backoff defaults wrong: %v/%v", f.cfg.InitialBackoff, f.cfg.MaxBackoff)
	}
}
