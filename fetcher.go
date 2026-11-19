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
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Defaults applied when the corresponding FetcherConfig field is left at its
// zero value.
const (
	defaultMaxConcurrent          = 8
	defaultMaxConcurrentPerOrigin = 2
	defaultMaxAttempts            = 3
	defaultInitialBackoff         = time.Second
	defaultMaxBackoff             = 30 * time.Second
)

// Sentinel failures reported through FetchResult.Err. Callers can use
// errors.Is to detect them.
var (
	// ErrFetcherClosed is returned for requests that were still queued when
	// the Fetcher was closed.
	ErrFetcherClosed = errors.New("goavro: fetcher has been closed")

	// ErrRetriesExhausted wraps the last encountered failure when every
	// permitted attempt was spent on retryable errors.
	ErrRetriesExhausted = errors.New("goavro: retries exhausted")
)

// FetcherConfig configures a Fetcher. Zero-valued fields are replaced with
// documented defaults.
type FetcherConfig struct {
	// HTTPClient performs the actual HTTP calls. Defaults to http.DefaultClient.
	HTTPClient *http.Client

	// MaxConcurrent bounds the number of HTTP requests in flight at the same
	// time across all origins. Waiting for pacing, cooldown or retry delays
	// does not occupy a slot. Defaults to 8.
	MaxConcurrent int

	// MaxConcurrentPerOrigin bounds the number of simultaneous requests sent
	// to a single origin (scheme://host[:port]). Defaults to 2. Values larger
	// than MaxConcurrent are clamped down to MaxConcurrent.
	MaxConcurrentPerOrigin int

	// MaxAttempts is the maximum number of HTTP attempts made for a single
	// request, including the initial attempt. A value of 1 disables retries.
	// Defaults to 3.
	MaxAttempts int

	// MinInterval is the minimum time between two requests dispatched to the
	// same origin, measured at actual send time. Zero disables pacing.
	MinInterval time.Duration

	// InitialBackoff is the delay applied before the first retry when a
	// retryable response carries no usable Retry-After header. Defaults to
	// one second.
	InitialBackoff time.Duration

	// MaxBackoff caps the exponential backoff (doubled after every failed
	// attempt) used when Retry-After is absent or invalid. Defaults to 30s.
	MaxBackoff time.Duration
}

// FetchRequest describes one Avro GET request. Each request is decoded with
// its own Codec, built from the schema declared by the caller.
type FetchRequest struct {
	// ID identifies the request. It is echoed unchanged on FetchResult.
	ID string

	// URL is the fully qualified HTTP(S) URL to fetch.
	URL string

	// Method optionally overrides the HTTP method. Empty means GET.
	Method string

	// Headers optionally supplies HTTP headers sent with every attempt.
	Headers http.Header

	// Body optionally supplies a request body.
	Body []byte

	// Codec decodes every successful response body according to the Avro
	// schema it was built from. Required.
	Codec *Codec

	// Context optionally bounds this single request. When nil, the context
	// passed to Fetch is used.
	Context context.Context
}

// FetchResult reports the outcome of one FetchRequest.
type FetchResult struct {
	// ID is the identifier copied from the corresponding FetchRequest.
	ID string

	// Native is the fully decoded datum in Go native form, including nil for
	// Avro null. It is nil whenever Err is non-nil.
	Native interface{}

	// Attempts is the number of HTTP attempts that were actually made.
	Attempts int

	// Err is nil on success and otherwise describes a definitive failure.
	Err error
}

// OK reports whether the request succeeded and Native is usable.
func (r FetchResult) OK() bool {
	return r.Err == nil
}

// HTTPStatusError describes an HTTP response with a status code that does not
// indicate success. Retryable status codes (429 and 5xx) are only surfaced
// here once retries are exhausted or, for non-retryable status codes, on the
// first attempt.
type HTTPStatusError struct {
	// StatusCode is the HTTP status code returned by the server.
	StatusCode int

	// Status is the original HTTP status line.
	Status string
}

// Error implements the error interface.
func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("goavro: unexpected HTTP status %d %s", e.StatusCode, e.Status)
}

// Fetcher concurrently fetches Avro documents over HTTP while enforcing a
// global concurrency limit, a per-origin concurrency limit, a per-origin
// minimum send interval and shared per-origin cooldowns.
//
// A Fetcher is safe for concurrent use and is intended to be reused across
// many Fetch calls so that rate-limit state is shared for the lifetime of the
// process.
type Fetcher struct {
	cfg    FetcherConfig
	client *http.Client
	global chan struct{}

	mu      sync.Mutex
	closed  bool
	origins map[string]*originRunner
	wg      sync.WaitGroup
}

// NewFetcher validates the configuration, applies defaults and returns a
// ready-to-use Fetcher.
func NewFetcher(cfg FetcherConfig) (*Fetcher, error) {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = defaultMaxConcurrent
	}
	if cfg.MaxConcurrentPerOrigin <= 0 {
		cfg.MaxConcurrentPerOrigin = defaultMaxConcurrentPerOrigin
	}
	if cfg.MaxConcurrentPerOrigin > cfg.MaxConcurrent {
		cfg.MaxConcurrentPerOrigin = cfg.MaxConcurrent
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultMaxAttempts
	}
	if cfg.MinInterval < 0 {
		cfg.MinInterval = 0
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = defaultInitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		cfg.MaxBackoff = cfg.InitialBackoff
	}

	return &Fetcher{
		cfg:     cfg,
		client:  cfg.HTTPClient,
		global:  make(chan struct{}, cfg.MaxConcurrent),
		origins: make(map[string]*originRunner),
	}, nil
}

// Close stops accepting new requests, fails every request that is still
// queued with ErrFetcherClosed and waits for all in-flight HTTP attempts to
// finish. It is safe to call concurrently with Fetch.
func (f *Fetcher) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return ErrFetcherClosed
	}
	f.closed = true
	runners := make([]*originRunner, 0, len(f.origins))
	for _, r := range f.origins {
		runners = append(runners, r)
	}
	f.origins = nil
	f.mu.Unlock()

	for _, r := range runners {
		close(r.stop)
	}
	f.wg.Wait()
	return nil
}

// Fetch executes every request concurrently under the configured limits and
// returns one result per request, in the same order. A failure of one request
// never affects the others: each result carries its own definitive success or
// failure. The method blocks until every request has completed or ctx is
// canceled; a canceled ctx makes the still-pending requests fail with the
// context's error.
func (f *Fetcher) Fetch(ctx context.Context, requests []*FetchRequest) []FetchResult {
	results := make([]FetchResult, len(requests))

	type pending struct {
		index int
		job   *fetchJob
	}
	type target struct {
		index  int
		origin string
		job    *fetchJob
	}
	targets := make([]target, 0, len(requests))

	// Validate requests and build jobs without holding the lock: none of this
	// touches shared Fetcher state.
	for i, req := range requests {
		if req == nil {
			results[i] = FetchResult{Err: errors.New("goavro: nil FetchRequest")}
			continue
		}
		results[i].ID = req.ID
		if req.Codec == nil {
			results[i].Err = errors.New("goavro: FetchRequest requires a Codec")
			continue
		}
		if req.URL == "" {
			results[i].Err = errors.New("goavro: FetchRequest requires a URL")
			continue
		}
		u, err := url.Parse(req.URL)
		if err != nil || u.Host == "" {
			results[i].Err = fmt.Errorf("goavro: invalid URL %q: %w", req.URL, err)
			continue
		}

		reqCtx := ctx
		if req.Context != nil {
			reqCtx = req.Context
		}
		targets = append(targets, target{
			index:  i,
			origin: u.Scheme + "://" + u.Host,
			job: &fetchJob{
				req:  req,
				ctx:  reqCtx,
				done: make(chan FetchResult, 1),
			},
		})
	}

	// Resolve (and lazily create) the per-origin runners under the lock.
	type routed struct {
		target
		runner *originRunner
	}
	routedJobs := make([]routed, 0, len(targets))

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		for _, t := range targets {
			results[t.index].Err = ErrFetcherClosed
		}
		return results
	}
	for _, t := range targets {
		runner, ok := f.origins[t.origin]
		if !ok {
			runner = &originRunner{
				f:      f,
				origin: t.origin,
				limit:  f.cfg.MaxConcurrentPerOrigin,
				in:     make(chan originMessage, originChanCapacity),
				stop:   make(chan struct{}),
			}
			f.origins[t.origin] = runner
			f.wg.Add(1)
			go runner.loop()
		}
		routedJobs = append(routedJobs, routed{target: t, runner: runner})
	}
	f.mu.Unlock()

	// Enqueue outside the lock so a saturated origin channel can never block
	// Close or another Fetch call.
	waiting := make([]pending, 0, len(routedJobs))
	for _, rj := range routedJobs {
		select {
		case rj.runner.in <- originMessage{job: rj.job}:
			waiting = append(waiting, pending{index: rj.index, job: rj.job})
		case <-rj.runner.stop:
			results[rj.index].Err = ErrFetcherClosed
		case <-rj.job.ctx.Done():
			results[rj.index].Err = rj.job.ctx.Err()
		}
	}

	for _, p := range waiting {
		results[p.index] = <-p.job.done
	}
	return results
}

const originChanCapacity = 64

// fetchJob is the internal representation of a request traveling through an
// origin runner.
type fetchJob struct {
	req       *FetchRequest
	ctx       context.Context
	attempt   int
	confirmed bool
	done      chan FetchResult
}

// originMessage is the only channel type an originRunner consumes. A message
// carries exactly one payload: a newly queued job, a send-time confirmation or
// an attempt outcome.
type originMessage struct {
	job     *fetchJob
	sent    *fetchJob
	outcome *attemptOutcome
}

// attemptOutcome is the report a worker goroutine hands back to its runner.
type attemptOutcome struct {
	job     *fetchJob
	native  interface{}
	failed  error
	retry   bool
	lastErr error
	wait    time.Duration
	hasWait bool
}

// originRunner owns all rate-limit state for one origin and is the single
// goroutine allowed to mutate it. Worker goroutines communicate back through
// the buffered in channel so that no locks are needed on this state.
type originRunner struct {
	f      *Fetcher
	origin string
	in     chan originMessage
	stop   chan struct{}
	limit  int

	queue     []*fetchJob
	inflight  int
	confirmed int

	// lastSend is the actual send time of the most recent confirmed attempt.
	lastSend time.Time
	// cooldown is the absolute time until which no new attempt may be sent.
	// It only ever moves forward, so a late, shorter wait can never shorten a
	// cooldown established by an earlier, longer wait.
	cooldown time.Time

	timer   *time.Timer
	timerOn bool
	closing bool
}

// loop is the runner's event loop. It keeps running until stop is closed and
// every in-flight worker has reported back.
func (r *originRunner) loop() {
	defer r.f.wg.Done()

	r.timer = time.NewTimer(time.Hour)
	defer r.timer.Stop()
	r.timer.Stop()

	for {
		r.pump()

		var timerC <-chan time.Time
		if r.timerOn {
			timerC = r.timer.C
		}
		var headCtx <-chan struct{}
		if len(r.queue) > 0 {
			headCtx = r.queue[0].ctx.Done()
		}

		select {
		case <-r.stop:
			r.shutdown()
			return
		case m := <-r.in:
			r.handle(m)
		case <-timerC:
			r.timerOn = false
		case <-headCtx:
			r.purgeCanceled()
		}
	}
}

// pump dispatches every queued job that is currently allowed to run.
func (r *originRunner) pump() {
	if r.closing {
		return
	}
	for len(r.queue) > 0 && r.inflight < r.limit && r.inflight == r.confirmed {
		now := time.Now()
		readyAt := r.cooldown
		if pacedAt := r.lastSend.Add(r.f.cfg.MinInterval); pacedAt.After(readyAt) {
			readyAt = pacedAt
		}
		if now.Before(readyAt) {
			r.arm(readyAt.Sub(now))
			return
		}

		job := r.queue[0]
		r.queue = r.queue[1:]
		if job.ctx.Err() != nil {
			r.finish(job, nil, job.ctx.Err())
			continue
		}

		job.attempt++
		r.inflight++
		go r.execute(job)
	}
	if len(r.queue) == 0 {
		r.disarm()
	}
}

// handle processes one message from workers or from Fetch.
func (r *originRunner) handle(m originMessage) {
	switch {
	case m.sent != nil:
		// A worker has acquired its global slot and is about to call Do.
		m.sent.confirmed = true
		r.confirmed++
		r.lastSend = time.Now()

	case m.outcome != nil:
		r.inflight--
		if m.outcome.job.confirmed {
			// Balance the send-time confirmation unless the attempt ended
			// before it ever acquired a global slot (context cancellation).
			r.confirmed--
		}
		o := m.outcome
		switch {
		case o.failed != nil || !o.retry:
			r.finish(o.job, o.native, o.failed)
		case o.job.attempt >= r.f.cfg.MaxAttempts:
			r.finish(o.job, nil, fmt.Errorf("%w after %d attempt(s): %v",
				ErrRetriesExhausted, o.job.attempt, o.lastErr))
		default:
			r.bumpCooldown(o)
			r.queue = append(r.queue, o.job)
		}

	case m.job != nil:
		if r.closing {
			r.finish(m.job, nil, ErrFetcherClosed)
			return
		}
		r.queue = append(r.queue, m.job)
	}

	r.pump()
}

// bumpCooldown extends the shared origin cooldown. A valid Retry-After value
// is honored verbatim; otherwise the configured exponential backoff is used.
// The cooldown is a monotonic maximum, so a later, shorter wait never shortens
// an earlier, longer one.
func (r *originRunner) bumpCooldown(o *attemptOutcome) {
	wait := r.f.backoff(o.job.attempt)
	if o.hasWait {
		wait = o.wait
	}
	until := time.Now().Add(wait)
	if until.After(r.cooldown) {
		r.cooldown = until
	}
}

// purgeCanceled fails queued jobs whose context has ended.
func (r *originRunner) purgeCanceled() {
	kept := r.queue[:0]
	for _, job := range r.queue {
		if err := job.ctx.Err(); err != nil {
			r.finish(job, nil, err)
			continue
		}
		kept = append(kept, job)
	}
	r.queue = kept
}

// shutdown fails all queued work with ErrFetcherClosed while letting workers
// that already hold a slot finish their current attempt.
func (r *originRunner) shutdown() {
	r.closing = true
	r.disarm()

	for {
		for _, job := range r.queue {
			r.finish(job, nil, ErrFetcherClosed)
		}
		r.queue = nil

		if r.inflight == 0 {
			// Drain jobs that were buffered before Close flipped the flag.
			for {
				select {
				case m := <-r.in:
					if m.job != nil {
						r.finish(m.job, nil, ErrFetcherClosed)
					}
				default:
					return
				}
			}
		}

		m := <-r.in
		switch {
		case m.sent != nil:
			m.sent.confirmed = true
			r.confirmed++
			r.lastSend = time.Now()
		case m.outcome != nil:
			r.inflight--
			if m.outcome.job.confirmed {
				r.confirmed--
			}
			if m.outcome.failed != nil || !m.outcome.retry {
				r.finish(m.outcome.job, m.outcome.native, m.outcome.failed)
			} else {
				// No further retries after Close: report the last failure.
				r.finish(m.outcome.job, nil, m.outcome.lastErr)
			}
		case m.job != nil:
			r.finish(m.job, nil, ErrFetcherClosed)
		}
	}
}

// execute performs one HTTP attempt on a worker goroutine. The global
// concurrency slot is held only while the request and response read are in
// progress, never while waiting on cooldown or pacing.
func (r *originRunner) execute(job *fetchJob) {
	f := r.f
	o := &attemptOutcome{job: job}

	select {
	case f.global <- struct{}{}:
		defer func() { <-f.global }()
	case <-job.ctx.Done():
		o.failed = job.ctx.Err()
		r.report(o)
		return
	}

	method := job.req.Method
	if method == "" {
		method = http.MethodGet
	}
	var body io.Reader
	if len(job.req.Body) > 0 {
		body = bytes.NewReader(job.req.Body)
	}

	req, err := http.NewRequestWithContext(job.ctx, method, job.req.URL, body)
	if err != nil {
		o.failed = err
		r.report(o)
		return
	}
	if len(job.req.Headers) > 0 {
		req.Header = make(http.Header, len(job.req.Headers))
		for key, values := range job.req.Headers {
			copied := make([]string, len(values))
			copy(copied, values)
			req.Header[key] = copied
		}
	}

	// Record the real send time before Do so pacing reflects wire dispatch,
	// not queueing time. The runner (the sole mutator of origin state) marks
	// the job confirmed and gates the next dispatch on this.
	r.in <- originMessage{sent: job}

	resp, err := f.client.Do(req)
	if err != nil {
		if ctxErr := job.ctx.Err(); ctxErr != nil {
			o.failed = ctxErr
		} else {
			o.retry = true
			o.lastErr = fmt.Errorf("goavro: HTTP request failed: %w", err)
		}
		r.report(o)
		return
	}

	bodyBytes, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		if ctxErr := job.ctx.Err(); ctxErr != nil {
			o.failed = ctxErr
		} else {
			o.retry = true
			o.lastErr = fmt.Errorf("goavro: cannot read response body: %w", readErr)
		}
		r.report(o)
		return
	}
	if closeErr != nil {
		if ctxErr := job.ctx.Err(); ctxErr != nil {
			o.failed = ctxErr
		} else {
			o.retry = true
			o.lastErr = fmt.Errorf("goavro: cannot close response body: %w", closeErr)
		}
		r.report(o)
		return
	}

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		o.retry = true
		o.lastErr = &HTTPStatusError{StatusCode: resp.StatusCode, Status: resp.Status}
		if wait, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
			o.hasWait = true
			o.wait = wait
		}
		r.report(o)
		return
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		o.failed = &HTTPStatusError{StatusCode: resp.StatusCode, Status: resp.Status}
		r.report(o)
		return
	}

	native, remain, decodeErr := job.req.Codec.NativeFromBinary(bodyBytes)
	if decodeErr != nil {
		// A 2xx response whose body is not valid Avro for the declared schema
		// is a definitive failure: retrying identical bytes cannot help and
		// the datum must never be presented as success.
		o.failed = fmt.Errorf("goavro: cannot decode avro response: %w", decodeErr)
		r.report(o)
		return
	}
	if len(remain) > 0 {
		o.failed = fmt.Errorf("goavro: response contains %d undecoded trailing byte(s)", len(remain))
		r.report(o)
		return
	}

	o.native = native
	r.report(o)
}

// report hands the attempt outcome back to the runner. The runner keeps
// draining its channel until all in-flight workers have reported, so this
// send cannot block forever, including during shutdown.
func (r *originRunner) report(o *attemptOutcome) {
	r.in <- originMessage{outcome: o}
}

// finish publishes a terminal result to the job's buffered result channel.
func (r *originRunner) finish(job *fetchJob, native interface{}, err error) {
	job.done <- FetchResult{
		ID:       job.req.ID,
		Native:   native,
		Attempts: job.attempt,
		Err:      err,
	}
}

// arm schedules or reschedules the dispatch timer.
func (r *originRunner) arm(d time.Duration) {
	if r.timerOn && !r.timer.Stop() {
		select {
		case <-r.timer.C:
		default:
		}
	}
	r.timer.Reset(d)
	r.timerOn = true
}

// disarm stops the dispatch timer when nothing is waiting on it.
func (r *originRunner) disarm() {
	if !r.timerOn {
		return
	}
	if !r.timer.Stop() {
		select {
		case <-r.timer.C:
		default:
		}
	}
	r.timerOn = false
}

// backoff returns the delay before the next attempt when Retry-After is
// missing or unusable. The delay after attempt number N is InitialBackoff
// doubled N-1 times, capped at MaxBackoff.
func (f *Fetcher) backoff(attempt int) time.Duration {
	d := f.cfg.InitialBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if d <= 0 || d >= f.cfg.MaxBackoff {
			return f.cfg.MaxBackoff
		}
	}
	if d > f.cfg.MaxBackoff {
		return f.cfg.MaxBackoff
	}
	return d
}

// parseRetryAfter interprets an RFC 7231 Retry-After header, which is either
// a non-negative number of seconds or an HTTP date. The boolean result is
// false when the header is empty or cannot be parsed.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	if t, err := time.Parse(time.RFC1123, value); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	if t, err := time.Parse(time.RFC1123Z, value); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
