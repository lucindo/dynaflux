// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package fluxruntime_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// tlsServer is a TLS stand-in for DynamoDB, which offers HTTP/2 when h2 is set. Its
// listener's receive buffer is 4 KB, so a request it doesn't read waits in the client's
// kernel, not in its own. It notes when each
// request has arrived whole, and how many handlers are still open.
type tlsServer struct {
	ts     *httptest.Server
	resume chan struct{} // closed when the handler may read the request

	mu       sync.Mutex
	reads    int // requests read, whole or not
	arrivals []time.Time
	open     int
}

func newTLSServer(t *testing.T, answer http.HandlerFunc, h2 bool) *tlsServer {
	t.Helper()
	s := &tlsServer{resume: make(chan struct{})}
	s.ts = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-s.resume
		_, err := io.ReadAll(r.Body)
		s.mu.Lock()
		s.reads++
		if err != nil {
			s.mu.Unlock()
			return // the client reset the connection mid-request
		}
		s.arrivals = append(s.arrivals, time.Now())
		s.open++
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.open--
			s.mu.Unlock()
		}()
		answer(w, r)
	}))
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var err error
		if cerr := c.Control(func(fd uintptr) {
			err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4<<10)
		}); cerr != nil {
			return cerr
		}
		return err
	}}
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ts.Listener.Close(); err != nil {
		t.Fatal(err)
	}
	s.ts.Listener = l
	s.ts.EnableHTTP2 = h2
	s.ts.StartTLS()
	t.Cleanup(s.ts.Close)
	return s
}

// client returns c with the server's certificate trusted.
func (s *tlsServer) client(c *awshttp.BuildableClient) *awshttp.BuildableClient {
	pool := s.ts.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	return c.WithTransportOptions(func(tr *http.Transport) { tr.TLSClientConfig.RootCAs = pool })
}

// state returns how many requests the server read, whole or not, when each whole one
// arrived, and how many handlers are open.
func (s *tlsServer) state() (int, []time.Time, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads, slices.Clone(s.arrivals), s.open
}

func throttle(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusBadRequest)
	io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#ThrottlingException","message":"fake"}`)
}

func hang(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }

// Bounds of TestNoRequestAfterDeadline. An attempt may start, or a request arrive, up to
// slack after the deadline: the context's timer can fire late. Put must return within the
// smallest reserve the core keeps, 0.5 s at a 5 s timeout, or the reply comes too late.
const (
	slack      = 50 * time.Millisecond
	minReserve = 500 * time.Millisecond
	// pause is how long after Put returned the server goes on: well past slack, so a request
	// it then reads whole counts as late. settle bounds the wait for it to read.
	pause  = 100 * time.Millisecond
	settle = 5 * time.Second
)

// putResult is what one Put under a deadline did.
type putResult struct {
	calls    []emulatortest.Call
	late     time.Duration // from the deadline to Put's return
	arrivals []time.Time   // after returned + slack
}

// putUntil sends one Put of an item of size bytes through c to srv, under a context that
// ends after timeout. When stall is set, srv reads the request only after Put returned, as a
// server that stopped reading mid-request and then went on. It checks that the call ends
// with the deadline and leaves no attempt in flight.
func putUntil(t *testing.T, srv *tlsServer, c aws.HTTPClient, timeout time.Duration, size int, stall bool) putResult {
	t.Helper()
	if !stall {
		close(srv.resume)
	}
	rec := emulatortest.NewRecorder(c)
	b := dynamo.New(dynamodb.NewFromConfig(aws.Config{
		Region:       emulatortest.Region,
		BaseEndpoint: &srv.ts.URL,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: emulatortest.AccessKeyID, SecretAccessKey: "fake"}, nil
		}),
		HTTPClient: rec,
		// Retries every 10 ms, without a limit the deadline could hide behind.
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.RateLimiter = ratelimit.None
				o.MaxAttempts = 1000
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 10 * time.Millisecond, nil })
			})
		},
	}))
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	str := func(v string) store.Value { return store.Value{Type: store.S, Str: v} }
	it := store.Item{"pk": str("p"), "sk": str("a"), "v": str(strings.Repeat("x", size))}
	err := b.Put(ctx, "t", it, store.Cond{})
	returned := time.Now()
	if n := rec.InFlight(); n != 0 {
		t.Errorf("%d attempts in flight when Put returned", n)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Put = %v, want context.DeadlineExceeded", err)
	}
	res := putResult{calls: rec.Calls(), late: returned.Sub(deadline)}
	for i, c := range res.calls {
		if c.Start.After(deadline.Add(slack)) {
			t.Errorf("attempt %d sent %v after the deadline", i, c.Start.Sub(deadline))
		}
	}
	time.Sleep(pause)
	if stall {
		close(srv.resume)
	}
	// Until the stalled server has read whatever reached it, and each handler has seen its
	// connection end.
	reads, arrivals, open := srv.state()
	for end := time.Now().Add(settle); (stall && reads == 0 || open != 0) && time.Now().Before(end); {
		time.Sleep(10 * time.Millisecond)
		reads, arrivals, open = srv.state()
	}
	if stall && reads == 0 || open != 0 {
		t.Fatalf("%v after Put returned, the server has read %d requests and has %d open", time.Since(returned), reads, open)
	}
	for _, at := range arrivals {
		if at.After(returned.Add(slack)) {
			res.arrivals = append(res.arrivals, at)
		}
	}
	return res
}

// Rerun of the store's TestNoRequestAfterDeadline over TLS, as in production, through the
// runtime's client, whose connections close with SO_LINGER 0. Each case checks how long
// Put takes to return after the deadline, TLS close included, against the core's reserve.
func TestNoRequestAfterDeadline(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		answer  http.HandlerFunc
		timeout time.Duration
		size    int
		stall   bool
		h2      bool
		calls   func(n int) bool
	}{
		{"throttled", throttle, 300 * time.Millisecond, 10, false, false, func(n int) bool { return n >= 2 }},
		{"hung", hang, 200 * time.Millisecond, 10, false, false, func(n int) bool { return n == 1 }},
		{"expired", throttle, -time.Millisecond, 10, false, false, func(n int) bool { return n == 0 }},
		// A request the server stops reading stays in the client's kernel, which by default
		// sends it once the server reads again, long after the deadline.
		{"stalled mid-request", hang, 300 * time.Millisecond, 390 << 10, true, false, func(n int) bool { return n == 1 }},
		// Over HTTP/2, a cancelled request resets only its stream, and the connection stays
		// open with the request in its kernel.
		{"stalled, HTTP/2 offered", hang, 300 * time.Millisecond, 390 << 10, true, true, func(n int) bool { return n == 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newTLSServer(t, tt.answer, tt.h2)
			ddb, _ := sdkOptions(t, "")
			res := putUntil(t, s, s.client(ddb.HTTPClient.(*awshttp.BuildableClient)), tt.timeout, tt.size, tt.stall)
			t.Logf("Put returned %v after the deadline", res.late)
			if !tt.calls(len(res.calls)) {
				t.Errorf("%d attempts", len(res.calls))
			}
			if res.late > minReserve {
				t.Errorf("Put returned %v after the deadline, more than the reserve of %v", res.late, minReserve)
			}
			if len(res.arrivals) > 0 {
				t.Errorf("requests arrived whole %v after Put returned", res.arrivals)
			}
		})
	}
}

// The control of the stalled case: a client with the default linger delivers the request
// after Put returned. If it didn't, the stalled case couldn't tell the two apart.
func TestNoRequestAfterDeadlineControl(t *testing.T) {
	t.Parallel()
	s := newTLSServer(t, hang, false)
	res := putUntil(t, s, s.client(awshttp.NewBuildableClient()), 300*time.Millisecond, 390<<10, true)
	if len(res.arrivals) != 1 {
		t.Errorf("with the default linger, %d requests arrived after Put returned, want 1", len(res.arrivals))
	}
}

// throttled answers every request with ThrottlingException, and counts them.
type throttled struct{ n atomic.Int64 }

func (f *throttled) Do(r *http.Request) (*http.Response, error) {
	if _, err := io.ReadAll(r.Body); err != nil {
		return nil, err
	}
	f.n.Add(1)
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(`{"__type":"com.amazonaws.dynamodb.v20120810#ThrottlingException","message":"fake"}`)),
		Request:    r,
	}, nil
}

// attempts sends calls GetItems, one after another, to a DynamoDB that always throttles,
// and returns the attempts they made.
func attempts(t *testing.T, r aws.Retryer, calls int) int64 {
	t.Helper()
	f := &throttled{}
	c := dynamodb.NewFromConfig(aws.Config{
		Region:       emulatortest.Region,
		BaseEndpoint: new("http://127.0.0.1:1"), // never dialed
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: emulatortest.AccessKeyID, SecretAccessKey: "fake"}, nil
		}),
		HTTPClient: f,
		Retryer:    func() aws.Retryer { return r },
	})
	for range calls {
		_, err := c.GetItem(t.Context(), &dynamodb.GetItemInput{
			TableName: new("t"),
			Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "p"}},
		})
		if err == nil {
			t.Fatal("GetItem succeeded against a DynamoDB that always throttles")
		}
	}
	return f.n.Load()
}

// The SDK's client-side retry quota stops retries for the rest of the process once a burst
// of throttling spends it, and a Lambda environment lives for hours. The runtime's retryer
// has none, so every call gets its 3 attempts.
func TestRetryerHasNoQuota(t *testing.T) {
	t.Parallel()
	ddb, _ := sdkOptions(t, "")
	synctest.Test(t, func(t *testing.T) {
		const calls = 150
		if got := attempts(t, ddb.Retryer, calls); got != 3*calls {
			t.Errorf("%d attempts, want %d", got, 3*calls)
		}
		// The default quota of 500 pays for 100 retries.
		if got := attempts(t, retry.NewStandard(), calls); got != calls+100 {
			t.Errorf("the default retryer made %d attempts, want %d: the control no longer shows the quota", got, calls+100)
		}
	})
}

// Run's clients close each connection with a reset in every defaults mode of the SDK. A mode
// other than legacy makes the service clients rebuild the HTTP client's dialer.
func TestConnectionsReset(t *testing.T) {
	t.Parallel()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	modes := []aws.DefaultsMode{"", aws.DefaultsModeLegacy, aws.DefaultsModeStandard, aws.DefaultsModeInRegion,
		aws.DefaultsModeCrossRegion, aws.DefaultsModeMobile}
	for _, mode := range modes {
		ddb, fho := sdkOptions(t, mode)
		for name, c := range map[string]aws.HTTPClient{"DynamoDB": ddb.HTTPClient, "Firehose": fho.HTTPClient} {
			if got := closeSeen(t, l, c.(*awshttp.BuildableClient)); !errors.Is(got, syscall.ECONNRESET) {
				t.Errorf("mode %q: the server's read of a %s connection that closed ended with %v, want ECONNRESET", mode, name, got)
			}
		}
	}
}

// closeSeen dials l through c's transport, closes the connection, and returns the error of
// the server's read.
func closeSeen(t *testing.T, l net.Listener, c *awshttp.BuildableClient) error {
	t.Helper()
	conn, err := c.GetTransport().DialContext(t.Context(), "tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = server.Read(make([]byte, 1))
	return err
}
