package anway

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testServer struct {
	addr string
	dir  string
	cmd  *exec.Cmd
	env  []string
}

func serverBinary(t *testing.T) string {
	t.Helper()

	path := os.Getenv("SERVER_BIN")
	if path == "" {
		t.Skip("SERVER_BIN is not set")
	}

	return path
}

func freeAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	return listener.Addr().String()
}

func startServer(t *testing.T, env ...string) *testServer {
	t.Helper()

	server := &testServer{addr: freeAddr(t), dir: t.TempDir(), env: env}

	server.start(t)

	t.Cleanup(func() { server.stop() })

	return server
}

func (s *testServer) start(t *testing.T) {
	t.Helper()

	s.cmd = exec.Command(serverBinary(t))
	s.cmd.Env = append(os.Environ(), "BIND_ADDR="+s.addr, "DATA_DIR="+s.dir, "SHUTDOWN_TIMEOUT=1s")
	s.cmd.Env = append(s.cmd.Env, s.env...)

	if err := s.cmd.Start(); err != nil {
		t.Fatal(err)
	}

	for range 200 {
		conn, err := net.Dial("tcp", s.addr)
		if err == nil {
			conn.Close()

			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("server did not start")
}

func (s *testServer) stop() {
	s.cmd.Process.Kill()
	s.cmd.Wait()
}

func (s *testServer) url() string {
	return "tcp://" + s.addr
}

func open(t *testing.T, serverURL string) *Client {
	t.Helper()

	client, err := Open(serverURL, Options{WID: fmt.Sprintf("test-%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { client.Close() })

	return client
}

func newJob(t *testing.T, jobType string, args ...any) *Job {
	t.Helper()

	job, err := NewJob(jobType, args...)
	if err != nil {
		t.Fatal(err)
	}

	return job
}

func TestC01ConnectWithURLAndEnv(t *testing.T) {
	server := startServer(t)

	t.Setenv("ANWAY_URL", server.url())

	if _, err := open(t, "").Info(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestC02Password(t *testing.T) {
	server := startServer(t, "SERVER_PASSWORD=secret")

	if _, err := open(t, "tcp://:secret@"+server.addr).Info(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := open(t, "tcp://:wrong@"+server.addr).Info(context.Background()); err == nil {
		t.Fatal("a wrong password must fail")
	}
}

func TestC03TLS(t *testing.T) {
	serverBinary(t)

	source := httptest.NewUnstartedServer(nil)
	source.StartTLS()

	certificate := source.TLS.Certificates[0]
	source.Close()

	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)

	if err != nil {
		t.Fatal(err)
	}

	writePEM(t, certFile, "CERTIFICATE", certificate.Certificate[0])
	writePEM(t, keyFile, "PRIVATE KEY", key)

	server := startServer(t, "TLS_CERT_FILE="+certFile, "TLS_KEY_FILE="+keyFile)
	roots := x509.NewCertPool()
	parsed, err := x509.ParseCertificate(certificate.Certificate[0])

	if err != nil {
		t.Fatal(err)
	}

	roots.AddCert(parsed)

	options := Options{TLS: &tls.Config{RootCAs: roots, ServerName: "example.com"}}

	client, err := Open("tcp+tls://"+server.addr, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Info(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func writePEM(t *testing.T, path, kind string, raw []byte) {
	t.Helper()

	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: raw}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestC04PushFetchAck(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "email", "a@b.com", 42)); err != nil {
		t.Fatal(err)
	}

	job, err := client.Fetch(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}

	var (
		address string
		count   int
	)

	if err := job.Bind(&address, &count); err != nil {
		t.Fatal(err)
	}

	if job.Type != "email" || address != "a@b.com" || count != 42 {
		t.Fatalf("unexpected job: %+v", job)
	}

	if err := client.Ack(ctx, job.JID); err != nil {
		t.Fatal(err)
	}
}

func TestC05EmptyFetch(t *testing.T) {
	job, err := open(t, startServer(t).url()).Fetch(context.Background(), "default")
	if job != nil || err != nil {
		t.Fatalf("unexpected result: %+v %v", job, err)
	}
}

func TestC06Fail(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()
	pushed := newJob(t, "x")

	if err := client.Push(ctx, pushed); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Fetch(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	if err := client.Fail(ctx, pushed.JID, "Boom", "failed", nil); err != nil {
		t.Fatal(err)
	}

	if err := client.Mutate(ctx, "requeue", "retries", Filter{}); err != nil {
		t.Fatal(err)
	}

	job, err := client.Fetch(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}

	if job.Failure == nil || job.Failure.ErrType != "Boom" || job.Failure.Message != "failed" {
		t.Fatalf("unexpected failure: %+v", job.Failure)
	}
}

func TestC07NotUnique(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x", 1).SetCustom("unique_for", 60)); err != nil {
		t.Fatal(err)
	}

	err := client.Push(ctx, newJob(t, "x", 1).SetCustom("unique_for", 60))
	if !errors.Is(err, ErrNotUnique) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestC08Scheduled(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x").RunAt(time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}

	if job, err := client.Fetch(ctx, "default"); job != nil || err != nil {
		t.Fatalf("unexpected result: %+v %v", job, err)
	}
}

func TestC09PauseResume(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x")); err != nil {
		t.Fatal(err)
	}

	if err := client.PauseQueues(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	if job, _ := client.Fetch(ctx, "default"); job != nil {
		t.Fatal("a paused queue must not send jobs")
	}

	if err := client.ResumeQueues(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	if job, _ := client.Fetch(ctx, "default"); job == nil {
		t.Fatal("expected a job")
	}
}

func TestC10MutateRequeue(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	for _, kind := range []string{"a", "b"} {
		pushed := newJob(t, kind)

		if err := client.Push(ctx, pushed); err != nil {
			t.Fatal(err)
		}

		if _, err := client.Fetch(ctx, "default"); err != nil {
			t.Fatal(err)
		}

		if err := client.Fail(ctx, pushed.JID, "Boom", "failed", nil); err != nil {
			t.Fatal(err)
		}
	}

	if err := client.Mutate(ctx, "requeue", "retries", Filter{JobType: "a"}); err != nil {
		t.Fatal(err)
	}

	if count := retries(t, client); count != 1 {
		t.Fatalf("unexpected retries: %v", count)
	}

	job, err := client.Fetch(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}

	if job == nil || job.Type != "a" {
		t.Fatalf("unexpected job: %+v", job)
	}
}

func TestC11Batch(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	bid, err := client.BatchNew(ctx, Batch{Success: newJob(t, "on-success"), Complete: newJob(t, "on-complete")})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Push(ctx, newJob(t, "member").SetCustom("bid", bid)); err != nil {
		t.Fatal(err)
	}

	if err := client.BatchCommit(ctx, bid); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"member", "on-complete", "on-success"} {
		job, err := client.Fetch(ctx, "default")
		if err != nil {
			t.Fatal(err)
		}

		if job == nil || job.Type != want {
			t.Fatalf("unexpected job, want %s: %+v", want, job)
		}

		if err := client.Ack(ctx, job.JID); err != nil {
			t.Fatal(err)
		}
	}

	status, err := client.BatchStatus(ctx, bid)
	if err != nil {
		t.Fatal(err)
	}

	if status.Total != 1 || status.Pending != 0 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestC12Track(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()
	pushed := newJob(t, "x").SetCustom("track", 1)

	if err := client.Push(ctx, pushed); err != nil {
		t.Fatal(err)
	}

	percent := 40

	if err := client.TrackSet(ctx, TrackUpdate{JID: pushed.JID, Percent: &percent, Desc: "step"}); err != nil {
		t.Fatal(err)
	}

	track, err := client.TrackGet(ctx, pushed.JID)
	if err != nil {
		t.Fatal(err)
	}

	if track.Percent == nil || *track.Percent != 40 || track.Desc != "step" {
		t.Fatalf("unexpected track: %+v", track)
	}
}

func TestC13Info(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x")); err != nil {
		t.Fatal(err)
	}

	sizes, err := client.QueueSizes(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if sizes["default"] != 1 {
		t.Fatalf("unexpected sizes: %v", sizes)
	}
}

func TestC14ConcurrentCallers(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	var group sync.WaitGroup

	for range 100 {
		group.Go(func() {
			if err := client.Push(ctx, newJob(t, "x")); err != nil {
				t.Error(err)
			}
		})
	}

	group.Wait()

	if sizes, err := client.QueueSizes(ctx); err != nil || sizes["default"] != 100 {
		t.Fatalf("unexpected sizes: %v %v", sizes, err)
	}
}

func TestC16ClosedClient(t *testing.T) {
	client := open(t, startServer(t).url())

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	if err := client.Push(context.Background(), newJob(t, "x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestC15Reconnect(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x")); err != nil {
		t.Fatal(err)
	}

	server.stop()
	server.start(t)

	var err error

	for range 3 {
		if err = client.Push(ctx, newJob(t, "x")); err == nil {
			break
		}
	}

	if err != nil {
		t.Fatal(err)
	}

	if sizes, err := client.QueueSizes(ctx); err != nil || sizes["default"] != 2 {
		t.Fatalf("unexpected sizes: %v %v", sizes, err)
	}
}

func runWorker(t *testing.T, worker *Worker) (context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- worker.Run(ctx) }()

	t.Cleanup(cancel)

	return cancel, done
}

func waitFor(t *testing.T, check func() bool) {
	t.Helper()

	for range 500 {
		if check() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not met")
}

func retries(t *testing.T, client *Client) float64 {
	t.Helper()

	info, err := client.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	tasks := info["faktory"].(map[string]any)["tasks"].(map[string]any)

	return tasks["Retries"].(map[string]any)["size"].(float64)
}

func processed(t *testing.T, client *Client) float64 {
	t.Helper()

	info, err := client.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	return info["faktory"].(map[string]any)["total_processed"].(float64)
}

func TestW01RunsAndAcks(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 2})

	worker.Register("x", func(context.Context, *Job) error { return nil })
	runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "x")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return processed(t, client) == 1 })
}

func TestW02W03W04FailureKinds(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1})

	worker.Register("error", func(context.Context, *Job) error { return errors.New("failed") })
	worker.Register("panic", func(context.Context, *Job) error { panic("boom") })
	worker.Register("ok", func(context.Context, *Job) error { return nil })
	runWorker(t, worker)

	for _, kind := range []string{"error", "panic", "unknown", "ok"} {
		if err := client.Push(context.Background(), newJob(t, kind)); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, func() bool { return retries(t, client) == 3 && processed(t, client) == 1 })
}

func TestW05MiddlewareOrder(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1})
	result := make(chan string, 1)

	wrap := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx context.Context, job *Job) error {
				return next(context.WithValue(ctx, contextKey(name), ctxOrder(ctx)+name), job)
			}
		}
	}

	worker.Use(wrap("a"), wrap("b"))

	worker.Register("x", func(ctx context.Context, _ *Job) error {
		result <- ctx.Value(contextKey("b")).(string)

		return nil
	})

	runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "x")); err != nil {
		t.Fatal(err)
	}

	if order := <-result; order != "ab" {
		t.Fatalf("unexpected order: %s", order)
	}
}

type contextKey string

func ctxOrder(ctx context.Context) string {
	value, _ := ctx.Value(contextKey("a")).(string)

	return value
}

func TestW06ServerTerminateFailsUnfinishedJobs(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{
		Concurrency:     1,
		BeatInterval:    20 * time.Millisecond,
		ShutdownTimeout: 50 * time.Millisecond,
		CancelGrace:     100 * time.Millisecond,
	})
	started := make(chan struct{})

	worker.Register("slow", func(ctx context.Context, _ *Job) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})

	_, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "slow")); err != nil {
		t.Fatal(err)
	}

	<-started

	if err := server.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}

	waitDone(t, done)

	server.stop()
	server.start(t)

	failure := requeuedFailure(t, server)
	if !strings.Contains(failure.Message, "context canceled") {
		t.Fatalf("unexpected failure: %+v", failure)
	}
}

// requeuedFailure reads the failure of the one job in retries.
func requeuedFailure(t *testing.T, server *testServer) *Failure {
	t.Helper()

	client := open(t, server.url())
	ctx := context.Background()

	if count := retries(t, client); count != 1 {
		t.Fatalf("unexpected retries: %v", count)
	}

	if err := client.Mutate(ctx, "requeue", "retries", Filter{}); err != nil {
		t.Fatal(err)
	}

	job, err := client.Fetch(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}

	if job == nil || job.Failure == nil {
		t.Fatalf("unexpected job: %+v", job)
	}

	return job.Failure
}

func waitDone(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not stop")
	}
}

func TestW07LocalStopLetsJobsFinish(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1})
	started := make(chan struct{})

	var finished atomic.Bool

	worker.Register("slow", func(context.Context, *Job) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		finished.Store(true)

		return nil
	})

	cancel, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "slow")); err != nil {
		t.Fatal(err)
	}

	<-started
	cancel()

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if !finished.Load() || processed(t, client) != 1 {
		t.Fatal("the job must finish and ack")
	}
}

// scriptedServer replies quiet to beats 2 to 20, and then terminate.
type scriptedServer struct {
	beats        atomic.Int64
	quietFetches atomic.Int64
}

const lastQuietBeat = 20

func (s *scriptedServer) serve(conn net.Conn) {
	defer conn.Close()

	io.WriteString(conn, "+HI {\"v\":2}\r\n")

	lines := bufio.NewScanner(conn)

	for lines.Scan() {
		verb, _, _ := strings.Cut(lines.Text(), " ")

		switch verb {
		case "FETCH":
			if s.beats.Load() > 1 {
				s.quietFetches.Add(1)
			}

			time.Sleep(5 * time.Millisecond)
			io.WriteString(conn, "$-1\r\n")
		case "BEAT":
			io.WriteString(conn, s.beatReply(s.beats.Add(1)))
		default:
			io.WriteString(conn, "+OK\r\n")
		}
	}
}

func (s *scriptedServer) beatReply(count int64) string {
	state := "quiet"

	if count == 1 {
		return "+OK\r\n"
	}

	if count > lastQuietBeat {
		state = "terminate"
	}

	body := fmt.Sprintf(`{"state":%q}`, state)

	return fmt.Sprintf("$%d\r\n%s\r\n", len(body), body)
}

func TestW08QuietKeepsWorkerAlive(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	script := &scriptedServer{}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go script.serve(conn)
		}
	}()

	worker := NewWorker("tcp://"+listener.Addr().String(), WorkerOptions{
		Concurrency:  1,
		BeatInterval: 50 * time.Millisecond,
	})

	_, done := runWorker(t, worker)

	waitDone(t, done)

	if count := script.beats.Load(); count <= lastQuietBeat {
		t.Fatalf("the worker stopped after %d beats, before terminate", count)
	}

	if count := script.quietFetches.Load(); count > 2 {
		t.Fatalf("a quiet worker sent %d fetches", count)
	}
}

func TestW09ShutdownTimeoutCancelsJobs(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1, ShutdownTimeout: 50 * time.Millisecond})
	started := make(chan struct{})

	worker.Register("slow", func(ctx context.Context, _ *Job) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})

	cancel, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "slow")); err != nil {
		t.Fatal(err)
	}

	<-started
	cancel()
	waitDone(t, done)

	if count := retries(t, client); count != 1 {
		t.Fatalf("unexpected retries: %v", count)
	}
}

func TestW10HandlerThatIgnoresContextCannotBlockShutdown(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{
		Concurrency:     1,
		ShutdownTimeout: 50 * time.Millisecond,
		CancelGrace:     50 * time.Millisecond,
	})
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})

	worker.Register("stuck", func(context.Context, *Job) error {
		defer close(returned)

		close(started)
		<-release

		return nil
	})

	cancel, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "stuck")); err != nil {
		t.Fatal(err)
	}

	<-started
	cancel()
	waitDone(t, done)

	close(release)
	<-returned
	time.Sleep(100 * time.Millisecond)

	if count := retries(t, client); count != 1 {
		t.Fatalf("unexpected retries: %v", count)
	}

	if count := processed(t, client); count != 0 {
		t.Fatalf("a late return must not ack: %v", count)
	}
}
