// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration && unix

package cli_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/emulatortest"
)

const (
	// guideEndpoint is the DynamoDB Local that the guide starts, and make integration's.
	guideEndpoint = "http://127.0.0.1:8000"
	// guideTables starts the names of the tables of the guide's store, shop, without a prefix.
	guideTables = "shop-"
	// devReady is the line dev prints on stderr once it has projected every stream.
	devReady = "caught up"
)

// TestGettingStarted follows getting-started.md as a reader does: it runs each command of
// its console and sh blocks, except those marked skip, with bash in a new module, and
// compares the output of each console command with the lines the guide shows after it. The
// module builds against this checkout offline, in Go's default -mod=readonly, so a step the
// guide misses, such as go mod tidy, fails here as it would for a reader. The guide's dev
// runs on make integration's DynamoDB Local.
func TestGettingStarted(t *testing.T) { //nolint:paralleltest // with -update, it and TestDocs would both rewrite the guide
	if !*update {
		t.Parallel()
	}
	if ep := os.Getenv("DYNAFLUX_DYNAMODB_ENDPOINT"); ep != guideEndpoint {
		t.Fatalf("DYNAFLUX_DYNAMODB_ENDPOINT is %q: the guide's commands use DynamoDB Local at %s, as make integration does", ep, guideEndpoint)
	}
	m := readMarkdown(t, guidePath)
	blocks, err := docBlocks(m)
	if err != nil {
		t.Fatal(err)
	}
	ddb := devClient(t)
	deleteGuideTables(t, ddb) // a run before this one may have left them
	t.Cleanup(func() { deleteGuideTables(t, ddb) })
	g := &guideRun{docRun: docRun{t: t, m: m}, env: guideEnv(t)}
	g.newProject("shop")
	defer g.stopBackground()
	for _, b := range blocks {
		g.block(b)
	}
	g.stopBackground()
	g.rewrite()
}

// guideRun is a run of the guide in TestGettingStarted.
type guideRun struct {
	docRun

	env []string
	bg  *background // the command started in the background, until the next one or the end
}

func (g *guideRun) block(b docBlock) {
	switch {
	case b.dir != "":
		g.errorf(b.line, "TestGettingStarted runs the guide in one project: remove the dir directive")
	case b.skip:
	case b.goBlock:
		g.errorf(b.line, "TestGettingStarted compiles the guide's Go files with its go commands: write them with a file directive")
	case b.file != "":
		path := filepath.Join(g.dir, filepath.FromSlash(b.file))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			g.t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(strings.Join(b.lines, "\n")+"\n"), 0o644); err != nil {
			g.t.Fatal(err)
		}
	case b.lang == "console" || b.lang == "sh":
		for k, c := range b.commands() {
			if k == 0 && b.background {
				g.startBackground(c)
				continue
			}
			g.command(b, c, k == 0)
		}
	}
}

// command runs c, the command of block b that is its first when first, and compares its
// exit status and output with the guide's. A query, inspect or status runs again until
// they match or devWait passes, as the guide tells the reader to while dev catches up.
func (g *guideRun) command(b docBlock, c docCommand, first bool) {
	want := 0
	if first {
		want = b.exit
	}
	compare := c.console && (!first || !b.outputAny)
	normalized := first && b.normalized
	out, status := g.run(c.text)
	for deadline := time.Now().Add(devWait); (status != want || compare && !sameOutput(out, c.want, normalized)) &&
		retried(c.text) && time.Now().Before(deadline); {
		time.Sleep(500 * time.Millisecond)
		out, status = g.run(c.text)
	}
	switch {
	case status != want:
		g.t.Fatalf("%s:%d: %s: exit status %d, want %d; the steps after it need it. Output:\n%s", g.m.path, c.line+1, c.text, status, want, out)
	case !compare || sameOutput(out, c.want, normalized):
	case *update:
		e := docEdit{out: c.out, end: c.end}
		for line := range strings.SplitSeq(out, "\n") {
			e.lines = append(e.lines, b.indent+line)
		}
		g.edits = append(g.edits, e)
	default:
		g.errorf(c.out-1, "%s: got output\n%s\nwant\n%s\n(go test -tags integration -run TestGettingStarted -update rewrites it)", c.text, out, strings.Join(c.want, "\n"))
	}
	if strings.HasPrefix(c.text, "go mod init ") {
		g.replaceLibrary()
	}
}

// retried reports whether a command reads what dev projects, so the guide tells the reader
// to run it again when dev hasn't caught up: a query, inspect or status.
func retried(text string) bool {
	cmds := shellCommands(text)
	if len(cmds) != 1 {
		return false
	}
	args, ok := unitArgs(cmds[0].words)
	return ok && len(args) > 0 && slices.Contains([]string{"query", "inspect", "status"}, args[0])
}

// run runs a command of the guide with bash in the project, and returns its standard output
// and error, in the order it wrote them, without the last newline, and its exit status.
func (g *guideRun) run(text string) (string, int) {
	cmd := exec.CommandContext(g.t.Context(), "bash", "-c", text)
	cmd.Dir, cmd.Env = g.dir, g.env
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	got := strings.TrimSuffix(out.String(), "\n")
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return got, exit.ExitCode()
	}
	if err != nil {
		g.t.Fatalf("%s: %v", text, err)
	}
	return got, 0
}

// replaceLibrary makes the module that go mod init started build against this checkout,
// which a reader gets from the module proxy.
func (g *guideRun) replaceLibrary() {
	checkout, err := filepath.Abs(repoRoot)
	if err != nil {
		g.t.Fatal(err)
	}
	if out, status := g.run("go mod edit -replace=" + libraryModule + "=" + checkout); status != 0 {
		g.t.Fatalf("go mod edit: exit status %d:\n%s", status, out)
	}
}

// guideEnv is the environment of the guide's commands: Go with the test's module and build
// caches and its module proxy, a new home so that no other Go or AWS configuration of the
// machine applies, and docEnv's closed AWS endpoint and fake keys, so that a command without
// -endpoint never reaches AWS. The proxy stays on because the guide's go mod tidy also needs
// the test dependencies of the modules it imports, which a fresh cache (as in CI) lacks.
func guideEnv(t *testing.T) []string {
	t.Helper()
	names := []string{"GOCACHE", "GOMODCACHE", "GOPATH", "GOPROXY", "GOSUMDB"}
	out, err := exec.CommandContext(t.Context(), "go", append([]string{"env"}, names...)...).Output()
	if err != nil {
		t.Fatal(err)
	}
	vals := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(vals) != len(names) {
		t.Fatalf("go env: %q", out)
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS="}
	for i, name := range names {
		env = append(env, name+"="+vals[i])
	}
	for k, v := range docEnv() {
		env = append(env, k+"="+v)
	}
	return env
}

// background is a command that runs until the test stops it, as dev does in a terminal of
// its own.
type background struct {
	text string
	cmd  *exec.Cmd
	done chan struct{} // closed when its standard error closes

	mu     sync.Mutex
	stderr strings.Builder
}

func (b *background) output() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stderr.String()
}

// startBackground stops the command started before, as the reader does with Ctrl-C to
// restart dev, then starts c in its own process group and waits for its ready line.
func (g *guideRun) startBackground(c docCommand) {
	g.stopBackground()
	cmd := exec.Command("bash", "-c", c.text) // stopBackground interrupts it, as Ctrl-C does
	cmd.Dir, cmd.Env = g.dir, g.env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		g.t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		g.t.Fatal(err)
	}
	bg := &background{text: c.text, cmd: cmd, done: make(chan struct{})}
	g.bg = bg
	ready := make(chan struct{})
	go func(ready chan<- struct{}) {
		defer close(bg.done)
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			bg.mu.Lock()
			bg.stderr.WriteString(sc.Text() + "\n")
			bg.mu.Unlock()
			if sc.Text() == devReady && ready != nil {
				close(ready)
				ready = nil
			}
		}
	}(ready)
	select {
	case <-ready:
	case <-bg.done:
		g.t.Fatalf("%s:%d: %s stopped before it printed %q:\n%s", g.m.path, c.line+1, c.text, devReady, bg.output())
	case <-time.After(devWait):
		g.t.Fatalf("%s:%d: %s didn't print %q in %v:\n%s", g.m.path, c.line+1, c.text, devReady, devWait, bg.output())
	}
}

// stopBackground interrupts the background command's process group, as Ctrl-C does in its
// terminal, and checks that it stopped with status 0.
func (g *guideRun) stopBackground() {
	bg := g.bg
	if bg == nil {
		return
	}
	g.bg = nil
	if err := syscall.Kill(-bg.cmd.Process.Pid, syscall.SIGINT); err != nil {
		g.t.Errorf("interrupt %s: %v", bg.text, err)
	}
	select {
	case <-bg.done:
	case <-time.After(devWait):
		if err := syscall.Kill(-bg.cmd.Process.Pid, syscall.SIGKILL); err != nil {
			g.t.Errorf("kill %s: %v", bg.text, err)
		}
		<-bg.done
	}
	if err := bg.cmd.Wait(); err != nil {
		g.t.Errorf("%s, interrupted: %v; standard error:\n%s", bg.text, err, bg.output())
	}
}

// devClient returns a DynamoDB client of the database that dev and every -endpoint command
// use: DynamoDB Local keeps one per access key and region.
func devClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	cfg := emulatortest.Config(t, nil)
	cfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: cli.LocalAccessKey, SecretAccessKey: "fake"}, nil
	})
	return dynamodb.NewFromConfig(cfg)
}

// deleteGuideTables deletes the tables of the guide's store, as dev -reset would.
func deleteGuideTables(t *testing.T, ddb *dynamodb.Client) {
	t.Helper()
	ctx := context.WithoutCancel(t.Context()) // it also runs in Cleanup, after the context ends
	var names []string
	for p := dynamodb.NewListTablesPaginator(ddb, &dynamodb.ListTablesInput{}); p.HasMorePages(); {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range page.TableNames {
			if strings.HasPrefix(name, guideTables) {
				names = append(names, name)
			}
		}
	}
	for _, name := range names {
		if _, err := ddb.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: &name}); err != nil {
			t.Fatal(err)
		}
	}
}
