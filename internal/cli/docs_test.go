// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"iter"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/lucindo/dynaflux/internal/cli"
)

const (
	// The doc tests read the Markdown files of the repository, whose root is two levels up.
	repoRoot = "../.."
	// guidePath is the guide that TestGettingStarted runs, with DynamoDB Local and the Go
	// toolchain. No test runs the integration blocks of another document.
	guidePath = "docs/guide/getting-started.md"
)

var (
	shellcheck = flag.Bool("shellcheck", false, "check the sh blocks of the README and the guides with shellcheck") //nolint:gochecknoglobals // go test flags are package variables

	directive      = regexp.MustCompile(`^<!-- doctest: (.+?) -->$`)
	shellcheckLine = regexp.MustCompile(`(?m)^-:(\d+):(.*)$`)
	assignment     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)

	// What changes from run to run in an output, which normalize replaces: ULIDs, times and
	// dates, catalog IDs, and the release that registered a catalog.
	ulidText    = regexp.MustCompile(`\b[0-9A-HJKMNP-TV-Z]{26}\b`)
	timeText    = regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?Z)?\b`)
	catalogText = regexp.MustCompile(`catalog [0-9a-f]{12}\b`)
	releaseText = regexp.MustCompile(`\(dynaflux .*?\), registered`)
)

// TestDocs runs the README and the guides in docs/guide: each command in their console and
// sh blocks, in a new project, and compares its output with the lines that a console block
// shows after it. The directives of docBlock prepare the project and mark the blocks
// that run only in TestGettingStarted, or never.
func TestDocs(t *testing.T) {
	t.Parallel()
	for _, path := range testedDocs(t) {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			m := readMarkdown(t, path)
			blocks, err := docBlocks(m)
			if err != nil {
				t.Fatal(err)
			}
			r := docRun{t: t, m: m}
			r.newProject("project")
			for _, b := range blocks {
				r.block(b)
			}
			r.rewrite()
		})
	}
}

// TestDocShell checks the sh blocks of the README and the guides with shellcheck, which a
// machine may lack: make shellcheck runs it, as CI's infra-lint job does.
func TestDocShell(t *testing.T) {
	t.Parallel()
	if !*shellcheck {
		t.Skip("make shellcheck runs this test with -shellcheck")
	}
	bin, err := exec.LookPath("shellcheck")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range testedDocs(t) {
		for _, f := range readMarkdown(t, path).fences() {
			if f.lang != "sh" {
				continue
			}
			cmd := exec.CommandContext(t.Context(), bin, "--shell=bash", "--format=gcc", "-")
			cmd.Stdin = strings.NewReader(strings.Join(f.lines, "\n") + "\n")
			out, runErr := cmd.CombinedOutput()
			if runErr == nil {
				continue
			}
			found := shellcheckLine.FindAllStringSubmatch(string(out), -1)
			if len(found) == 0 {
				t.Errorf("%s:%d: shellcheck: %v\n%s", path, f.line+1, runErr, out)
			}
			for _, m := range found {
				n, _ := strconv.Atoi(m[1])
				t.Errorf("%s:%d:%s", path, f.line+1+n, m[2])
			}
		}
	}
}

// testedDocs returns the documents that TestDocs runs: the README and the guides.
func testedDocs(t *testing.T) []string {
	t.Helper()
	guides, err := fs.Glob(os.DirFS(repoRoot), "docs/guide/*.md")
	if err != nil {
		t.Fatal(err)
	}
	return append([]string{"README.md"}, guides...)
}

// markdown is a Markdown file of the repository.
type markdown struct {
	path  string // relative to the repository root, with slashes
	lines []string
}

func readMarkdown(t *testing.T, path string) markdown {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(path)))
	if err != nil {
		t.Fatal(err)
	}
	return markdown{path, strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")}
}

// fence is a fenced code block.
type fence struct {
	line, end int    // the indexes of its opening and closing lines in the file
	indent    string // the indent of its opening line, which its lines drop
	lang      string // the info string
	lines     []string
}

// fences returns the file's fenced code blocks.
func (m markdown) fences() []fence {
	var (
		fences []fence
		f      fence
		inside bool
	)
	for i, line := range m.lines {
		trimmed := strings.TrimLeft(line, " ")
		switch {
		case !strings.HasPrefix(trimmed, "```"):
			if inside {
				f.lines = append(f.lines, strings.TrimPrefix(line, f.indent))
			}
		case inside:
			f.end = i
			fences = append(fences, f)
			inside = false
		default:
			f = fence{line: i, indent: line[:len(line)-len(trimmed)], lang: strings.TrimSpace(strings.TrimPrefix(trimmed, "```"))}
			inside = true
		}
	}
	return fences
}

// prose returns the lines outside fenced code blocks, with their indexes.
func (m markdown) prose() iter.Seq2[int, string] {
	return func(yield func(int, string) bool) {
		fences := m.fences()
		for i := 0; i < len(m.lines); i++ {
			if len(fences) > 0 && fences[0].line == i {
				i = fences[0].end
				fences = fences[1:]
				continue
			}
			if !yield(i, m.lines[i]) {
				return
			}
		}
	}
}

// docBlock is a fenced block of a tested document, with the directives written before it,
// each as an HTML comment such as <!-- doctest: exit 1 -->:
//
//   - dir NAME: this block and the ones after it run in a new project, NAME.
//   - file PATH: the block is written to PATH in the project.
//   - exit N: the block's first command exits with status N, not 0.
//   - integration: the block runs only in TestGettingStarted, so only getting-started.md
//     may have it.
//   - background: the block's first command starts in the background (integration only).
//   - output any: the output of the block's first command isn't compared.
//   - output normalized: the output of the block's first command is compared with the
//     volatile parts of both replaced.
//   - skip: the block never runs.
//   - go: the Go block is vetted as a package of the example, so that it can import
//     examples/analytics/internal/flux (TestDocGo, in the integration lane).
//   - golden NAME.txtar: each output line of the block, normalized, is a line of
//     testdata/NAME.txtar, normalized; for a skip block that a golden test pins.
//   - same NAME: the block equals every other block marked same NAME (TestDocSame).
type docBlock struct {
	fence

	dir, file, golden, same                                       string
	exit                                                          int
	integration, background, outputAny, normalized, skip, goBlock bool
}

// docBlocks returns the fenced blocks of m with their directives.
func docBlocks(m markdown) ([]docBlock, error) {
	fences := m.fences()
	blocks := make([]docBlock, len(fences))
	for k, f := range fences {
		blocks[k].fence = f
	}
	var errs []error
	k := 0
	for i, line := range m.prose() {
		if !strings.HasPrefix(line, "<!-- doctest:") {
			continue
		}
		for k < len(fences) && fences[k].line < i {
			k++
		}
		var err error
		switch d := directive.FindStringSubmatch(line); {
		case d == nil:
			err = errors.New("write a directive as <!-- doctest: name -->")
		case k == len(fences):
			err = errors.New("no block follows the directive")
		default:
			err = blocks[k].set(d[1])
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s:%d: %w", m.path, i+1, err))
		}
	}
	return blocks, errors.Join(errs...)
}

// set applies the directive d to the block.
func (b *docBlock) set(d string) error {
	switch name, arg, _ := strings.Cut(d, " "); {
	case d == "integration":
		b.integration = true
	case d == "background":
		b.background = true
	case d == "output any":
		b.outputAny = true
	case d == "output normalized":
		b.normalized = true
	case d == "skip":
		b.skip = true
	case d == "go":
		b.goBlock = true
	case name == "dir" && filepath.IsLocal(arg):
		b.dir = arg
	case name == "file" && filepath.IsLocal(arg):
		b.file = arg
	case name == "golden" && filepath.IsLocal(arg) && strings.HasSuffix(arg, ".txtar"):
		b.golden = arg
	case name == "same" && arg != "":
		b.same = arg
	case name == "exit":
		n, err := strconv.Atoi(arg)
		if err != nil {
			return fmt.Errorf("exit %q: want a status", arg)
		}
		b.exit = n
	default:
		return fmt.Errorf("unknown directive %q", d)
	}
	return nil
}

// docCommand is a command of a console or sh block.
type docCommand struct {
	line int    // the index of its first line in the file
	text string // the command, with its continuation lines joined
	// A console block shows the command's output after it: want, lines out to end of the file.
	console  bool
	want     []string
	out, end int
}

// commands returns the commands of a console or sh block. In a console block, a command is
// a line that starts with "$ ", and the lines up to the next one are its output.
func (f fence) commands() []docCommand {
	console := f.lang == "console"
	var cmds []docCommand
	for i := 0; i < len(f.lines); i++ {
		text := f.lines[i]
		if console {
			var ok bool
			if text, ok = strings.CutPrefix(text, "$ "); !ok {
				continue
			}
		}
		c := docCommand{line: f.line + 1 + i, console: console}
		for strings.HasSuffix(text, `\`) && i+1 < len(f.lines) {
			i++
			text = strings.TrimSuffix(text, `\`) + " " + strings.TrimSpace(f.lines[i])
		}
		c.text = text
		first := i + 1
		for console && i+1 < len(f.lines) && !strings.HasPrefix(f.lines[i+1], "$ ") {
			i++
		}
		if console {
			c.want = f.lines[first : i+1]
		}
		c.out, c.end = f.line+1+first, f.line+1+i+1
		cmds = append(cmds, c)
	}
	return cmds
}

// docRun is a run of a tested document in the unit lane.
type docRun struct {
	t     *testing.T
	m     markdown
	dir   string    // the project that commands run in
	edits []docEdit // the outputs that -update rewrites
}

// docEdit replaces lines out to end of a document.
type docEdit struct {
	out, end int
	lines    []string
}

func (r *docRun) newProject(name string) {
	r.dir = filepath.Join(r.t.TempDir(), name)
	if err := os.Mkdir(r.dir, 0o755); err != nil {
		r.t.Fatal(err)
	}
}

func (r *docRun) errorf(line int, format string, args ...any) {
	r.t.Helper()
	r.t.Errorf("%s:%d: %s", r.m.path, line+1, fmt.Sprintf(format, args...))
}

// block runs b. The unit lane runs only the commands that need neither the network, a
// toolchain nor DynamoDB Local; TestGettingStarted runs the rest.
func (r *docRun) block(b docBlock) {
	if b.dir != "" {
		r.newProject(b.dir)
	}
	if b.golden != "" {
		r.checkGolden(b)
	}
	if r.m.path != guidePath && (b.integration || b.background) {
		r.errorf(b.line, "only TestGettingStarted runs integration blocks, of %s: mark the block skip, or add a test that runs it", guidePath)
	}
	if b.skip {
		return
	}
	r.checkNoAWS(b)
	switch {
	case b.integration || b.goBlock:
	case b.file != "":
		path := filepath.Join(r.dir, filepath.FromSlash(b.file))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			r.t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(strings.Join(b.lines, "\n")+"\n"), 0o644); err != nil {
			r.t.Fatal(err)
		}
	case b.background:
		r.errorf(b.line, "background runs only in the integration lane: mark the block integration")
	case b.lang == "console" || b.lang == "sh":
		for k, c := range b.commands() {
			r.command(b, c, k == 0)
		}
	}
}

// checkNoAWS fails on a sam or aws command, which may run only in a skip block.
func (r *docRun) checkNoAWS(b docBlock) {
	if b.lang != "console" && b.lang != "sh" {
		return
	}
	for _, c := range b.commands() {
		for _, sc := range shellCommands(c.text) {
			if sc.words[0] == "sam" || sc.words[0] == "aws" {
				r.errorf(c.line, "%s calls AWS: mark the block skip", sc.words[0])
			}
		}
	}
}

// command runs c, the command of block b that is its first when first, in process.
func (r *docRun) command(b docBlock, c docCommand, first bool) {
	cmds := shellCommands(c.text)
	switch {
	case len(cmds) == 0:
		return
	case len(cmds) > 1 || cmds[0].redirect || len(cmds[0].assigns) > 0 ||
		slices.ContainsFunc(cmds[0].words, func(w string) bool { return strings.Contains(w, "$") }):
		r.errorf(c.line, "%s: the unit lane runs only a plain command: mark the block integration", c.text)
		return
	}
	words := cmds[0].words
	args, ok := unitArgs(words)
	switch {
	case !ok && words[0] == "go":
		r.goCommand(c, words)
		return
	case !ok:
		r.errorf(c.line, "%s: the unit lane runs only dynaflux and go: mark the block integration or skip", c.text)
		return
	case len(args) == 0 || !unitCommand(args[0]):
		r.errorf(c.line, "%s: the unit lane runs only init, validate, explain, generate, version and help: mark the block integration or skip", c.text)
		return
	}
	var out strings.Builder
	status := cli.RunEnv(r.t.Context(), args, strings.NewReader(""), &out, &out, r.dir, cli.Env{Vars: docEnv(), GoModCeiling: r.dir})
	want := 0
	if first {
		want = b.exit
	}
	if status != want {
		r.errorf(c.line, "%s: exit status %d, want %d; output:\n%s", c.text, status, want, out.String())
	}
	got := strings.TrimSuffix(out.String(), "\n")
	if !c.console || first && b.outputAny || sameOutput(got, c.want, first && b.normalized) {
		return
	}
	if *update {
		e := docEdit{out: c.out, end: c.end}
		for line := range strings.SplitSeq(got, "\n") {
			e.lines = append(e.lines, b.indent+line)
		}
		r.edits = append(r.edits, e)
		return
	}
	r.errorf(c.out-1, "%s: got output\n%s\nwant\n%s\n(go test -run TestDocs -update rewrites it)", c.text, got, strings.Join(c.want, "\n"))
}

// sameOutput reports whether got is the output that want shows, with the volatile parts of
// both replaced when normalized.
func sameOutput(got string, want []string, normalized bool) bool {
	w := strings.Join(want, "\n")
	if normalized {
		got, w = normalize(got), normalize(w)
	}
	return got == w
}

// normalize replaces the parts of an output that change from run to run with placeholders,
// for the normalized and golden directives.
func normalize(s string) string {
	s = ulidText.ReplaceAllString(s, "<ulid>")
	s = timeText.ReplaceAllString(s, "<time>")
	s = catalogText.ReplaceAllString(s, "catalog <id>")
	return releaseText.ReplaceAllString(s, "(dynaflux <release>), registered")
}

// checkGolden checks that each output line of b, normalized, is a line of its golden file,
// normalized, so that the golden test keeps the block's output current.
func (r *docRun) checkGolden(b docBlock) {
	data, err := os.ReadFile(filepath.Join("testdata", b.golden))
	if err != nil {
		r.t.Fatal(err)
	}
	golden := map[string]bool{}
	for line := range strings.Lines(normalize(string(data))) {
		golden[strings.TrimSuffix(line, "\n")] = true
	}
	for i, line := range b.lines {
		if line != "" && !strings.HasPrefix(line, "$ ") && !golden[normalize(line)] {
			r.errorf(b.line+1+i, "no line of testdata/%s is this one, with times, IDs and releases normalized:\n%s", b.golden, line)
		}
	}
}

// unitArgs returns the arguments of a dynaflux command that the unit lane runs in process.
func unitArgs(words []string) ([]string, bool) {
	switch {
	case words[0] == "dynaflux":
		return words[1:], true
	case len(words) > 2 && words[0] == "go" && words[1] == "tool" && words[2] == "dynaflux":
		return words[3:], true
	}
	return nil, false
}

func unitCommand(name string) bool {
	switch name {
	case "init", "validate", "explain", "generate", "gen", "version", "help":
		return true
	}
	return false
}

// docEnv is the environment of every command a document runs, which keeps an AWS call on a
// closed local port with fake keys.
func docEnv() map[string]string {
	return map[string]string{
		"AWS_ENDPOINT_URL":      "http://127.0.0.1:9",
		"AWS_ACCESS_KEY_ID":     "fake",
		"AWS_SECRET_ACCESS_KEY": "fake",
	}
}

// Every project of the docs requires Dynaflux at the version Go gives a replaced module that
// no version names, and replaces it with this checkout.
const (
	libraryModule  = "github.com/lucindo/dynaflux"
	libraryVersion = "v0.0.0-00010101000000-000000000000"
)

// goCommand skips a go command, which only TestGettingStarted runs, after writing what go mod
// init and a go get of Dynaflux would write to go.mod there, so that dynaflux reads the same
// go.mod in both lanes. Another document may only set up a project with go.
func (r *docRun) goCommand(c docCommand, words []string) {
	switch {
	case len(words) > 2 && words[1] == "mod" && words[2] == "init":
		r.goModInit(c, words[3:])
	case len(words) > 2 && words[1] == "get":
		r.goGet(c, words[2:])
	case r.m.path == guidePath, len(words) > 2 && words[1] == "mod" && words[2] == "tidy", len(words) > 1 && words[1] == "install":
	default:
		r.errorf(c.line, "%s: no test runs this go command outside %s: mark the block skip, or add a test that runs it", c.text, guidePath)
	}
}

// goModInit writes the go.mod of go mod init, with the replace that TestGettingStarted adds.
func (r *docRun) goModInit(c docCommand, args []string) {
	if len(args) != 1 {
		r.errorf(c.line, "%s: name the module", c.text)
		return
	}
	checkout, err := filepath.Abs(repoRoot)
	if err != nil {
		r.t.Fatal(err)
	}
	data := fmt.Sprintf("module %s\n\ngo 1.26\n\nreplace %s => %s\n", args[0], libraryModule, checkout)
	if err = os.WriteFile(filepath.Join(r.dir, "go.mod"), []byte(data), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

// goGet adds to go.mod the require, and the tool with -tool, of a go get of Dynaflux.
func (r *docRun) goGet(c docCommand, args []string) {
	var lines []string
	for _, a := range args {
		if p, _, _ := strings.Cut(a, "@"); p == libraryModule || strings.HasPrefix(p, libraryModule+"/") {
			lines = append(lines, "require "+libraryModule+" "+libraryVersion)
			if slices.Contains(args, "-tool") {
				lines = append(lines, "tool "+p)
			}
		}
	}
	if len(lines) == 0 {
		return
	}
	gomod := filepath.Join(r.dir, "go.mod")
	data, err := os.ReadFile(gomod)
	if err != nil {
		r.errorf(c.line, "%s: %v: run go mod init first", c.text, err)
		return
	}
	if err = os.WriteFile(gomod, append(data, strings.Join(lines, "\n")+"\n"...), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

// rewrite writes the outputs that -update collected into the document.
func (r *docRun) rewrite() {
	if len(r.edits) == 0 {
		return
	}
	lines := r.m.lines
	for _, e := range slices.Backward(r.edits) {
		lines = slices.Replace(lines, e.out, e.end, e.lines...)
	}
	path := filepath.Join(repoRoot, filepath.FromSlash(r.m.path))
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

// shellCommand is a simple command of a shell line, with its quotes removed and its
// variables and substitutions left as written.
type shellCommand struct {
	assigns  []string // the NAME=value words before the command
	words    []string
	redirect bool // whether it redirects its input or output
}

// shellCommands splits a shell line into its simple commands: at ;, &, |, && and ||, at the
// parentheses of a subshell, and after the keywords of if, while and for. It drops comments
// and redirections, and reads <...> as a word, as the docs write a placeholder.
func shellCommands(line string) []shellCommand {
	s := shellScanner{line: line}
	for i := 0; i < len(line); i++ {
		i = s.next(i)
	}
	s.endCommand()
	return s.cmds
}

type shellScanner struct {
	line   string
	cmds   []shellCommand
	cur    shellCommand
	word   strings.Builder
	inWord bool
	// target is whether the next word is a redirection's target.
	target bool
}

// next scans the token at i and returns the index of its last byte.
func (s *shellScanner) next(i int) int {
	c := s.line[i]
	switch {
	case c == '#' && !s.inWord:
		return len(s.line)
	case c == ' ' || c == '\t':
		s.endWord()
	case strings.IndexByte(";&|()", c) >= 0:
		s.endCommand()
	case c == '<' && placeholderEnd(s.line, i) > 0:
		return s.add(i, placeholderEnd(s.line, i))
	case c == '<' || c == '>':
		return s.redirect(i)
	case c == '\'':
		end := strings.IndexByte(s.line[i+1:], '\'')
		if end < 0 {
			end = len(s.line) - i - 1
		}
		s.add(i+1, i+1+end)
		return i + 1 + end
	case c == '"':
		return s.doubleQuoted(i)
	case strings.HasPrefix(s.line[i:], "$("):
		return s.add(i, substitutionEnd(s.line, i))
	default:
		return s.add(i, i+1)
	}
	return i
}

// add adds bytes i to j of the line to the word, and returns the index of the last.
func (s *shellScanner) add(i, j int) int {
	s.inWord = true
	s.word.WriteString(s.line[i:j])
	return j - 1
}

// doubleQuoted adds the double-quoted string at i to the word, and returns the index of its
// closing quote.
func (s *shellScanner) doubleQuoted(i int) int {
	s.inWord = true
	for i++; i < len(s.line) && s.line[i] != '"'; i++ {
		if s.line[i] == '\\' && i+1 < len(s.line) && strings.IndexByte(`"\$`+"`", s.line[i+1]) >= 0 {
			i++
		}
		s.word.WriteByte(s.line[i])
	}
	return i
}

// redirect drops the redirection at i, with a file descriptor before it, and returns the
// index of its last byte. A target that isn't a descriptor, as in >&2, is the next word.
func (s *shellScanner) redirect(i int) int {
	if w := s.word.String(); s.inWord && strings.Trim(w, "0123456789") == "" {
		s.word.Reset()
		s.inWord = false
	}
	s.endWord()
	s.cur.redirect = true
	j := i + 1
	for j < len(s.line) && strings.IndexByte(">&|", s.line[j]) >= 0 {
		j++
	}
	if s.line[j-1] != '&' {
		s.target = true
		return j - 1
	}
	for j < len(s.line) && strings.IndexByte("0123456789-", s.line[j]) >= 0 {
		j++
	}
	return j - 1
}

func (s *shellScanner) endWord() {
	if !s.inWord {
		return
	}
	w := s.word.String()
	s.word.Reset()
	s.inWord = false
	switch {
	case s.target:
		s.target = false
	case len(s.cur.words) == 0 && assignment.MatchString(w):
		s.cur.assigns = append(s.cur.assigns, w)
	case len(s.cur.words) == 0 && shellKeyword(w):
	default:
		s.cur.words = append(s.cur.words, w)
	}
}

func (s *shellScanner) endCommand() {
	s.endWord()
	if len(s.cur.words) > 0 {
		s.cmds = append(s.cmds, s.cur)
	}
	s.cur = shellCommand{}
}

func shellKeyword(w string) bool {
	switch w {
	case "if", "then", "else", "elif", "fi", "while", "until", "do", "done", "time", "!", "{", "}":
		return true
	}
	return false
}

// substitutionEnd returns the index after the command substitution $(...) at i.
func substitutionEnd(line string, i int) int {
	depth := 0
	for j := i + 1; j < len(line); j++ {
		switch line[j] {
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return j + 1
			}
		}
	}
	return len(line)
}

// placeholderEnd returns the index after a placeholder such as <sandbox account ID> at i:
// a < followed by a letter, up to the next >. It returns -1 when there is none.
func placeholderEnd(line string, i int) int {
	if i+1 == len(line) || !unicode.IsLetter(rune(line[i+1])) {
		return -1
	}
	j := strings.IndexAny(line[i+1:], "<>")
	if j < 0 || line[i+1+j] != '>' {
		return -1
	}
	return i + 1 + j + 1
}
