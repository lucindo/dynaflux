// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode"

	"github.com/lucindo/dynaflux/internal/cli"
)

var (
	inlineLink = regexp.MustCompile(`\]\(([^)\s]+)\)`)
	refLink    = regexp.MustCompile(`^\s*\[[^\]]+\]:\s+(\S+)`)
	urlScheme  = regexp.MustCompile(`^[a-z][a-z0-9+.-]*:`)
	heading    = regexp.MustCompile(`^#{1,6}\s+(.*)`)
	headLink   = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)
	dfCode     = regexp.MustCompile(`\bDF\d{4}\b`)
	specRow    = regexp.MustCompile(`(?m)^\| (DF\d{4}) \| (error|warning|retired) \|`)
	cliCode    = regexp.MustCompile(`"(DF7\d{3})"`)
	alarmName  = regexp.MustCompile(`(?m)^\s*AlarmName: (.*)\n\s*AlarmDescription: "?(critical|warning):`)
	alarmPart  = regexp.MustCompile(`\{[^}]*\}|[^{]+`)
	amount     = regexp.MustCompile(`\$\d+(,\d{3})*(\.\d+)?`)
	// price follows an amount that is a price of the cost model, not a result of explain.
	price = regexp.MustCompile(`^ per (GB|TB|million|dashboard|alarm metric)\b`)
)

// TestDocCommandsParse parses every dynaflux command in the repository's Markdown with the
// command's own flags, and checks its operand count: the commands of sh and console blocks,
// and the code spans that start with dynaflux, or with a command name and a flag, such as
// generate -rebuild.
func TestDocCommandsParse(t *testing.T) {
	t.Parallel()
	for _, p := range repoMarkdown(t) {
		for _, c := range docInvocations(readMarkdown(t, p)) {
			if err := lintCommand(c.args, c.span); err != nil {
				t.Errorf("%s:%d: %s: %v", p, c.line+1, c.text, err)
			}
		}
	}
}

// docInvocation is a dynaflux command that a Markdown file writes.
type docInvocation struct {
	line int
	text string
	args []string // the arguments after dynaflux
	span bool     // whether it is a code span
}

// docInvocations returns the dynaflux commands of m.
func docInvocations(m markdown) []docInvocation {
	var found []docInvocation
	for _, f := range m.fences() {
		if f.lang != "sh" && f.lang != "console" {
			continue
		}
		for _, c := range f.commands() {
			for _, sc := range shellCommands(c.text) {
				if args, ok := dynafluxArgs(sc.words); ok {
					found = append(found, docInvocation{c.line, c.text, args, false})
				}
			}
		}
	}
	for i, line := range m.prose() {
		for _, span := range codeSpan.FindAllStringSubmatch(line, -1) {
			if args, ok := spanArgs(span[1]); ok {
				found = append(found, docInvocation{i, span[0], args, true})
			}
		}
	}
	return found
}

// dynafluxArgs returns the arguments of a command that runs dynaflux, as the docs spell it.
func dynafluxArgs(words []string) ([]string, bool) {
	switch {
	case words[0] == "dynaflux" || strings.HasSuffix(words[0], "bin/dynaflux"):
		return words[1:], true
	case len(words) > 2 && words[0] == "go" && words[1] == "tool" && words[2] == "dynaflux":
		return words[3:], true
	case len(words) > 2 && words[0] == "go" && words[1] == "run" && strings.HasSuffix(strings.Split(words[2], "@")[0], "cmd/dynaflux"):
		return words[3:], true
	}
	return nil, false
}

// spanArgs returns the dynaflux arguments of a code span.
func spanArgs(span string) ([]string, bool) {
	cmds := shellCommands(span)
	if len(cmds) == 0 {
		return nil, false
	}
	words := cmds[0].words
	if args, ok := dynafluxArgs(words); ok {
		return args, len(args) > 0
	}
	// ParseCommand accepts a command name alone whenever it names a command.
	if _, _, err := cli.ParseCommand(words[:1]); err == nil && len(words) > 1 && strings.HasPrefix(words[1], "-") {
		return words, true
	}
	return nil, false
}

// lintCommand parses the arguments after dynaflux as Run would. A code span may name a
// command without its operands, or end with a flag without its value.
func lintCommand(args []string, span bool) error {
	if i := slices.IndexFunc(args, func(a string) bool { return strings.HasPrefix(a, "--") }); i >= 0 {
		return fmt.Errorf("%s: write flags with one dash", args[i])
	}
	name, operands, err := cli.ParseCommand(args)
	if span && err != nil && err.Error() == "flag needs an argument: "+args[len(args)-1] {
		name, operands, err = cli.ParseCommand(args[:len(args)-1])
	}
	if err != nil {
		return err
	}
	least, most, ok := operandRange(name)
	switch {
	case !ok:
		return fmt.Errorf("add the operands of %s to operandRange", name)
	case len(operands) < least && !span, most >= 0 && len(operands) > most:
		return fmt.Errorf("%s takes %d to %d operands, not %d: %q", name, least, most, len(operands), operands)
	}
	return nil
}

// operandRange returns the least and the most operands of a command, with -1 for no limit.
func operandRange(name string) (int, int, bool) {
	switch name {
	case "init", "validate", "generate", "diff", "dev", "status", "version":
		return 0, 0, true
	case "explain", "retire", "inspect", "query", "import":
		return 1, 1, true
	case "backfill":
		return 0, -1, true
	case "help":
		return 0, 1, true
	}
	return 0, 0, false
}

// TestDocLinks checks the relative links of the repository's Markdown: each names a file in
// the repository, and an anchor names a heading of the Markdown file, by GitHub's rules.
func TestDocLinks(t *testing.T) {
	t.Parallel()
	ignored := gitIgnored(t)
	anchors := map[string]map[string]bool{}
	for _, p := range repoMarkdown(t) {
		for i, line := range readMarkdown(t, p).prose() {
			line = codeSpan.ReplaceAllString(line, "")
			targets := inlineLink.FindAllStringSubmatch(line, -1)
			if m := refLink.FindStringSubmatch(line); m != nil {
				targets = append(targets, m)
			}
			for _, m := range targets {
				if err := checkLink(t, p, m[1], ignored, anchors); err != "" {
					t.Errorf("%s:%d: %s: %s", p, i+1, m[1], err)
				}
			}
		}
	}
}

// checkLink checks the link target in the Markdown file at p, and returns what is wrong
// with it. anchors caches the anchors of each file.
func checkLink(t *testing.T, p, target string, ignored map[string]bool, anchors map[string]map[string]bool) string {
	t.Helper()
	if urlScheme.MatchString(target) {
		return ""
	}
	file, anchor, _ := strings.Cut(target, "#")
	if file == "" {
		file = p
	} else {
		file = path.Join(path.Dir(p), file)
	}
	top, _, _ := strings.Cut(file, "/")
	switch _, err := os.Stat(filepath.Join(repoRoot, filepath.FromSlash(file))); {
	case top == ".." || ignored[top]:
		return "the target isn't in the repository"
	case err != nil:
		return "no such file"
	case anchor == "" || !strings.HasSuffix(file, ".md"):
		return ""
	}
	if anchors[file] == nil {
		anchors[file] = headingAnchors(readMarkdown(t, file))
	}
	if !anchors[file][anchor] {
		return "no heading of " + file + " has this anchor"
	}
	return ""
}

// headingAnchors returns the anchors that GitHub gives the headings of m.
func headingAnchors(m markdown) map[string]bool {
	anchors := map[string]bool{}
	seen := map[string]int{}
	for _, line := range m.prose() {
		h := heading.FindStringSubmatch(line)
		if h == nil {
			continue
		}
		var b strings.Builder
		for _, r := range headLink.ReplaceAllString(strings.ToLower(strings.TrimSpace(h[1])), "$1") {
			switch {
			case unicode.IsLetter(r), unicode.IsDigit(r), r == '-', r == '_':
				b.WriteRune(r)
			case r == ' ':
				b.WriteByte('-')
			}
		}
		slug := b.String()
		if n := seen[slug]; n > 0 {
			anchors[fmt.Sprintf("%s-%d", slug, n)] = true
		} else {
			anchors[slug] = true
		}
		seen[slug]++
	}
	return anchors
}

// TestDocCodes checks that every diagnostic code in the README and the guides is a current
// code of the specs' tables, and that lifecycle.md's current DF7 codes are the CLI's.
func TestDocCodes(t *testing.T) {
	t.Parallel()
	spec := specCodes(t)
	for _, p := range testedDocs(t) {
		for i, line := range readMarkdown(t, p).lines {
			for _, code := range dfCode.FindAllString(line, -1) {
				switch spec[code] {
				case "":
					t.Errorf("%s:%d: %s isn't in the code tables of the specs", p, i+1, code)
				case "retired":
					t.Errorf("%s:%d: %s is retired", p, i+1, code)
				}
			}
		}
	}
	inCLI := cliCodes(t)
	for _, code := range slices.Sorted(maps.Keys(spec)) {
		if current := strings.HasPrefix(code, "DF7") && spec[code] != "retired"; current != inCLI[code] {
			t.Errorf("%s: a current code of lifecycle.md: %t; reported by the CLI: %t", code, current, inCLI[code])
		}
	}
	for _, code := range slices.Sorted(maps.Keys(inCLI)) {
		if spec[code] == "" {
			t.Errorf("%s: reported by the CLI, and not in lifecycle.md", code)
		}
	}
}

// specCodes returns the codes in the code tables of the specs with their severity column:
// error, warning or retired.
func specCodes(t *testing.T) map[string]string {
	t.Helper()
	codes := map[string]string{}
	for _, p := range []string{"docs/spec/sql.md", "docs/spec/config.md", "docs/spec/lifecycle.md"} {
		for _, m := range specRow.FindAllStringSubmatch(strings.Join(readMarkdown(t, p).lines, "\n"), -1) {
			codes[m[1]] = m[2]
		}
	}
	return codes
}

// cliCodes returns the DF7 codes in the package's source.
func cliCodes(t *testing.T) map[string]bool {
	t.Helper()
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	codes := map[string]bool{}
	for _, src := range sources {
		if strings.HasSuffix(src, "_test.go") {
			continue
		}
		data, readErr := os.ReadFile(src)
		if readErr != nil {
			t.Fatal(readErr)
		}
		for _, m := range cliCode.FindAllStringSubmatch(string(data), -1) {
			codes[m[1]] = true
		}
	}
	return codes
}

// TestDocAmounts checks that each dollar amount in the prose of the README and the guides
// is in the output of one of the document's console blocks that a test runs, so that a
// change to explain's model can't leave the prose behind. A price of the model, such as
// $5 per TB, may stand alone.
func TestDocAmounts(t *testing.T) {
	t.Parallel()
	for _, p := range testedDocs(t) {
		m := readMarkdown(t, p)
		shown := shownAmounts(t, m)
		for i, line := range m.prose() {
			line = codeSpan.ReplaceAllString(line, "")
			for _, loc := range amount.FindAllStringIndex(line, -1) {
				if a := line[loc[0]:loc[1]]; !shown[a] && !price.MatchString(line[loc[1]:]) {
					t.Errorf("%s:%d: %s isn't in the output of a console block that a test runs", p, i+1, a)
				}
			}
		}
	}
}

// shownAmounts returns the dollar amounts in the output of m's console blocks that a test
// runs.
func shownAmounts(t *testing.T, m markdown) map[string]bool {
	t.Helper()
	blocks, err := docBlocks(m)
	if err != nil {
		t.Fatal(err)
	}
	shown := map[string]bool{}
	for _, b := range blocks {
		if b.lang != "console" || b.skip {
			continue
		}
		for _, c := range b.commands() {
			for _, a := range amount.FindAllString(strings.Join(c.want, "\n"), -1) {
				shown[a] = true
			}
		}
	}
	return shown
}

// TestDocSame checks that the blocks marked with one same directive are equal, such as the
// teardown steps that getting-started.md and operations.md share.
func TestDocSame(t *testing.T) {
	t.Parallel()
	type marked struct {
		path string
		b    docBlock
	}
	groups := map[string][]marked{}
	for _, p := range testedDocs(t) {
		blocks, err := docBlocks(readMarkdown(t, p))
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range blocks {
			if b.same != "" {
				groups[b.same] = append(groups[b.same], marked{p, b})
			}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(groups)) {
		g := groups[name]
		if len(g) == 1 {
			t.Errorf("%s:%d: no other block is marked same %s", g[0].path, g[0].b.line+1, name)
		}
		for _, m := range g[1:] {
			if !slices.Equal(m.b.lines, g[0].b.lines) {
				t.Errorf("%s:%d: the block differs from %s:%d, which is also marked same %s", m.path, m.b.line+1, g[0].path, g[0].b.line+1, name)
			}
		}
	}
}

// TestDocAlarms checks the alarm table of infra.md §10 against the alarms of the infra
// goldens, and every alarm table of the README and the guides against infra.md's: each
// golden alarm fits one of its name patterns, and each pattern fits a golden alarm. A
// table's Severity column must match how the golden alarms' AlarmDescription starts.
func TestDocAlarms(t *testing.T) {
	t.Parallel()
	const infraDoc = "docs/spec/infra.md"
	infra := alarmTables(readMarkdown(t, infraDoc))
	if len(infra) != 1 {
		t.Fatalf("infra.md has %d alarm tables, want 1", len(infra))
	}
	severity := alarmKinds(t, alarmPatterns(infra[0]))
	if missing := slices.DeleteFunc(alarmPatterns(infra[0]), func(p string) bool { return severity[p] != "" }); len(missing) > 0 {
		t.Errorf("infra.md lists alarms that no golden has: %q", missing)
	}
	want := slices.Sorted(maps.Keys(severity))
	for _, p := range append([]string{infraDoc}, testedDocs(t)...) {
		for _, table := range alarmTables(readMarkdown(t, p)) {
			if got := slices.Sorted(slices.Values(alarmPatterns(table))); p != infraDoc && !slices.Equal(got, want) {
				t.Errorf("%s: the alarm table lists\n%q\nwant\n%q", p, got, want)
			}
			for _, row := range table {
				if row.severity != "" && severity[row.pattern] != "" && row.severity != severity[row.pattern] {
					t.Errorf("%s: %s is %s, but its AlarmDescription in the templates starts with %s:", p, row.pattern, row.severity, severity[row.pattern])
				}
			}
		}
	}
}

// alarmRow is a row of an alarm table: an alarm name pattern without backquotes, and its
// severity when the table has a Severity column.
type alarmRow struct{ pattern, severity string }

// alarmTables returns the rows of each table of m whose first header is Alarm.
func alarmTables(m markdown) [][]alarmRow {
	var (
		tables [][]alarmRow
		in     bool
		sev    int // the index of the Severity column, or -1
	)
	for _, line := range m.prose() {
		cells := strings.Split(line, "|")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		switch {
		case len(cells) < 3:
			in = false
		case cells[1] == "Alarm":
			tables, in, sev = append(tables, nil), true, slices.Index(cells, "Severity")
		case in && !strings.HasPrefix(cells[1], "---"):
			row := alarmRow{pattern: strings.Trim(cells[1], "`")}
			if sev > 0 && sev < len(cells) {
				row.severity = cells[sev]
			}
			tables[len(tables)-1] = append(tables[len(tables)-1], row)
		}
	}
	return tables
}

func alarmPatterns(table []alarmRow) []string {
	patterns := make([]string, len(table))
	for i, row := range table {
		patterns[i] = row.pattern
	}
	return patterns
}

// alarmKinds returns the name patterns of an alarm table that the alarms of the infra
// goldens fit, each with the severity that starts the alarms' AlarmDescription. It fails
// for an alarm that fits no pattern or several, and for a pattern whose alarms differ in
// severity.
func alarmKinds(t *testing.T, patterns []string) map[string]string {
	t.Helper()
	res := map[string]*regexp.Regexp{}
	for _, p := range patterns {
		res[p] = alarmRegexp(t, p)
	}
	goldens, err := fs.Glob(os.DirFS(repoRoot), "internal/infra/testdata/*/*.txtar")
	if err != nil || len(goldens) == 0 {
		t.Fatalf("no infra goldens: %v", err)
	}
	kinds := map[string]string{}
	for _, g := range goldens {
		data, readErr := os.ReadFile(filepath.Join(repoRoot, g))
		if readErr != nil {
			t.Fatal(readErr)
		}
		found := alarmName.FindAllStringSubmatch(string(data), -1)
		if n := strings.Count(string(data), "AlarmName: "); n != len(found) {
			t.Errorf("%s: %d alarms, %d of them with an AlarmDescription that starts with critical: or warning:", g, n, len(found))
		}
		for _, m := range found {
			fit := slices.DeleteFunc(slices.Clone(patterns), func(p string) bool { return !res[p].MatchString(m[1]) })
			switch {
			case len(fit) != 1:
				t.Errorf("%s: alarm %s fits %d patterns of the table: %q", g, m[1], len(fit), fit)
			case kinds[fit[0]] != "" && kinds[fit[0]] != m[2]:
				t.Errorf("%s: alarm %s is %s, and another alarm of %s is %s", g, m[1], m[2], fit[0], kinds[fit[0]])
			default:
				kinds[fit[0]] = m[2]
			}
		}
	}
	return kinds
}

// alarmRegexp returns the expression of a pattern such as {projector}-lag that matches the
// alarm names of the goldens, which prefix each name with the Prefix parameter.
func alarmRegexp(t *testing.T, pattern string) *regexp.Regexp {
	t.Helper()
	parts := map[string]string{
		"{base}":            `[a-z0-9-]+`,
		"{stream}":          `[a-z0-9_]+`,
		"{projector}":       `[a-z0-9-]+-projector(-[a-z0-9_]+)?`,
		"{stream table}":    `[a-z0-9-]+-stream-[a-z0-9_]+`,
		"{Firehose stream}": `[a-z0-9-]+-archive-[a-z0-9_]+`,
	}
	expr := alarmPart.ReplaceAllStringFunc(pattern, func(s string) string {
		if !strings.HasPrefix(s, "{") {
			return regexp.QuoteMeta(s)
		}
		if parts[s] == "" {
			t.Errorf("alarm %s: unknown placeholder %s", pattern, s)
		}
		return parts[s]
	})
	return regexp.MustCompile(`^!Sub \$\{Prefix\}` + expr + `$`)
}

// repoMarkdown returns the Markdown files of the repository, without hidden directories and
// the names that .gitignore lists at the root.
func repoMarkdown(t *testing.T) []string {
	t.Helper()
	ignored := gitIgnored(t)
	var paths []string
	err := fs.WalkDir(os.DirFS(repoRoot), ".", func(p string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case p != "." && (ignored[p] || d.IsDir() && strings.HasPrefix(d.Name(), ".")):
			if d.IsDir() {
				return fs.SkipDir
			}
		case strings.HasSuffix(p, ".md"):
			paths = append(paths, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

// gitIgnored returns the plain names, without wildcards, that the root .gitignore lists.
func gitIgnored(t *testing.T) map[string]bool {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	ignored := map[string]bool{}
	for line := range strings.Lines(string(data)) {
		name := strings.Trim(strings.TrimSpace(line), "/")
		if name != "" && !strings.ContainsAny(name, "#*?[/!") {
			ignored[name] = true
		}
	}
	return ignored
}
