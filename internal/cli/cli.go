// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package cli implements the dynaflux command.
package cli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"slices"
	"strings"

	"golang.org/x/term"

	"github.com/lucindo/dynaflux/internal/diag"
)

// Exit statuses, from docs/spec/sql.md appendix A.1.
const (
	exitOK     = 0
	exitErrors = 1 // errors in the project, an explain report or a deployed store, and refusals
	exitUsage  = 2 // usage, I/O and AWS access problems
)

// command is a subcommand of dynaflux.
type command struct {
	name    string
	alias   string // another name that runs the command, or ""
	args    string // what follows the name in the usage line
	summary string // its line in the command list
	doc     string // what dynaflux help <name> prints after the usage line
	// setup defines the command's flags on fs and returns the function that runs the
	// command on the arguments left after the flags.
	setup func(fs *flag.FlagSet) func(c *cli, args []string) int
}

func commands() []command {
	return []command{
		initCommand(), validateCommand(), explainCommand(), generateCommand(), diffCommand(), devCommand(), statusCommand(),
		backfillCommand(), retireCommand(), inspectCommand(), queryCommand(), importCommand(), versionCommand(), helpCommand(),
	}
}

// cli is one run of dynaflux.
type cli struct {
	ctx     context.Context
	workdir string
	stdin   io.Reader
	// terminal is whether stdin is a terminal, on which a command asks before a write that
	// can't be undone.
	terminal bool
	// connect reaches AWS, or DynamoDB Local at endpoint, and getenv reads the environment;
	// tests replace them.
	connect func(ctx context.Context, endpoint string) (cloud, error)
	getenv  func(key string) string
	// build describes this binary, nil without build information; tests replace it.
	build *debug.BuildInfo
	// goModCeiling is the highest directory where a go.mod is looked for, "" for the root;
	// tests set it, so that a go.mod above their temporary directory doesn't count.
	goModCeiling string
	// Commands write to out and errOut, and flush copies them to stdout and stderr, so a
	// failed write is checked in one place. A long-running command flushes to stream.
	out, errOut    strings.Builder
	stdout, stderr io.Writer
	outErr, errErr error // the first failed write to each
}

// Run runs dynaflux with args, the arguments after the program name, in the directory
// workdir. It returns the exit status: 0 on success; 1 for errors in the project, in an
// explain report or in a deployed store, and for a refused operation; and 2 for usage, I/O
// and AWS access problems. Cancelling ctx stops a long-running command; main cancels it on
// an interrupt. stdin is read only to confirm a write that can't be undone.
func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, workdir string) int {
	bi, _ := debug.ReadBuildInfo()
	c := &cli{
		ctx: ctx, workdir: workdir, stdin: stdin, terminal: isTerminal(stdin), connect: connectAWS, getenv: os.Getenv, build: bi,
		stdout: stdout, stderr: stderr,
	}
	return c.exit(c.run(args))
}

// isTerminal reports whether r is a terminal. A character device such as /dev/null, which
// cron gives a job, isn't one.
func isTerminal(r io.Reader) bool {
	f, ok := r.(*os.File)
	return ok && term.IsTerminal(int(f.Fd()))
}

// flush writes what the command printed since the last flush.
func (c *cli) flush() {
	if c.outErr == nil {
		_, c.outErr = io.WriteString(c.stdout, c.out.String())
	}
	if c.errErr == nil {
		_, c.errErr = io.WriteString(c.stderr, c.errOut.String())
	}
	c.out.Reset()
	c.errOut.Reset()
}

// exit flushes the output and returns status, or exitUsage when a write failed.
func (c *cli) exit(status int) int {
	c.flush()
	if c.outErr != nil {
		c.errorf("dynaflux: write output: %v", c.outErr)
		c.flush()
		status = exitUsage
	}
	if c.errErr != nil {
		return exitUsage
	}
	return status
}

// confirm asks question on a terminal before a write that can't be undone, and returns
// exitOK to go on. With yes it doesn't ask; without a terminal it refuses with exitUsage, so
// a script must pass -yes. A "no", or an interrupt, returns exitErrors.
func (c *cli) confirm(name, question string, yes bool) int {
	if yes {
		return exitOK
	}
	if !c.terminal {
		return c.usageError(name, question+": confirm with -yes when not on a terminal")
	}
	fmt.Fprintf(&c.errOut, "%s [y/N] ", question)
	c.flush()
	answer := make(chan string, 1)
	// A read from a terminal can't be cancelled: on an interrupt it ends with the process.
	go func() {
		s := bufio.NewScanner(c.stdin)
		s.Scan()
		answer <- s.Text()
	}()
	select {
	case a := <-answer:
		if a = strings.ToLower(strings.TrimSpace(a)); a == "y" || a == "yes" {
			return exitOK
		}
		c.errorf("dynaflux %s: nothing written", name)
	case <-c.ctx.Done():
		c.errorf("\ndynaflux %s: interrupted; nothing written", name)
	}
	return exitErrors
}

func (c *cli) run(args []string) int {
	if len(args) == 0 {
		writeUsage(&c.errOut)
		return exitUsage
	}
	name, args := args[0], args[1:]
	if name == "-h" || name == "-help" || name == "--help" {
		name = "help"
	}
	cmd, ok := c.find(name)
	if !ok {
		return exitUsage
	}
	fs := flagSet(cmd)
	run := cmd.setup(fs)
	operands, err := parse(fs, args)
	switch {
	case errors.Is(err, flag.ErrHelp):
		c.commandHelp(cmd, fs)
		return exitOK
	case err != nil:
		return c.usageError(cmd.name, err.Error())
	}
	return run(c, operands)
}

// find returns the command called name, or reports why there is none.
func (c *cli) find(name string) (command, bool) {
	cmds := commands()
	if i := slices.IndexFunc(cmds, func(cmd command) bool { return name == cmd.name || name == cmd.alias }); i >= 0 {
		return cmds[i], true
	}
	var names []string
	for _, cmd := range cmds {
		names = append(names, cmd.name)
		if cmd.alias != "" {
			names = append(names, cmd.alias)
		}
	}
	msg := fmt.Sprintf("dynaflux: unknown command %q", name)
	if s, ok := diag.Suggest(name, names); ok {
		msg += "; did you mean " + s + "?"
	}
	c.errorf("%s\nRun 'dynaflux help' for usage.", msg)
	return command{}, false
}

// flagSet returns an empty flag set for cmd. Run prints its errors and help itself.
func flagSet(cmd command) *flag.FlagSet {
	fs := flag.NewFlagSet("dynaflux "+cmd.name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

// parse parses the flags in args, which may also follow other arguments, and returns
// those arguments.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var operands []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return operands, nil
		}
		operands = append(operands, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func (c *cli) errorf(format string, args ...any) {
	fmt.Fprintf(&c.errOut, format+"\n", args...)
}

func (c *cli) usageError(name, msg string) int {
	c.errorf("dynaflux %s: %s\nRun 'dynaflux help %s' for usage.", name, msg, name)
	return exitUsage
}

func writeUsage(b *strings.Builder) {
	b.WriteString(`Dynaflux runs event-sourced fact streams and materialized views on DynamoDB Global Tables.

Usage:
  dynaflux <command> [arguments]

Commands:
`)
	for _, cmd := range commands() {
		name := cmd.name
		if cmd.alias != "" {
			name += " (" + cmd.alias + ")"
		}
		fmt.Fprintf(b, "  %-15s %s\n", name, cmd.summary)
	}
	b.WriteString("\nRun 'dynaflux help <command>' for more about a command.\n")
}

func (c *cli) commandHelp(cmd command, fs *flag.FlagSet) {
	line := "dynaflux " + cmd.name
	if cmd.args != "" {
		line += " " + cmd.args
	}
	fmt.Fprintf(&c.out, "Usage: %s\n\n%s", line, cmd.doc)
	hasFlags := false
	fs.VisitAll(func(*flag.Flag) { hasFlags = true })
	if hasFlags {
		c.out.WriteString("\nFlags:\n")
		fs.SetOutput(&c.out)
		fs.PrintDefaults()
	}
}

func helpCommand() command {
	return command{
		name:    "help",
		args:    "[command]",
		summary: "show help for a command",
		doc:     "Help lists the commands, or shows how to use the one named.\n",
		setup: func(*flag.FlagSet) func(*cli, []string) int {
			return (*cli).help
		},
	}
}

func (c *cli) help(args []string) int {
	switch len(args) {
	case 0:
		writeUsage(&c.out)
		return exitOK
	case 1:
		cmd, ok := c.find(args[0])
		if !ok {
			return exitUsage
		}
		fs := flagSet(cmd)
		cmd.setup(fs)
		c.commandHelp(cmd, fs)
		return exitOK
	default:
		return c.usageError("help", "name at most one command")
	}
}
