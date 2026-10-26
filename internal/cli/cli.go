// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package cli implements the dynaflux command.
package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/diag"
)

// Exit statuses, from docs/spec/sql.md appendix A.1.
const (
	exitOK     = 0
	exitErrors = 1 // errors in the project or in an explain report
	exitUsage  = 2 // usage and I/O problems
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
	return []command{validateCommand(), explainCommand(), generateCommand(), versionCommand(), helpCommand()}
}

// planned maps the commands of later milestones to their milestone, so that the CLI's
// shape shows already.
func planned() map[string]string {
	const m2, m5 = "M2", "M5"
	return map[string]string{
		"init": m2,
		"diff": m5, "dev": m5, "status": m5, "backfill": m5, "retire": m5, "import": m5, "inspect": m5, "query": m5,
	}
}

const plannedHelp = `Planned:
  init                                                         milestone M2
  diff, dev, status, backfill, retire, import, inspect, query  milestone M5
`

// cli is one run of dynaflux. Commands write to out and errOut, and Run copies them to
// stdout and stderr, so a failed write is checked in one place.
type cli struct {
	workdir     string
	out, errOut strings.Builder
}

// Run runs dynaflux with args, the arguments after the program name, in the directory
// workdir. It returns the exit status: 0 on success, 1 for errors in the project or in an
// explain report, and 2 for usage and I/O problems.
func Run(args []string, stdout, stderr io.Writer, workdir string) int {
	c := &cli{workdir: workdir}
	status := c.run(args)
	if _, err := io.WriteString(stdout, c.out.String()); err != nil {
		fmt.Fprintf(&c.errOut, "dynaflux: write output: %v\n", err)
		status = exitUsage
	}
	if _, err := io.WriteString(stderr, c.errOut.String()); err != nil {
		return exitUsage
	}
	return status
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
	if m, ok := planned()[name]; ok {
		c.errorf("dynaflux %s: not available yet (milestone %s)", name, m)
		return command{}, false
	}
	names := slices.Collect(maps.Keys(planned()))
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
	b.WriteString("\n" + plannedHelp + "\nRun 'dynaflux help <command>' for more about a command.\n")
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
