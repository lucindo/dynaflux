// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/cli"
)

func TestConfirm(t *testing.T) {
	t.Parallel()
	const question = "retire page_daily@v1 in us-east-1? [y/N] "
	tests := []struct {
		name     string
		stdin    string
		terminal bool
		yes      bool
		status   int
		stderr   string
	}{
		{"-yes", "", false, true, 0, ""},
		{"yes on a terminal", "y\n", true, false, 0, question},
		{"YES on a terminal", " YES \n", true, false, 0, question},
		{"no on a terminal", "n\n", true, false, 1, question + "dynaflux retire: nothing written\n"},
		{"no answer", "", true, false, 1, question + "dynaflux retire: nothing written\n"},
		{
			"no terminal", "y\n", false, false,
			2, "dynaflux retire: retire page_daily@v1 in us-east-1?: confirm with -yes when not on a terminal\n" +
				"Run 'dynaflux help retire' for usage.\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			status, stderr := cli.Confirm(t.Context(), strings.NewReader(tt.stdin), tt.terminal, tt.yes)
			if status != tt.status || stderr != tt.stderr {
				t.Errorf("got %d %q, want %d %q", status, stderr, tt.status, tt.stderr)
			}
		})
	}
}

// An interrupt while confirm waits for an answer refuses at once.
func TestConfirmInterrupted(t *testing.T) {
	t.Parallel()
	stdin, w := io.Pipe()
	defer w.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	status, stderr := cli.Confirm(ctx, stdin, true, false)
	want := "retire page_daily@v1 in us-east-1? [y/N] \ndynaflux retire: interrupted; nothing written\n"
	if status != 1 || stderr != want {
		t.Errorf("got %d %q, want 1 %q", status, stderr, want)
	}
}

// /dev/null is a character device but no terminal: a command run from cron with it as stdin
// refuses to write without -yes rather than read no answer.
func TestDevNullIsNoTerminal(t *testing.T) {
	t.Parallel()
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if cli.IsTerminal(f) {
		t.Errorf("%s is a terminal, want not", os.DevNull)
	}
}
