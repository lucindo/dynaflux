// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package scripts_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// changelog follows Keep a Changelog: an empty section, and link definitions at the end.
const changelog = `# Changelog

All notable changes are listed here.

## [Unreleased]

## [1.1.0] - 2026-12-01


## [1.0.0] - 2026-11-20

The first stable release.

### Fixed

- A fix.

## [1.0.0-rc.1] - 2026-11-10

### Added

- The first release candidate.

[Unreleased]: https://github.com/lucindo/dynaflux/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/lucindo/dynaflux/compare/v1.0.0...v1.1.0
`

// firstRelease is a changelog without link definitions, as the repository's is until a tag
// exists: its last section runs to the end of the file.
const firstRelease = `# Changelog

## [Unreleased]

## [1.0.0-rc.1] - 2026-11-10

### Added

- The first release candidate.
`

func TestReleaseNotes(t *testing.T) {
	t.Parallel()
	script, err := filepath.Abs("release-notes.sh")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, changelog, tag string
		want                 run
	}{
		{"stable", changelog, "v1.0.0", run{stdout: "The first stable release.\n\n### Fixed\n\n- A fix.\n"}},
		{"before the links", changelog, "v1.0.0-rc.1", run{stdout: "### Added\n\n- The first release candidate.\n"}},
		{"to the end of the file", firstRelease, "v1.0.0-rc.1", run{stdout: "### Added\n\n- The first release candidate.\n"}},
		{"empty", changelog, "v1.1.0", run{stderr: "CHANGELOG.md has no notes under a heading \"## [1.1.0]\".\n", status: 1}},
		{"missing", changelog, "v2.0.0", run{stderr: "CHANGELOG.md has no notes under a heading \"## [2.0.0]\".\n", status: 1}},
		{"tag without v", changelog, "1.0.0", run{stderr: "usage: " + script + " <tag>, such as " + script + " v1.0.0\n", status: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if werr := os.WriteFile(filepath.Join(dir, "CHANGELOG.md"), []byte(tt.changelog), 0o600); werr != nil {
				t.Fatal(werr)
			}
			if got := releaseNotes(t, script, dir, tt.tag); got != tt.want {
				t.Errorf("got %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

// run is what a run of release-notes.sh printed, and its exit status.
type run struct {
	stdout, stderr string
	status         int
}

// releaseNotes runs script, release-notes.sh, for tag in dir.
func releaseNotes(t *testing.T, script, dir, tag string) run {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(t.Context(), script, tag)
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		return run{stdout.String(), stderr.String(), exit.ExitCode()}
	case err != nil:
		t.Fatal(err)
	}
	return run{stdout.String(), stderr.String(), 0}
}
