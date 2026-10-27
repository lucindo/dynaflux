// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/validate"
)

// defaultRegion is the region init writes when neither -region nor AWS_REGION names one.
const defaultRegion = "us-east-1"

func initCommand() command {
	return command{
		name:    "init",
		args:    "[-store name] [-region region]...",
		summary: "start a project: dynaflux.yaml and an example stream, view and query",
		doc: `Init starts a Dynaflux project in the working directory, which should be the root of
a Go module. It writes dynaflux.yaml and a page-view example to replace with your own:
the stream dynaflux/streams/page_views.sql, the view dynaflux/views/page_daily.sql and
the query dynaflux/queries/pages.sql. The store is named after the directory unless
-store names it, and its regions are those named with -region, or else AWS_REGION, or
else us-east-1. Init makes no AWS call.

Init writes nothing when the directory or a parent already has dynaflux.yaml, or when
a file it would write exists.

Exit status: 0 when the files are written, 1 when a project or one of the files
exists, and 2 for usage and I/O problems, including a store name or region that
dynaflux.yaml doesn't allow.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			var o initOptions
			fs.StringVar(&o.store, "store", "", "the store's `name` (default the directory's name, lowercase, with _ as -)")
			fs.Var(&o.regions, "region", "a `region` of the store; repeat it for more (default AWS_REGION, or else "+defaultRegion+")")
			return func(c *cli, args []string) int { return c.init(o, args) }
		},
	}
}

type initOptions struct {
	store   string
	regions regionList
}

// regionList is a repeatable flag.
type regionList []string

func (r *regionList) String() string { return strings.Join(*r, ",") }

func (r *regionList) Set(s string) error {
	*r = append(*r, s)
	return nil
}

func (c *cli) init(o initOptions, args []string) int {
	if len(args) > 0 {
		return c.usageError("init", "unexpected arguments: "+strings.Join(args, " "))
	}
	dir, err := filepath.Abs(c.workdir)
	if err != nil {
		c.errorf("dynaflux init: %v", err)
		return exitUsage
	}
	switch existing, ferr := config.Find(dir); {
	case ferr == nil:
		c.errorf("dynaflux init: %s already exists, so this directory is in that project", existing)
		return exitErrors
	case !errors.Is(ferr, config.ErrNotFound):
		c.errorf("dynaflux init: %v", ferr)
		return exitUsage
	}
	store := o.store
	if store == "" {
		store = strings.ReplaceAll(strings.ToLower(filepath.Base(dir)), "_", "-")
	}
	regions := []string(o.regions)
	if len(regions) == 0 {
		regions = []string{cmp.Or(c.getenv("AWS_REGION"), defaultRegion)}
	}
	files := starterFiles(store, regions)
	if status := c.checkStarter(files, dir, o.store == ""); status != exitOK {
		return status
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		c.errorf("dynaflux init: %v", err)
		return exitUsage
	}
	defer root.Close()
	names := slices.Sorted(maps.Keys(files))
	for _, name := range names {
		switch _, serr := root.Stat(name); {
		case serr == nil:
			c.errorf("dynaflux init: %s exists; init writes nothing over your files", name)
			return exitErrors
		case !errors.Is(serr, fs.ErrNotExist):
			c.errorf("dynaflux init: %v", serr)
			return exitUsage
		}
	}
	if _, serr := root.Stat("go.mod"); errors.Is(serr, fs.ErrNotExist) {
		c.errorf("dynaflux init: warning: no go.mod here; dynaflux.yaml belongs at the root of a Go module (go mod init)")
	}
	for _, name := range names {
		if werr := writeNew(root, name, files[name].Data); werr != nil {
			c.errorf("dynaflux init: %v", werr)
			return exitUsage
		}
		fmt.Fprintf(&c.out, "wrote %s\n", name)
	}
	c.out.WriteString(`
Next:
  dynaflux generate   write the Go client package, the runtime's main.go and the templates
  dynaflux dev        run the store on DynamoDB Local
`)
	return exitOK
}

// checkStarter compiles the files before init writes them, so a store name or region that
// dynaflux.yaml doesn't allow is reported with its diagnostic and nothing is written.
func (c *cli) checkStarter(files fstest.MapFS, dir string, fromDir bool) int {
	_, ds := validate.Compile(files, dir)
	if !diag.HasErrors(ds) {
		return exitOK
	}
	c.errorf("dynaflux init: the project wouldn't be valid; nothing written")
	status := c.diagnostics(ds, exitUsage)
	if fromDir {
		c.errorf("  hint: name the store with -store")
	}
	return status
}

// writeNew writes a file that doesn't exist yet, and its directory.
func writeNew(root *os.Root, name string, data []byte) error {
	const dirPerm, filePerm = 0o755, 0o644
	if err := root.MkdirAll(path.Dir(name), dirPerm); err != nil {
		return err
	}
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return errors.Join(err, f.Close())
}

// starterFiles returns the files of a new project: dynaflux.yaml and the page-view example.
func starterFiles(store string, regions []string) fstest.MapFS {
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }
	return fstest.MapFS{
		config.FileName: file(fmt.Sprintf(`# The Dynaflux store. Every key is described in
# https://github.com/lucindo/dynaflux/blob/main/docs/spec/config.md
store: %s
regions: [%s]
sql: ["dynaflux/**/*.sql"]
`, store, strings.Join(regions, ", "))),
		"dynaflux/streams/page_views.sql": file(`-- Page views: one event per page a user opens.
CREATE STREAM page_views (
    user_id     String,
    page        String,
    duration_ms UInt32
)
PRIMARY KEY (user_id);
`),
		"dynaflux/views/page_daily.sql": file(`-- Views and visitors per page and day.
CREATE MATERIALIZED VIEW page_daily
ORDER BY (page, day)
PRIMARY KEY (page)
AS SELECT
    page,
    toStartOfDay(event_time) AS day,
    count()                  AS views,
    uniqExact(user_id)       AS visitors,
    avg(duration_ms)         AS avg_ms
FROM page_views
GROUP BY page, day;
`),
		"dynaflux/queries/pages.sql": file(`-- name: PageStats :many
SELECT day, views, visitors, avg_ms
FROM page_daily
WHERE page = {page:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;
`),
	}
}
