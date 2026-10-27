// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// LockFileName is the name of the lock file, which sits at the project root next to
// dynaflux.yaml and numbers the view versions.
const LockFileName = "dynaflux.lock"

// LockFormatVersion is the layout version of dynaflux.lock.
const LockFormatVersion = 1

// Lock is dynaflux.lock, which numbers the versions of every view.
type Lock struct {
	// Views maps a view name to its versions, newest first. A retired version keeps only its
	// Number, State and Fingerprint, so that its number is never reused.
	Views map[string][]ViewVersion
	// Rebuilds maps a view to its rebuild count, which dynaflux generate -rebuild raises by
	// one; a view never rebuilt has none. The compiler gives the count to the view's
	// definition (ViewVersion.Rebuild).
	Rebuilds map[string]int
}

// retiredEntry is a retired version as dynaflux.lock holds it: all that numbering needs.
type retiredEntry struct {
	Number      int          `json:"number"`
	State       VersionState `json:"state"`
	Fingerprint string       `json:"fingerprint"`
}

// LockChangeKind says how [Lock.Apply] changed a version.
type LockChangeKind string

// Lock changes.
const (
	LockNew          LockChangeKind = "new"           // A definition the lock didn't hold, with the next number.
	LockCurrentAgain LockChangeKind = "current again" // A retained version that the SQL defines again.
	LockRetained     LockChangeKind = "retained"      // A current version that the SQL no longer defines.
)

// LockChange is a version whose state [Lock.Apply] changed.
type LockChange struct {
	View   string
	Number int
	Kind   LockChangeKind
}

// String returns the line dynaflux generate prints, such as "page_daily@v2: new".
func (c LockChange) String() string {
	return VersionID(c.View, c.Number) + ": " + string(c.Kind)
}

// ParseLock reads dynaflux.lock. It accepts any JSON layout, and more than one current
// version of a view, which a clean git merge can leave and [Lock.Apply] repairs. It
// rejects everything else that isn't a lock [MarshalLock] could write: a format other
// than [LockFormatVersion], a key repeated in one object or not a lower-case name, unknown
// fields, unknown states, a view without versions or with a repeated number, a retired
// entry with more than its number, state and fingerprint, an entry that isn't a valid view
// version, such as a hand-edited one, whose definition no longer matches its fingerprint,
// and a rebuild count below 1, of a view the lock doesn't hold, or below an entry's. A
// JSON syntax error wraps a [*json.SyntaxError], whose Offset locates it.
func ParseLock(data []byte) (*Lock, error) {
	// The probe rejects syntax errors and trailing data, and reads the format before the
	// strict decode, so a newer lock gets a format error rather than an unknown field.
	var probe struct {
		Format int `json:"format"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if probe.Format != LockFormatVersion {
		return nil, fmt.Errorf("format %d is not supported; this Dynaflux reads format %d", probe.Format, LockFormatVersion)
	}
	keys := json.NewDecoder(bytes.NewReader(data))
	keys.UseNumber() // Numbers out of float64's range are the strict decode's to report.
	if err := checkKeys(keys, ""); err != nil {
		return nil, err
	}
	var raw struct {
		Format  int                          `json:"format"`
		Rebuild map[string]int               `json:"rebuild"`
		Views   map[string][]json.RawMessage `json:"views"`
	}
	if err := strictDecode(data, &raw); err != nil {
		return nil, err
	}
	l := &Lock{Views: make(map[string][]ViewVersion, len(raw.Views)), Rebuilds: raw.Rebuild}
	for name, entries := range raw.Views {
		vs, err := parseLockedView(name, entries)
		if err != nil {
			return nil, err
		}
		l.Views[name] = vs
	}
	if err := l.checkRebuilds(); err != nil {
		return nil, err
	}
	return l, nil
}

// checkRebuilds checks that each rebuild count is of a view in l, at least 1, and at least
// the count of each of the view's versions.
func (l *Lock) checkRebuilds() error {
	for _, name := range slices.Sorted(maps.Keys(l.Rebuilds)) {
		if _, ok := l.Views[name]; !ok {
			return fmt.Errorf("rebuild names view %s, which the lock doesn't hold", name)
		}
		if n := l.Rebuilds[name]; n < 1 {
			return fmt.Errorf("view %s has rebuild count %d; it must be at least 1", name, n)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(l.Views)) {
		for _, vv := range l.Views[name] {
			if vv.Rebuild > l.Rebuilds[name] {
				return fmt.Errorf("%s has rebuild count %d, more than its view's %d",
					VersionID(name, vv.Number), vv.Rebuild, l.Rebuilds[name])
			}
		}
	}
	return nil
}

// parseLockedView decodes and checks the entries of one view, and sorts them newest first.
func parseLockedView(name string, entries []json.RawMessage) ([]ViewVersion, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("view %s has no versions", name)
	}
	vs := make([]ViewVersion, len(entries))
	for i, e := range entries {
		var err error
		if vs[i], err = parseLockEntry(name, i+1, e); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(vs, newestFirst)
	for i := 1; i < len(vs); i++ {
		if vs[i].Number == vs[i-1].Number {
			return nil, fmt.Errorf("view %s: version %d appears twice", name, vs[i].Number)
		}
	}
	return vs, nil
}

// parseLockEntry decodes and checks entry n of view.
func parseLockEntry(view string, n int, data []byte) (ViewVersion, error) {
	var head struct {
		State VersionState `json:"state"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return ViewVersion{}, fmt.Errorf("view %s, entry %d: %w", view, n, err)
	}
	if head.State == StateRetired {
		var r retiredEntry
		if err := strictDecode(data, &r); err != nil {
			return ViewVersion{}, fmt.Errorf("view %s, entry %d: a retired entry holds only its number, state and fingerprint: %w",
				view, n, err)
		}
		id := VersionID(view, r.Number)
		switch {
		case r.Number < 1:
			return ViewVersion{}, fmt.Errorf("%s: version numbers start at 1", id)
		case len(r.Fingerprint) != 64 || strings.Trim(r.Fingerprint, "0123456789abcdef") != "":
			return ViewVersion{}, fmt.Errorf("%s: fingerprint %q isn't 64 lowercase hex digits", id, r.Fingerprint)
		}
		return ViewVersion{Number: r.Number, State: r.State, Fingerprint: r.Fingerprint}, nil
	}
	var vv ViewVersion
	if err := strictDecode(data, &vv); err != nil {
		return ViewVersion{}, fmt.Errorf("view %s, entry %d: %w", view, n, err)
	}
	id := VersionID(view, vv.Number)
	// UnmarshalText rejects the unknown states and modes, but an absent one skips it.
	switch {
	case !vv.State.known():
		return ViewVersion{}, fmt.Errorf("%s has no state", id)
	case !vv.Mode.known():
		return ViewVersion{}, fmt.Errorf("%s has no mode", id)
	}
	if err := checkVersion(&vv); err != nil {
		return ViewVersion{}, fmt.Errorf("%s: %w", id, err)
	}
	return vv, nil
}

// checkKeys checks the keys of the value that dec reads next, at path. encoding/json
// would keep the last of a repeated key, as a merge tool's "accept both" can leave, and
// match a key to a field without regard to case; every key in a lock is a lower-case name.
func checkKeys(dec *json.Decoder, path string) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch tok {
	case json.Delim('{'):
		seen := map[string]bool{}
		for dec.More() {
			if tok, err = dec.Token(); err != nil {
				return err
			}
			key, isString := tok.(string)
			at := strings.TrimPrefix(path+"."+key, ".")
			switch {
			case seen[key]:
				return fmt.Errorf("key %q appears twice", at)
			case !isString || !IsName(key):
				return fmt.Errorf("key %q isn't a lower-case name", at)
			}
			seen[key] = true
			if err = checkKeys(dec, at); err != nil {
				return err
			}
		}
	case json.Delim('['):
		for i := 0; dec.More(); i++ {
			if err = checkKeys(dec, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	default:
		return nil
	}
	_, err = dec.Token() // The closing delimiter.
	return err
}

func strictDecode(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func newestFirst(a, b ViewVersion) int {
	return cmp.Compare(b.Number, a.Number)
}

// Apply numbers the views that the SQL files define (sql.md §9.9). defs maps each to its
// definition, which the compiler gave the view's rebuild count and then its Fingerprint. A
// definition with the fingerprint of a version in the lock that isn't retired takes that
// version's number, and otherwise the view's next number; it is current, and every other
// version that isn't retired is retained. So the SQL alone decides which version is
// current, a view the SQL no longer defines keeps its versions, retained, and a retired
// number never comes back. Apply returns the new lock and the changes, sorted by view and
// number.
func (l *Lock) Apply(defs map[string]ViewVersion) (*Lock, []LockChange) {
	next := &Lock{Views: make(map[string][]ViewVersion, len(l.Views)), Rebuilds: maps.Clone(l.Rebuilds)}
	for name, vs := range l.Views {
		vs = slices.Clone(vs)
		for i := range vs {
			if vs[i].State == StateCurrent {
				vs[i].State = StateRetained
			}
		}
		next.Views[name] = vs
	}
	var changes []LockChange
	for name, def := range defs {
		c := next.define(name, def)
		if c.Kind == LockNew || l.state(name, c.Number) != StateCurrent {
			changes = append(changes, c)
		}
	}
	for name, vs := range next.Views {
		for _, vv := range vs {
			if vv.State == StateRetained && l.state(name, vv.Number) == StateCurrent {
				changes = append(changes, LockChange{name, vv.Number, LockRetained})
			}
		}
	}
	slices.SortFunc(changes, func(a, b LockChange) int {
		return cmp.Or(strings.Compare(a.View, b.View), cmp.Compare(a.Number, b.Number))
	})
	return next, changes
}

// define makes def the current version of view in l, and returns it as a new version or
// as one current again.
func (l *Lock) define(view string, def ViewVersion) LockChange {
	def.State = StateCurrent
	vs := l.Views[view]
	i := slices.IndexFunc(vs, func(vv ViewVersion) bool { return vv.State != StateRetired && vv.Fingerprint == def.Fingerprint })
	if i >= 0 {
		// The definition can differ from the locked one only in MODE, which changes in place.
		def.Number = vs[i].Number
		vs[i] = def
		return LockChange{view, def.Number, LockCurrentAgain}
	}
	def.Number = 1
	for _, vv := range vs {
		def.Number = max(def.Number, vv.Number+1)
	}
	l.Views[view] = slices.Insert(vs, 0, def)
	return LockChange{view, def.Number, LockNew}
}

// state returns the state of a view's version in l, or "" when l doesn't hold it.
func (l *Lock) state(view string, number int) VersionState {
	for _, vv := range l.Views[view] {
		if vv.Number == number {
			return vv.State
		}
	}
	return ""
}

// Retire marks a retained version retired, which keeps only its number and fingerprint.
// Retiring a retired version does nothing. It fails for a version that is current or that
// l doesn't hold.
func (l *Lock) Retire(view string, number int) error {
	vs := l.Views[view]
	i := slices.IndexFunc(vs, func(vv ViewVersion) bool { return vv.Number == number })
	id := VersionID(view, number)
	switch {
	case i < 0:
		return fmt.Errorf("retire %s: %s doesn't hold it", id, LockFileName)
	case vs[i].State == StateCurrent:
		return fmt.Errorf("retire %s: it is the current version", id)
	}
	vs[i] = ViewVersion{Number: number, State: StateRetired, Fingerprint: vs[i].Fingerprint}
	return nil
}

// CatalogViews returns the views of the catalog that l numbers: every version that isn't
// retired, in name order and oldest first.
func (l *Lock) CatalogViews() []View {
	views := make([]View, 0, len(l.Views))
	for _, name := range slices.Sorted(maps.Keys(l.Views)) {
		vs := slices.DeleteFunc(slices.Clone(l.Views[name]), func(vv ViewVersion) bool { return vv.State == StateRetired })
		if len(vs) == 0 {
			continue
		}
		slices.SortFunc(vs, func(a, b ViewVersion) int { return cmp.Compare(a.Number, b.Number) })
		views = append(views, View{Name: name, Versions: vs})
	}
	return views
}

// Retired returns the IDs of the retired versions in l, sorted, as [Catalog.Retired]
// holds them.
func (l *Lock) Retired() []string {
	var ids []string
	for name, vs := range l.Views {
		for _, vv := range vs {
			if vv.State == StateRetired {
				ids = append(ids, VersionID(name, vv.Number))
			}
		}
	}
	slices.Sort(ids)
	return ids
}

// MarshalLock writes l as dynaflux.lock: JSON indented by two spaces, the rebuild counts,
// then views by name and their versions newest first, so a new version adds lines at the
// top of its view and branches that change different views merge cleanly. It fails for a
// lock that [ParseLock] would reject, so dynaflux generate never writes a lock it can't
// read back.
func MarshalLock(l *Lock) ([]byte, error) {
	if l == nil {
		return nil, errors.New("marshal lock: lock is nil")
	}
	views := make(map[string][]any, len(l.Views))
	for name, vs := range l.Views {
		vs = slices.Clone(vs)
		slices.SortFunc(vs, newestFirst)
		entries := make([]any, len(vs))
		for i, vv := range vs {
			if vv.State == StateRetired {
				entries[i] = retiredEntry{vv.Number, vv.State, vv.Fingerprint}
				continue
			}
			vv.Aggregates = sortedAggregates(vv.Aggregates)
			entries[i] = vv
		}
		views[name] = entries
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	err := enc.Encode(struct {
		Format  int              `json:"format"`
		Rebuild map[string]int   `json:"rebuild,omitempty"`
		Views   map[string][]any `json:"views"`
	}{LockFormatVersion, l.Rebuilds, views})
	if err != nil {
		return nil, fmt.Errorf("marshal lock: %w", err)
	}
	if _, err = ParseLock(buf.Bytes()); err != nil {
		return nil, fmt.Errorf("marshal lock: %w", err)
	}
	return buf.Bytes(), nil
}
