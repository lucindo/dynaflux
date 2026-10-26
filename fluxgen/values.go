// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package fluxgen

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/internal/access"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/gotype"
)

// checkFields checks that ptrs point to fields of the Go types of cols.
func checkFields(what string, cols []catalog.Column, ptrs []any) error {
	if len(ptrs) != len(cols) {
		return fmt.Errorf("dynaflux: %s: %d fields, want %d; regenerate the client", what, len(ptrs), len(cols))
	}
	for i, c := range cols {
		if got, want := reflect.TypeOf(ptrs[i]), reflect.PointerTo(gotype.Of(c.Type)); got != want {
			return fmt.Errorf("dynaflux: %s: field %d (%s %s) is %v, want %v; regenerate the client", what, i, c.Name, c.Type, got, want)
		}
	}
	return nil
}

// value returns the value of the field ptr points to, of column c.
func value(c catalog.Column, ptr any) (expr.Value, error) {
	f := reflect.ValueOf(ptr).Elem()
	switch {
	case c.Type.Nullable:
		if f.IsNil() {
			return expr.Null(), nil
		}
		f = f.Elem()
	case c.Type.Array:
		vs := make([]expr.Value, f.Len())
		for i := range vs {
			v, err := scalarValue(c.Type, f.Index(i))
			if err != nil {
				return expr.Value{}, invalid(c.Name, fmt.Errorf("element %d: %w", i+1, err))
			}
			vs[i] = v
		}
		return expr.Array(vs), nil
	}
	v, err := scalarValue(c.Type, f)
	if err != nil {
		return expr.Value{}, invalid(c.Name, err)
	}
	return v, nil
}

func invalid(name string, err error) error {
	return &dynaflux.ValueError{Name: name, Err: fmt.Errorf("%w: %w", dynaflux.ErrInvalidValue, err)}
}

// eventTime returns the value of an event_time field: NULL when it is zero, for the
// writer's clock.
func eventTime(c catalog.Column, ptr any) (expr.Value, error) {
	if t, ok := ptr.(*time.Time); ok && t.IsZero() {
		return expr.Null(), nil
	}
	return value(c, ptr)
}

// scalarValue converts f, a non-NULL value of t's kind. It rejects only what it can't
// convert exactly; the engine checks the rest of sql.md §4.3.
func scalarValue(t catalog.Type, f reflect.Value) (expr.Value, error) {
	switch x := f.Interface().(type) {
	case string:
		return expr.String(x), nil
	case uint8, uint16, uint32, uint64:
		return expr.Uint(f.Uint()), nil
	case int8, int16, int32, int64:
		return expr.Int(f.Int()), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return expr.Value{}, fmt.Errorf("%v is not finite", x)
		}
		return expr.Float(x), nil
	case dynaflux.Decimal:
		return decimalValue(t, x)
	case bool:
		return expr.Bool(x), nil
	case dynaflux.UUID:
		return expr.UUID(x), nil
	case time.Time:
		return timeValue(t, x)
	}
	return expr.Value{}, fmt.Errorf("unexpected Go type %v", f.Type())
}

// decimalValue rescales d to t's scale, which must keep its value exactly.
func decimalValue(t catalog.Type, d dynaflux.Decimal) (expr.Value, error) {
	u, scale := d.Big()
	if scale <= t.Scale {
		return expr.Decimal(u.Mul(u, expr.Pow10(t.Scale-scale))), nil
	}
	q, r := new(big.Int).QuoRem(u, expr.Pow10(scale-t.Scale), new(big.Int))
	if r.Sign() != 0 {
		return expr.Value{}, fmt.Errorf("%s has more than %d digits after the point", d, t.Scale)
	}
	return expr.Decimal(q), nil
}

// secondsLimit is catalog.TimeLimitMicros in seconds. Times are checked in seconds first:
// UnixMicro overflows long after 2106.
const secondsLimit = catalog.TimeLimitMicros / int64(time.Second/time.Microsecond)

// timeValue converts tm: a Date is its UTC date, and a DateTime64(3) drops its
// microseconds.
func timeValue(t catalog.Type, tm time.Time) (expr.Value, error) {
	if t.Kind == catalog.KindDate {
		y, m, d := tm.UTC().Date()
		tm = time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	}
	if s := tm.Unix(); s < 0 || s >= secondsLimit {
		return expr.Value{}, fmt.Errorf("%s is outside 1970 to 2106", tm.Format(time.RFC3339Nano))
	}
	us := tm.UnixMicro()
	if t.Kind == catalog.KindDateTime64 && t.Precision == catalog.DateTime64Millis {
		us -= us % int64(time.Millisecond/time.Microsecond)
	}
	return expr.Micros(us), nil
}

// scan stores row, values of cols, in the fields ptrs point to. An empty array is nil.
func scan(cols []catalog.Column, row []expr.Value, ptrs []any) error {
	for i, c := range cols {
		f := reflect.ValueOf(ptrs[i]).Elem()
		v := row[i]
		var err error
		switch {
		case c.Type.Nullable && v.IsNull(), c.Type.Array && len(v.AsArray()) == 0:
			f.SetZero()
		case c.Type.Nullable:
			p := reflect.New(f.Type().Elem())
			err = setScalar(c.Type, v, p.Elem())
			f.Set(p)
		case c.Type.Array:
			elems := v.AsArray()
			s := reflect.MakeSlice(f.Type(), len(elems), len(elems))
			for j, e := range elems {
				if err = setScalar(c.Type, e, s.Index(j)); err != nil {
					break
				}
			}
			f.Set(s)
		default:
			err = setScalar(c.Type, v, f)
		}
		if err != nil {
			return fmt.Errorf("dynaflux: %s: %w", c.Name, err)
		}
	}
	return nil
}

func setScalar(t catalog.Type, v expr.Value, f reflect.Value) error {
	switch t.Kind {
	case catalog.KindString:
		f.SetString(v.AsString())
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64:
		f.SetUint(v.AsUint())
	case catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		f.SetInt(v.AsInt())
	case catalog.KindFloat64:
		f.SetFloat(v.AsFloat())
	case catalog.KindDecimal:
		d, err := dynaflux.DecimalFromBig(v.AsDecimal(), t.Scale)
		if err != nil {
			return err
		}
		f.Set(reflect.ValueOf(d))
	case catalog.KindBool:
		f.SetBool(v.AsBool())
	case catalog.KindUUID:
		f.Set(reflect.ValueOf(dynaflux.UUID(v.AsUUID())))
	case catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		f.Set(reflect.ValueOf(time.UnixMicro(v.AsMicros()).UTC()))
	default:
		return errors.New("unknown type " + t.String())
	}
	return nil
}

// readRange returns the event_time bounds of opts in microseconds, rounded up: an event at
// a whole microsecond is at or after a time with nanoseconds exactly when it is at or after
// the next microsecond.
func readRange(opts []dynaflux.ReadOption) (*int64, *int64) {
	since, until := access.Get().ReadRange(opts)
	return ceilMicros(since), ceilMicros(until)
}

func ceilMicros(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	// Outside 1970 to 2106, any time past the same end bounds the same events.
	us := int64(-1)
	switch s := t.Unix(); {
	case s >= secondsLimit:
		us = math.MaxInt64
	case s >= 0:
		us = t.UnixMicro()
		if t.Nanosecond()%int(time.Microsecond) != 0 {
			us++
		}
	}
	return &us
}
