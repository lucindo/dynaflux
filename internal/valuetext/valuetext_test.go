// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package valuetext_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/valuetext"
)

func typ(t *testing.T, s string) catalog.Type {
	t.Helper()
	ct, err := catalog.ParseType(s)
	if err != nil {
		t.Fatal(err)
	}
	return ct
}

// TestRoundTrip parses each text, checks both forms of the value, and parses the JSON form
// back to the same value.
func TestRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typ, in, text, json string
	}{
		{"String", "/a&b \"q\"\n\x01é", "/a&b \"q\"\n\x01é", `"/a&b \"q\"\n\u0001é"`},
		{"UInt8", "255", "255", "255"},
		{"UInt16", "65535", "65535", "65535"},
		{"UInt32", "4294967295", "4294967295", "4294967295"},
		{"UInt64", "18446744073709551615", "18446744073709551615", `"18446744073709551615"`},
		{"Int8", "-128", "-128", "-128"},
		{"Int16", "-32768", "-32768", "-32768"},
		{"Int32", "+2147483647", "2147483647", "2147483647"},
		{"Int64", "-9223372036854775808", "-9223372036854775808", `"-9223372036854775808"`},
		{"Float64", "0.1", "0.1", "0.1"},
		{"Float64", "-0", "0", "0"},
		{"Float64", "1e21", "1e+21", "1e+21"},
		{"Float64", "0.0000001", "1e-7", "1e-7"},
		{"Decimal(10, 2)", "1.2", "1.20", `"1.20"`},
		{"Decimal(10, 2)", "-0.05", "-0.05", `"-0.05"`},
		{"Decimal(10, 2)", "3.140", "3.14", `"3.14"`},
		{"Decimal(38, 0)", "99999999999999999999999999999999999999", "99999999999999999999999999999999999999",
			`"99999999999999999999999999999999999999"`},
		{"Bool", "false", "false", "false"},
		{"UUID", "61F0C404-5CB3-11e7-907B-A6006AD3DBA0", "61f0c404-5cb3-11e7-907b-a6006ad3dba0",
			`"61f0c404-5cb3-11e7-907b-a6006ad3dba0"`},
		{"Date", "1970-01-01", "1970-01-01", `"1970-01-01"`},
		{"Date", "2106-02-07", "2106-02-07", `"2106-02-07"`},
		{"DateTime", "2026-10-26T13:45:00.999Z", "2026-10-26T13:45:00Z", `"2026-10-26T13:45:00Z"`},
		{"DateTime64(3)", "2026-10-26T13:45:00.123999Z", "2026-10-26T13:45:00.123Z", `"2026-10-26T13:45:00.123Z"`},
		{"DateTime64(6)", "2026-10-26T10:45:00.5-03:00", "2026-10-26T13:45:00.500000Z", `"2026-10-26T13:45:00.500000Z"`},
		{"DateTime64(6)", "2106-02-07T06:28:14.999999Z", "2106-02-07T06:28:14.999999Z", `"2106-02-07T06:28:14.999999Z"`},
		{"Array(String)", ` [ '/a' , 'it\'s \\ ok',bare ] `, `['/a','it\'s \\ ok','bare']`, `["/a","it's \\ ok","bare"]`},
		{"Array(UInt32)", "[1, 2,3]", "[1,2,3]", "[1,2,3]"},
		{"Array(Int64)", "[]", "[]", "[]"},
		{"Array(Date)", "[2026-10-26,'2026-10-27']", "['2026-10-26','2026-10-27']", `["2026-10-26","2026-10-27"]`},
		{"Array(Decimal(4, 1))", "['1.5',2]", "[1.5,2.0]", `["1.5","2.0"]`},
	}
	for _, tt := range tests {
		t.Run(tt.typ+" "+tt.in, func(t *testing.T) {
			t.Parallel()
			ct := typ(t, tt.typ)
			v, err := valuetext.ParseText(ct, tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(valuetext.AppendText(nil, ct, v)); got != tt.text {
				t.Errorf("text %q, want %q", got, tt.text)
			}
			if got := string(valuetext.AppendJSON(nil, ct, v)); got != tt.json {
				t.Errorf("JSON %s, want %s", got, tt.json)
			}
			back, err := valuetext.ParseJSON(ct, []byte(tt.json))
			if err != nil {
				t.Fatal(err)
			}
			if got := string(valuetext.AppendText(nil, ct, back)); got != tt.text {
				t.Errorf("text of the JSON form %q, want %q", got, tt.text)
			}
		})
	}
}

func TestParseTextErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typ, in, want string
	}{
		{"UInt8", "256", `invalid UInt8 "256": UInt8 value 256 is out of range`},
		{"UInt64", "18446744073709551616", `invalid UInt64 "18446744073709551616": out of range`},
		{"Int8", "1.5", `invalid Int8 "1.5": want an integer`},
		{"Float64", "NaN", `invalid Float64 "NaN": want a finite number`},
		{"Float64", "1e-200", `invalid Float64 "1e-200": Float64 value 1e-200 is outside DynamoDB's number range`},
		{"Decimal(10, 2)", "1.234", `invalid Decimal(10, 2) "1.234": more than 2 digits after the point`},
		{"Decimal(3, 0)", "1000", `invalid Decimal(3, 0) "1000": Decimal(3, 0) value has more than 3 digits`},
		{"Decimal(10, 2)", "1e2", `invalid Decimal(10, 2) "1e2": want a decimal number, such as -12.50`},
		{"Decimal(10, 2)", ".5", `invalid Decimal(10, 2) ".5": want a decimal number, such as -12.50`},
		{"Bool", "1", `invalid Bool "1": want true or false`},
		{"UUID", "61f0c404-5cb3-11e7-907b-a6006ad3dbaz", `invalid UUID "61f0c404-5cb3-11e7-907b-a6006ad3dbaz": want 8-4-4-4-12 hex digits`},
		{"UUID", "61f0c4045cb311e7907ba6006ad3dba0", `invalid UUID "61f0c4045cb311e7907ba6006ad3dba0": want 8-4-4-4-12 hex digits`},
		{"Date", "2026-02-30", `invalid Date "2026-02-30": want YYYY-MM-DD`},
		{"Date", "2106-02-08", `invalid Date "2106-02-08": Date value 4295030400000000 (microseconds) is outside 1970 to 2106`},
		{"DateTime64(3)", "2026-10-26", `invalid DateTime64(3) "2026-10-26": want RFC 3339 with at most 6 fractional digits, such as 2026-10-26T13:45:00.250Z`},
		{"DateTime64(6)", "2026-10-26T13:45:00.1234567Z",
			`invalid DateTime64(6) "2026-10-26T13:45:00.1234567Z": want RFC 3339 with at most 6 fractional digits, such as 2026-10-26T13:45:00.250Z`},
		{"DateTime64(3)", "1969-12-31T23:59:59.9999Z",
			`invalid DateTime64(3) "1969-12-31T23:59:59.9999Z": DateTime64(3) value -1000 (microseconds) is outside 1970 to 2106`},
		{"String", "\xff", `invalid String "\xff": not valid UTF-8`},
		{"Array(String)", "'a'", `invalid Array(String) "'a'": want an array literal, such as ['a','b'] or [1,2]`},
		{"Array(String)", "['a'", `invalid Array(String) "['a'": want an array literal, such as ['a','b'] or [1,2]`},
		{"Array(String)", "['a]", `invalid Array(String) "['a]": element 1: no closing quote`},
		{"Array(String)", "['a',]", `invalid Array(String) "['a',]": element 1: want a comma and another element, or ]`},
		{"Array(String)", "['a' 'b']", `invalid Array(String) "['a' 'b']": element 1: want a comma and another element, or ]`},
		{"Array(String)", `['a\n']`, `invalid Array(String) "['a\\n']": element 1: only \' and \\ are escapes`},
		{"Array(UInt8)", "[1,x]", `invalid Array(UInt8) "[1,x]": element 2: want an integer`},
		{"Array(UInt8)", "[1,300]", `invalid Array(UInt8) "[1,300]": element 2: UInt8 value 300 is out of range`},
	}
	for _, tt := range tests {
		t.Run(tt.typ+" "+tt.in, func(t *testing.T) {
			t.Parallel()
			_, err := valuetext.ParseText(typ(t, tt.typ), tt.in)
			if err == nil || err.Error() != tt.want {
				t.Errorf("got %v, want %s", err, tt.want)
			}
		})
	}
}

func TestParseJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typ, in, text string
	}{
		{"Int64", "1e2", "100"},
		{"UInt8", "2.50e1", "25"},
		{"UInt32", `"7"`, "7"},
		{"Int64", "-9223372036854775808", "-9223372036854775808"},
		{"Decimal(10, 2)", "1.5e1", "15.00"},
		{"Decimal(10, 2)", "12", "12.00"},
		{"Decimal(10, 2)", "-0.0e-99999", "0.00"},
		{"Float64", " 2.5 ", "2.5"},
		{"Nullable(String)", "null", "NULL"},
		{"Nullable(UInt8)", "3", "3"},
	}
	for _, tt := range tests {
		t.Run(tt.typ+" "+tt.in, func(t *testing.T) {
			t.Parallel()
			ct := typ(t, tt.typ)
			v, err := valuetext.ParseJSON(ct, []byte(tt.in))
			if err != nil {
				t.Fatal(err)
			}
			if got := string(valuetext.AppendText(nil, ct, v)); got != tt.text {
				t.Errorf("got %q, want %q", got, tt.text)
			}
		})
	}
}

func TestParseJSONErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typ, in, want string
	}{
		{"UInt8", "1.5", "invalid UInt8: want an integer"},
		{"Int64", "1e30", "invalid Int64: out of range"},
		{"Decimal(10, 2)", "1e-3", "invalid Decimal(10, 2): more than 2 digits after the point"},
		{"Decimal(10, 2)", "1e99999999999999999999", "invalid Decimal(10, 2): want a decimal number, such as -12.50"},
		{"Decimal(10, 2)", "1e40", "invalid Decimal(10, 2): more than 38 digits"},
		// Exponents at int's limits, which the scale and the fraction's length would overflow.
		{"Int64", "1e-9223372036854775808", "invalid Int64: want an integer"},
		{"Decimal(10, 1)", "1e9223372036854775807", "invalid Decimal(10, 1): more than 38 digits"},
		{"Decimal(10, 2)", "1.5e-9223372036854775808", "invalid Decimal(10, 2): more than 2 digits after the point"},
		{"String", "1", "invalid String: want a JSON string"},
		{"Date", "true", "invalid Date: want a JSON string"},
		{"Float64", `"1.5"`, "invalid Float64: want a JSON number"},
		{"Bool", `"true"`, "invalid Bool: want true or false"},
		{"UInt8", "{}", "invalid UInt8: want a JSON number or string"},
		{"String", "null", "invalid String: not Nullable"},
		{"Array(UInt8)", "[1,null]", "invalid Array(UInt8): element 2: want a JSON number or string"},
		{"Array(UInt8)", `"[1]"`, "invalid Array(UInt8): want a JSON array"},
		{"UInt8", "1 2", "invalid UInt8: more than one JSON value"},
		{"UInt8", "", "invalid UInt8: EOF"},
	}
	for _, tt := range tests {
		t.Run(tt.typ+" "+tt.in, func(t *testing.T) {
			t.Parallel()
			_, err := valuetext.ParseJSON(typ(t, tt.typ), []byte(tt.in))
			if err == nil || err.Error() != tt.want {
				t.Errorf("got %v, want %s", err, tt.want)
			}
		})
	}
}
