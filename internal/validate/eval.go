// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"errors"
	"math"
	"math/big"
	"slices"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// The compiler computes DEFAULT values (sql.md §8.2) with this evaluator of constant IR:
// every operator and scalar function, with the semantics of §5 and §6.

// cval is a constant value. Which field holds it depends on its type.
type cval struct {
	null bool
	num  *big.Rat // integers and Decimals, exactly
	f    float64  // Float64
	str  string   // String and UUID
	b    bool
	tm   time.Time // Date, DateTime and DateTime64, in UTC
}

// errEval is an evaluation error (sql.md §5.8): the value fails to compute.
var errEval = errors.New("evaluation error")

const decimalBase = 10

func isOneOf(op catalog.Op, ops ...catalog.Op) bool {
	return slices.Contains(ops, op)
}

func eval(e catalog.Expr) (cval, error) {
	switch {
	case e.Op == catalog.OpLiteral:
		return parseLiteral(e)
	case e.Op == catalog.OpNull:
		return cval{null: true}, nil
	case isOneOf(e.Op, catalog.OpAnd, catalog.OpOr):
		return evalLogic(e)
	case isOneOf(e.Op, catalog.OpIf, catalog.OpMultiIf, catalog.OpCoalesce):
		return evalChoice(e)
	case e.Op == catalog.OpIsNull:
		v, err := eval(e.Args[0])
		return cval{b: v.null}, err
	}
	args := make([]cval, len(e.Args))
	for i, a := range e.Args {
		v, err := eval(a)
		if err != nil || v.null {
			return v, err
		}
		args[i] = v
	}
	return apply(e, args)
}

// apply evaluates an operation whose arguments aren't NULL.
func apply(e catalog.Expr, args []cval) (cval, error) {
	switch {
	case e.Op == catalog.OpNot:
		return cval{b: !args[0].b}, nil
	case e.Op == catalog.OpIn:
		return evalIn(e, args), nil
	case compareSymbol(e.Op) != "":
		c := compareValues(args[0], e.Args[0].Type, args[1], e.Args[1].Type)
		return cval{b: compareResult(compareSymbol(e.Op), c)}, nil
	case arithSymbol(e.Op) != "":
		return evalArith(e, args)
	}
	for _, f := range scalarEvaluators() {
		if f.op == e.Op {
			return f.eval(args)
		}
	}
	// Columns, parameters, aggregates and arrays are never part of a constant.
	return cval{}, errEval
}

type opSymbol struct {
	op     catalog.Op
	symbol string
}

func symbolOf(op catalog.Op, table []opSymbol) string {
	for _, s := range table {
		if s.op == op {
			return s.symbol
		}
	}
	return ""
}

const opNegate = "negate"

func arithSymbol(op catalog.Op) string {
	return symbolOf(op, []opSymbol{
		{catalog.OpPlus, opPlus}, {catalog.OpMinus, opMinus}, {catalog.OpMultiply, opTimes},
		{catalog.OpDivide, opDivide}, {catalog.OpModulo, opModulo}, {catalog.OpIntDiv, opIntDiv},
		{catalog.OpNegate, opNegate},
	})
}

func compareSymbol(op catalog.Op) string {
	return symbolOf(op, []opSymbol{
		{catalog.OpEquals, "="}, {catalog.OpNotEquals, "!="}, {catalog.OpLess, "<"},
		{catalog.OpLessOrEquals, "<="}, {catalog.OpGreater, ">"}, {catalog.OpGreaterOrEquals, ">="},
	})
}

func parseLiteral(e catalog.Expr) (cval, error) {
	t := e.Type
	switch {
	case isFloat(t):
		f, err := strconv.ParseFloat(e.Value, 64)
		return cval{f: f}, err
	case isNumeric(t):
		r, ok := new(big.Rat).SetString(e.Value)
		if !ok {
			return cval{}, errEval
		}
		return cval{num: r}, nil
	case isBool(t):
		return cval{b: e.Value == "true"}, nil
	case t.Kind == catalog.KindDate:
		tm, err := time.Parse(dateLayout, e.Value)
		return cval{tm: tm}, err
	case isTemporal(t):
		tm, err := time.Parse(dateTimeLayout+".999999", e.Value)
		return cval{tm: tm}, err
	}
	return cval{str: e.Value}, nil
}

// num returns the exact value of a number of type t.
func num(v cval, t catalog.Type) *big.Rat {
	if isFloat(t) {
		return new(big.Rat).SetFloat64(v.f)
	}
	return v.num
}

// toFloat returns the Float64 nearest to a number of type t.
func toFloat(v cval, t catalog.Type) float64 {
	if isFloat(t) {
		return v.f
	}
	f, _ := v.num.Float64()
	return f
}

func evalArith(e catalog.Expr, args []cval) (cval, error) {
	sym := arithSymbol(e.Op)
	if isFloat(e.Type) {
		return evalFloat(sym, e, args)
	}
	x := num(args[0], e.Args[0].Type)
	if sym == opNegate {
		return fitResult(new(big.Rat).Neg(x), e.Type)
	}
	y := num(args[1], e.Args[1].Type)
	if (sym == opDivide || sym == opModulo || sym == opIntDiv) && y.Sign() == 0 {
		return cval{}, errEval
	}
	if isInteger(e.Type) {
		return evalInteger(sym, e, x.Num(), y.Num())
	}
	var r *big.Rat
	switch sym {
	case opPlus:
		r = new(big.Rat).Add(x, y)
	case opMinus:
		r = new(big.Rat).Sub(x, y)
	case opTimes:
		r = new(big.Rat).Mul(x, y)
	default:
		r = truncate(new(big.Rat).Quo(x, y), e.Type.Scale)
	}
	return fitResult(r, e.Type)
}

func evalFloat(sym string, e catalog.Expr, args []cval) (cval, error) {
	x := toFloat(args[0], e.Args[0].Type)
	var f float64
	if sym == opNegate {
		f = -x
	} else {
		y := toFloat(args[1], e.Args[1].Type)
		switch sym {
		case opPlus:
			f = x + y
		case opMinus:
			f = x - y
		case opTimes:
			f = x * y
		default:
			f = x / y
		}
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return cval{}, errEval
	}
	// Adding 0 folds -0.0 into 0.0 (sql.md Dev 5).
	return cval{f: f + 0}, nil
}

// evalInteger computes an integer operation exactly, then wraps it into the result type,
// as ClickHouse does (sql.md §5.6).
func evalInteger(sym string, e catalog.Expr, x, y *big.Int) (cval, error) {
	var r *big.Int
	switch sym {
	case opPlus:
		r = new(big.Int).Add(x, y)
	case opMinus:
		r = new(big.Int).Sub(x, y)
	case opTimes:
		r = new(big.Int).Mul(x, y)
	default:
		// The minimum of the dividend's type divided by -1 fails, as in ClickHouse.
		lo, _ := intRange(e.Args[0].Type)
		if isSigned(e.Args[0].Type.Kind) && x.Cmp(lo) == 0 && y.Cmp(big.NewInt(-1)) == 0 {
			return cval{}, errEval
		}
		if sym == opModulo {
			r = new(big.Int).Rem(x, y)
		} else {
			r = new(big.Int).Quo(x, y)
		}
	}
	return cval{num: new(big.Rat).SetInt(wrap(r, e.Type))}, nil
}

// wrap reduces x into the range of the integer type t, in two's complement.
func wrap(x *big.Int, t catalog.Type) *big.Int {
	mod := new(big.Int).Lsh(big.NewInt(1), uint(intBits(t.Kind)))
	r := new(big.Int).Mod(x, mod)
	if isSigned(t.Kind) && r.Cmp(new(big.Int).Rsh(mod, 1)) >= 0 {
		r.Sub(r, mod)
	}
	return r
}

// intRange returns the smallest and the largest value of an integer type.
func intRange(t catalog.Type) (*big.Int, *big.Int) {
	bits := uint(intBits(t.Kind))
	if !isSigned(t.Kind) {
		hi := new(big.Int).Lsh(big.NewInt(1), bits)
		return big.NewInt(0), hi.Sub(hi, big.NewInt(1))
	}
	hi := new(big.Int).Lsh(big.NewInt(1), bits-1)
	lo := new(big.Int).Neg(hi)
	return lo, hi.Sub(hi, big.NewInt(1))
}

// truncate cuts r toward zero to scale fractional digits.
func truncate(r *big.Rat, scale int) *big.Rat {
	pow := pow10(scale)
	m := new(big.Int).Quo(new(big.Int).Mul(r.Num(), pow), r.Denom())
	return new(big.Rat).SetFrac(m, pow)
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(decimalBase), big.NewInt(int64(n)), nil)
}

// fitResult wraps an integer result, and checks a Decimal result against the storage of
// its precision class (sql.md §5.6): 32 bits for 9, 64 bits for 18, and 38 digits for 38.
func fitResult(r *big.Rat, t catalog.Type) (cval, error) {
	if isInteger(t) {
		return cval{num: new(big.Rat).SetInt(wrap(r.Num(), t))}, nil
	}
	m := new(big.Int).Mul(r.Num(), pow10(t.Scale))
	m.Quo(m, r.Denom())
	var fits bool
	switch decimalClass(t.Precision) {
	case class9:
		fits = m.IsInt64() && m.Int64() >= math.MinInt32 && m.Int64() <= math.MaxInt32
	case class18:
		fits = m.IsInt64()
	default:
		fits = new(big.Int).Abs(m).Cmp(pow10(class38)) < 0
	}
	if !fits {
		return cval{}, errEval
	}
	return cval{num: r}, nil
}
