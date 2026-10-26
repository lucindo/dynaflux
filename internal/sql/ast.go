// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package sql parses Dynaflux SQL files into a syntax tree that keeps the
// shape and positions of the source. It reports lexical, syntax and annotation
// syntax errors; the compiler checks every other rule on the tree.
package sql

import (
	"fmt"

	"github.com/lucindo/dynaflux/internal/diag"
)

// Span is where a node sits in its file.
type Span struct {
	Pos   diag.Pos // the node's first token
	Start int      // byte offset of the first token in File.Src
	End   int      // byte offset just past the node's last token
}

// Extent returns s, so that every node that embeds a Span is a Node.
func (s Span) Extent() Span { return s }

// Node is any syntax node.
type Node interface {
	Extent() Span
}

// File is a parsed SQL file.
type File struct {
	Path  string
	Src   []byte
	Stmts []Stmt
}

// Stmt is a statement: *CreateStream, *CreateView or *Query.
//
//sumtype:decl
type Stmt interface {
	Node
	Header() StmtHeader
	stmtNode()
}

// StmtHeader holds what every statement has.
type StmtHeader struct {
	Annotations []Annotation
	// Broken marks a statement with a lexical or syntax error. It keeps only
	// its span, annotations and name: the name still counts as defined, but
	// nothing else in the statement may be checked. A stream or view name is
	// "" when the error came before it, and then defines nothing.
	Broken bool
}

// Header returns h, so that every statement that embeds a StmtHeader is a
// Stmt.
func (h StmtHeader) Header() StmtHeader { return h }

// CreateStream is a CREATE STREAM statement. Each clause is nil when absent.
type CreateStream struct {
	Span
	StmtHeader

	Name         Ident
	Columns      []*Column
	PrimaryKey   *NameList
	DedupBy      *NameList
	LogRetention *LogRetention
	Accept       *ExprClause
	Shards       *IntClause
	Consistency  *Consistency
}

// Column is a column definition of a stream.
type Column struct {
	Span

	Name    Ident
	Type    *Type
	Default *ExprClause
	PII     *PII
	// Options are the ClickHouse column options that Dynaflux rejects (DF2002).
	Options []ColumnOption
}

// PII is the reserved PII column tag (DF2005).
type PII struct {
	Span

	Category *StringLit // nil for a bare PII
}

// ColumnOption is a ClickHouse column option such as CODEC(...).
type ColumnOption struct {
	Span

	Name string // NULL, NOT NULL, MATERIALIZED, ALIAS, EPHEMERAL, CODEC, TTL or COMMENT
}

// LogRetention is LOG RETENTION [-]n unit, or LOG RETENTION FOREVER.
type LogRetention struct {
	Span

	Forever bool
	Neg     bool       // a - came before the number
	Value   *NumberLit // nil when Forever
	Unit    Ident
}

// Consistency is the reserved CONSISTENCY = '<mode>' clause (DF2108).
type Consistency struct {
	Span

	Value *StringLit
}

// CreateView is a CREATE MATERIALIZED VIEW statement. Each clause is nil when
// absent.
type CreateView struct {
	Span
	StmtHeader

	Name       Ident
	Mode       *Mode
	OrderBy    *NameList
	PrimaryKey *NameList
	TTL        *ExprClause
	Split      *IntClause
	Select     *Select
}

// Mode is MODE ASYNC or MODE SYNC.
type Mode struct {
	Span

	Sync bool
}

// Query is a top-level SELECT: a named query when it has a -- name: annotation.
type Query struct {
	Span
	StmtHeader

	Select *Select // nil when Broken
}

// Select is a SELECT of a view or a query. Each clause is nil when absent.
type Select struct {
	Span

	Items      []*SelectItem
	From       Ident
	ArrayJoins []*ArrayJoin
	Where      *ExprClause
	GroupBy    *GroupBy
	OrderBy    *OrderBy
	Limit      *ExprClause // X is a *NumberLit or a *Param
}

// SelectItem is one item of a SELECT list.
type SelectItem struct {
	Span

	Star  bool // SELECT *; X and Alias are nil
	X     Expr
	Alias *Ident
}

// ArrayJoin is one [LEFT] ARRAY JOIN clause.
type ArrayJoin struct {
	Span

	Left  bool
	Items []*ArrayItem
}

// ArrayItem is one array of an ARRAY JOIN.
type ArrayItem struct {
	Span

	X     Expr
	Alias *Ident
}

// GroupBy is a GROUP BY clause.
type GroupBy struct {
	Span

	Items []Expr
}

// OrderBy is the ORDER BY clause of a SELECT.
type OrderBy struct {
	Span

	Items []*OrderItem
}

// OrderItem is one item of a SELECT's ORDER BY.
type OrderItem struct {
	Span

	X          Expr
	Desc       bool
	NullsFirst bool // NULLS FIRST; NULLS LAST and no NULLS clause leave it false
}

// ExprClause is a clause made of keywords and one expression, such as WHERE or
// a column's DEFAULT. Its span starts at the first keyword.
type ExprClause struct {
	Span

	X Expr
}

// IntClause is a clause that takes an integer, such as SHARDS or SPLIT.
type IntClause struct {
	Span

	Value *NumberLit
}

// NameList is a clause that lists names, such as PRIMARY KEY (a, b). Its span
// starts at the clause's first keyword.
type NameList struct {
	Span

	Names []Ident
}

// AnnotationKey is the key of an annotation.
type AnnotationKey uint8

// Annotation keys.
const (
	AnnotationName   AnnotationKey = iota + 1 // -- name: <Name> :one|:many
	AnnotationExpose                          // -- expose: http
	AnnotationGoType                          // -- go:type <Name>
)

func (k AnnotationKey) String() string {
	switch k {
	case AnnotationName:
		return "name"
	case AnnotationExpose:
		return "expose"
	case AnnotationGoType:
		return "go:type"
	default:
		return fmt.Sprintf("AnnotationKey(%d)", uint8(k))
	}
}

// Annotation is an annotation comment with a known key. It belongs to the
// statement that follows it, whatever kind that statement is.
type Annotation struct {
	Span

	Key AnnotationKey
	// Bad marks a malformed annotation or one with an unknown value, which the
	// parser reported (DF1201, DF1202). It still counts as present, so that a
	// query with a bad -- name: isn't also reported as unnamed (DF1104), but
	// nothing else may be checked on it.
	Bad bool
	// Name is the <Name> of -- name: and -- go:type, as written; "" when the
	// annotation is malformed. The parser doesn't check that it is a valid Go
	// name (DF1206).
	Name    string
	NamePos diag.Pos // where Name starts
	Many    bool     // -- name: ... :many rather than :one
}

// Ident is an identifier as written: a name, a type name or a unit. The span
// of a qualified name covers all its parts.
type Ident struct {
	Span

	Name  string // without quotes
	Quote byte   // '"' or '`' for a quoted identifier, 0 otherwise
	// Qualifier holds the leading parts of a qualified name such as t.col,
	// which the compiler rejects (DF5102). Name is the last part.
	Qualifier []Ident
}

// Type is a type such as Nullable(String) or Decimal(10, 2).
type Type struct {
	Span

	Name   Ident
	Parens bool // written with an argument list, even an empty one: Array()
	Args   []TypeArg
}

// TypeArg is an argument of a type: *Type, *NumberLit (an integer),
// *StringLit or *EnumValue.
//
//sumtype:decl
type TypeArg interface {
	Node
	typeArg()
}

// EnumValue is a value of a ClickHouse Enum type, such as 'a' = 1 in
// Enum8('a' = 1). The parser accepts it so that the compiler can reject the
// type by name (DF5002).
type EnumValue struct {
	Span

	Name  *StringLit
	Value *NumberLit
}

// Expr is an expression.
//
//sumtype:decl
type Expr interface {
	Node
	exprNode()
}

// NumberLit is a number literal.
type NumberLit struct {
	Span

	// Neg marks a negative literal: a - in prefix position right before the
	// number, as in -1. Int and Float hold the magnitude.
	Neg     bool
	Text    string  // digits, point and exponent as written, without underscores
	IsFloat bool    // written with a point or an exponent
	Int     uint64  // the value of an integer
	Float   float64 // the value of a float: the float64 nearest to Text
}

// StringLit is a string literal.
type StringLit struct {
	Span

	Value string // with escapes applied
}

// BoolLit is TRUE or FALSE.
type BoolLit struct {
	Span

	Value bool
}

// NullLit is NULL.
type NullLit struct {
	Span
}

// Param is a query parameter such as {page:String}.
type Param struct {
	Span

	Name Ident
	Type *Type
}

// ColumnRef is a name used in an expression.
type ColumnRef struct {
	Ident
}

// Call is a function call. A call with two argument lists, such as
// quantile(0.9)(x), is Parametric: the first list is Params.
type Call struct {
	Span

	Name        Ident
	Star        bool     // the first argument list is *
	Distinct    bool     // DISTINCT starts the first argument list
	DistinctPos diag.Pos // where DISTINCT is written, when Distinct is set
	Parametric  bool
	Params      []Expr
	Args        []Expr
}

// Interval is INTERVAL [-] (number | string) [unit] [TO unit]. The unit is
// left out only after a string, as in INTERVAL '2 days': Unit is then zero.
type Interval struct {
	Span

	Neg   bool // a - came before the value
	Value Expr // *NumberLit (not Neg) or *StringLit
	Unit  Ident
	To    *Ident
}

// ArrayLit is an array literal such as ['a', 'b'].
type ArrayLit struct {
	Span

	Elems []Expr
}

// ParenExpr is a parenthesized expression. The parser keeps it because
// parentheses change meaning: -(1) isn't the literal -1, and (a = b) = c isn't
// a chained comparison.
type ParenExpr struct {
	Span

	X Expr
}

// UnaryOp is a prefix operator.
type UnaryOp uint8

// Prefix operators.
const (
	OpNot UnaryOp = iota + 1
	OpNeg
)

func (op UnaryOp) String() string {
	switch op {
	case OpNot:
		return kwNot
	case OpNeg:
		return "-"
	default:
		return fmt.Sprintf("UnaryOp(%d)", uint8(op))
	}
}

// UnaryExpr is NOT x or -x. Its span starts at the operator.
type UnaryExpr struct {
	Span

	Op UnaryOp
	X  Expr
}

// BinaryOp is an infix operator. == and <> parse as OpEq and OpNe.
type BinaryOp uint8

// Infix operators.
const (
	OpOr BinaryOp = iota + 1
	OpAnd
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAdd
	OpSub
	OpMul
	OpDiv
	OpMod
)

func (op BinaryOp) String() string {
	switch op {
	case OpOr:
		return "OR"
	case OpAnd:
		return kwAnd
	case OpEq:
		return "="
	case OpNe:
		return "!="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpMod:
		return "%"
	default:
		return fmt.Sprintf("BinaryOp(%d)", uint8(op))
	}
}

// BinaryExpr is x op y.
//
// Comparisons don't chain (DF5304). The parser builds a chain such as
// a = b = c left-nested, so a comparison, IN, BETWEEN or IS NULL whose X is
// another one, not wrapped in a ParenExpr, is a chain; OpPos is where the
// diagnostic points.
type BinaryExpr struct {
	Span

	Op    BinaryOp
	OpPos diag.Pos
	X, Y  Expr
}

// InExpr is x [NOT] IN (v, ...) or x [NOT] IN {p:Array(T)}. OpPos is the
// position of NOT or IN.
type InExpr struct {
	Span

	X     Expr
	Not   bool
	OpPos diag.Pos
	List  []Expr // the parenthesized list; nil when Param is set
	Param *Param
}

// BetweenExpr is x [NOT] BETWEEN lo AND hi. OpPos is the position of NOT or
// BETWEEN.
type BetweenExpr struct {
	Span

	X      Expr
	Not    bool
	OpPos  diag.Pos
	Lo, Hi Expr
}

// IsNullExpr is x IS [NOT] NULL. OpPos is the position of IS.
type IsNullExpr struct {
	Span

	X     Expr
	Not   bool
	OpPos diag.Pos
}

func (*CreateStream) stmtNode() {}
func (*CreateView) stmtNode()   {}
func (*Query) stmtNode()        {}

func (*Type) typeArg()      {}
func (*NumberLit) typeArg() {}
func (*StringLit) typeArg() {}
func (*EnumValue) typeArg() {}

func (*NumberLit) exprNode()   {}
func (*StringLit) exprNode()   {}
func (*BoolLit) exprNode()     {}
func (*NullLit) exprNode()     {}
func (*Param) exprNode()       {}
func (*ColumnRef) exprNode()   {}
func (*Call) exprNode()        {}
func (*Interval) exprNode()    {}
func (*ArrayLit) exprNode()    {}
func (*ParenExpr) exprNode()   {}
func (*UnaryExpr) exprNode()   {}
func (*BinaryExpr) exprNode()  {}
func (*InExpr) exprNode()      {}
func (*BetweenExpr) exprNode() {}
func (*IsNullExpr) exprNode()  {}
