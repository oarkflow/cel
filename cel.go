package cel

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Value represents any runtime value in CEL
type Value = any

// Context represents the evaluation context with variables and functions
type Context struct {
	Variables map[string]Value
	Functions map[string]Function
	timeNow   func() time.Time
	pool      *StringPool
}

// Context implements context.Context interface
func (c *Context) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c *Context) Done() <-chan struct{} {
	return nil
}

func (c *Context) Err() error {
	return nil
}

func (c *Context) Value(key interface{}) interface{} {
	return nil
}

// Function represents a callable function
type Function interface {
	Call(ctx context.Context, args ...Value) (Value, error)
}

// MethodHandler represents a method callable on a value
type MethodHandler func(ctx context.Context, receiver Value, args ...Value) (Value, error)

// Expression represents a parsed and compiled expression
type Expression struct {
	ast       ASTNode
	optimized bool
}

// Evaluate evaluates the expression against the given context
func (e *Expression) Evaluate(ctx *Context) (Value, error) {
	if e.ast == nil {
		return nil, fmt.Errorf("expression not parsed")
	}
	return e.ast.Evaluate(ctx)
}

// Parse parses the expression and returns an expression object
func (p *Parser) Parse() (*Expression, error) {
	tokens, err := p.tokenize()
	if err != nil {
		return nil, err
	}
	p.tokens = tokens
	ast, err := p.parseExpression(0)
	if err != nil {
		return nil, err
	}

	return &Expression{ast: ast}, nil
}

// Parser parses CEL expressions
type Parser struct {
	expr      string
	tokens    []Token
	pos       int
	functions map[string]Function
}

// NewParser creates a new parser for the given expression
func NewParser(expr string) *Parser {
	return &Parser{
		expr:      expr,
		functions: make(map[string]Function),
	}
}

// ASTNode represents a node in the Abstract Syntax Tree
type ASTNode interface {
	Evaluate(ctx *Context) (Value, error)
	String() string
}

// Token represents a lexical token
type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

// TokenType represents the type of token
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenNumber
	TokenString
	TokenIdentifier
	TokenOperator
	TokenKeyword
	TokenPunctuation
)

// AST Node Types
type (
	// Literal nodes
	NumberLiteral struct {
		Value float64
		raw   string
	}

	StringLiteral struct {
		Value string
		raw   string
	}

	BooleanLiteral struct {
		Value bool
		raw   string
	}

	NullLiteral struct {
		Value Value
	}

	ArrayLiteral struct {
		Elements []ASTNode
	}

	MapLiteral struct {
		Pairs map[ASTNode]ASTNode
	}

	// Variable and identifier nodes
	Identifier struct {
		Name string
	}

	// Operation nodes
	BinaryOp struct {
		Op    string
		Left  ASTNode
		Right ASTNode
	}

	UnaryOp struct {
		Op   string
		Expr ASTNode
	}

	Ternary struct {
		Cond ASTNode
		Then ASTNode
		Else ASTNode
	}

	// Function and method call nodes
	FunctionCall struct {
		Name      string
		Arguments []ASTNode
	}

	MethodCall struct {
		Object    ASTNode
		Method    string
		Arguments []ASTNode
	}

	// Collection operations
	Filter struct {
		Variable  string
		Source    ASTNode
		Predicate ASTNode
	}

	Map struct {
		Variable  string
		Source    ASTNode
		Transform ASTNode
	}

	All struct {
		Variable  string
		Source    ASTNode
		Predicate ASTNode
	}

	Exists struct {
		Variable  string
		Source    ASTNode
		Predicate ASTNode
	}

	Find struct {
		Variable  string
		Source    ASTNode
		Predicate ASTNode
	}

	Size struct {
		Expr ASTNode
	}

	First struct {
		Expr ASTNode
	}

	Last struct {
		Expr ASTNode
	}
)

// String methods for AST nodes
func (n *NumberLiteral) String() string  { return n.raw }
func (n *StringLiteral) String() string  { return n.raw }
func (n *BooleanLiteral) String() string { return n.raw }
func (n *NullLiteral) String() string    { return "null" }
func (n *ArrayLiteral) String() string   { return "[]" }
func (n *MapLiteral) String() string     { return "{}" }
func (n *Identifier) String() string     { return n.Name }
func (n *BinaryOp) String() string       { return fmt.Sprintf("(%s %s %s)", n.Left, n.Op, n.Right) }
func (n *UnaryOp) String() string        { return fmt.Sprintf("(%s %s)", n.Op, n.Expr) }
func (n *Ternary) String() string        { return fmt.Sprintf("(%s ? %s : %s)", n.Cond, n.Then, n.Else) }
func (n *FunctionCall) String() string   { return fmt.Sprintf("%s(...)", n.Name) }
func (n *MethodCall) String() string     { return fmt.Sprintf("%s.%s(...)", n.Object, n.Method) }
func (n *Filter) String() string         { return "filter(...)" }
func (n *Map) String() string            { return "map(...)" }
func (n *All) String() string            { return "all(...)" }
func (n *Exists) String() string         { return "exists(...)" }
func (n *Find) String() string           { return "find(...)" }
func (n *Size) String() string           { return "size(...)" }
func (n *First) String() string          { return "first(...)" }
func (n *Last) String() string           { return "last(...)" }

// Evaluate implementations for AST nodes
func (n *NumberLiteral) Evaluate(ctx *Context) (Value, error) {
	return n.Value, nil
}

func (n *StringLiteral) Evaluate(ctx *Context) (Value, error) {
	return n.Value, nil
}

func (n *BooleanLiteral) Evaluate(ctx *Context) (Value, error) {
	return n.Value, nil
}

func (n *NullLiteral) Evaluate(ctx *Context) (Value, error) {
	return n.Value, nil
}

func (n *ArrayLiteral) Evaluate(ctx *Context) (Value, error) {
	values := make([]Value, 0, len(n.Elements))
	for _, elem := range n.Elements {
		val, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func (n *MapLiteral) Evaluate(ctx *Context) (Value, error) {
	result := make(map[string]Value, len(n.Pairs))
	for keyNode, valNode := range n.Pairs {
		key, err := keyNode.Evaluate(ctx)
		if err != nil {
			return nil, err
		}
		keyStr, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("map key must be string, got %T", key)
		}

		val, err := valNode.Evaluate(ctx)
		if err != nil {
			return nil, err
		}
		result[keyStr] = val
	}
	return result, nil
}

func (n *Identifier) Evaluate(ctx *Context) (Value, error) {
	if val, ok := ctx.Variables[n.Name]; ok {
		return val, nil
	}

	// Check for built-in functions
	if fn, ok := ctx.Functions[n.Name]; ok {
		return fn, nil
	}

	return nil, fmt.Errorf("undefined variable: %s", n.Name)
}

func (n *BinaryOp) Evaluate(ctx *Context) (Value, error) {
	left, err := n.Left.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	right, err := n.Right.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return evaluateBinaryOp(n.Op, left, right, ctx)
}

func (n *UnaryOp) Evaluate(ctx *Context) (Value, error) {
	expr, err := n.Expr.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return evaluateUnaryOp(n.Op, expr, ctx)
}

func (n *Ternary) Evaluate(ctx *Context) (Value, error) {
	cond, err := n.Cond.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	condBool, ok := cond.(bool)
	if !ok {
		return nil, fmt.Errorf("ternary condition must be boolean, got %T", cond)
	}

	if condBool {
		return n.Then.Evaluate(ctx)
	}
	return n.Else.Evaluate(ctx)
}

func (n *FunctionCall) Evaluate(ctx *Context) (Value, error) {
	// Check for collection operations that need specialized handling
	if n.Name == "filter" || n.Name == "map" || n.Name == "all" || n.Name == "exists" || n.Name == "find" {
		return n.evaluateCollectionOperation(ctx)
	}

	// First try built-in functions
	if fn, ok := builtinFunctions[n.Name]; ok {
		if fn == nil {
			return nil, fmt.Errorf("builtin function %s is nil", n.Name)
		}
		args, err := evaluateArgs(n.Arguments, ctx)
		if err != nil {
			return nil, err
		}
		return fn(ctx, args...)
	}

	// Then try custom functions
	if fn, ok := ctx.Functions[n.Name]; ok {
		if fn == nil {
			return nil, fmt.Errorf("function %s is nil", n.Name)
		}
		args, err := evaluateArgs(n.Arguments, ctx)
		if err != nil {
			return nil, err
		}
		return fn.Call(ctx, args...)
	}

	return nil, fmt.Errorf("undefined function: %s", n.Name)
}

// evaluateCollectionOperation handles collection operations with variable scoping
func (n *FunctionCall) evaluateCollectionOperation(ctx *Context) (Value, error) {
	if len(n.Arguments) != 3 {
		return nil, fmt.Errorf("%s() requires 3 arguments", n.Name)
	}

	// Parse variable name
	variableNode, ok := n.Arguments[0].(*Identifier)
	if !ok {
		return nil, fmt.Errorf("%s() first argument must be variable name", n.Name)
	}

	source, err := n.Arguments[1].Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	slice, ok := source.([]Value)
	if !ok {
		return nil, fmt.Errorf("%s() second argument must be array, got %T", n.Name, source)
	}

	predicate := n.Arguments[2]

	switch n.Name {
	case "filter":
		result := make([]Value, 0, len(slice))
		for _, item := range slice {
			// Save current variables
			oldVal := ctx.Variables[variableNode.Name]
			ctx.Variables[variableNode.Name] = item

			keep, err := predicate.Evaluate(ctx)
			if err != nil {
				return nil, err
			}

			if keep.(bool) {
				result = append(result, item)
			}

			// Restore variable
			if oldVal != nil {
				ctx.Variables[variableNode.Name] = oldVal
			} else {
				delete(ctx.Variables, variableNode.Name)
			}
		}
		return result, nil

	case "map":
		result := make([]Value, 0, len(slice))
		for _, item := range slice {
			// Save current variables
			oldVal := ctx.Variables[variableNode.Name]
			ctx.Variables[variableNode.Name] = item

			transformed, err := predicate.Evaluate(ctx)
			if err != nil {
				return nil, err
			}

			result = append(result, transformed)

			// Restore variable
			if oldVal != nil {
				ctx.Variables[variableNode.Name] = oldVal
			} else {
				delete(ctx.Variables, variableNode.Name)
			}
		}
		return result, nil

	case "all":
		for _, item := range slice {
			// Save current variables
			oldVal := ctx.Variables[variableNode.Name]
			ctx.Variables[variableNode.Name] = item

			keep, err := predicate.Evaluate(ctx)
			if err != nil {
				return nil, err
			}

			if !keep.(bool) {
				// Restore variable
				if oldVal != nil {
					ctx.Variables[variableNode.Name] = oldVal
				} else {
					delete(ctx.Variables, variableNode.Name)
				}
				return false, nil
			}

			// Restore variable
			if oldVal != nil {
				ctx.Variables[variableNode.Name] = oldVal
			} else {
				delete(ctx.Variables, variableNode.Name)
			}
		}
		return true, nil

	case "exists":
		for _, item := range slice {
			// Save current variables
			oldVal := ctx.Variables[variableNode.Name]
			ctx.Variables[variableNode.Name] = item

			keep, err := predicate.Evaluate(ctx)
			if err != nil {
				return nil, err
			}

			if keep.(bool) {
				// Restore variable
				if oldVal != nil {
					ctx.Variables[variableNode.Name] = oldVal
				} else {
					delete(ctx.Variables, variableNode.Name)
				}
				return true, nil
			}

			// Restore variable
			if oldVal != nil {
				ctx.Variables[variableNode.Name] = oldVal
			} else {
				delete(ctx.Variables, variableNode.Name)
			}
		}
		return false, nil

	case "find":
		for _, item := range slice {
			// Save current variables
			oldVal := ctx.Variables[variableNode.Name]
			ctx.Variables[variableNode.Name] = item

			found, err := predicate.Evaluate(ctx)
			if err != nil {
				return nil, err
			}

			if found.(bool) {
				// Restore variable
				if oldVal != nil {
					ctx.Variables[variableNode.Name] = oldVal
				} else {
					delete(ctx.Variables, variableNode.Name)
				}
				return item, nil
			}

			// Restore variable
			if oldVal != nil {
				ctx.Variables[variableNode.Name] = oldVal
			} else {
				delete(ctx.Variables, variableNode.Name)
			}
		}
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown collection operation: %s", n.Name)
	}
}

func (n *MethodCall) Evaluate(ctx *Context) (Value, error) {
	object, err := n.Object.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	args, err := evaluateArgs(n.Arguments, ctx)
	if err != nil {
		return nil, err
	}

	return callMethod(ctx, object, n.Method, args)
}

func (n *Filter) Evaluate(ctx *Context) (Value, error) {
	source, err := n.Source.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	slice, ok := source.([]Value)
	if !ok {
		return nil, fmt.Errorf("filter source must be array, got %T", source)
	}

	result := make([]Value, 0, len(slice))
	for _, item := range slice {
		// Save current variables
		oldVal := ctx.Variables[n.Variable]
		ctx.Variables[n.Variable] = item

		keep, err := n.Predicate.Evaluate(ctx)
		if err != nil {
			return nil, err
		}

		if keep.(bool) {
			result = append(result, item)
		}

		// Restore variable
		if oldVal != nil {
			ctx.Variables[n.Variable] = oldVal
		} else {
			delete(ctx.Variables, n.Variable)
		}
	}

	return result, nil
}

func (n *Map) Evaluate(ctx *Context) (Value, error) {
	source, err := n.Source.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	slice, ok := source.([]Value)
	if !ok {
		return nil, fmt.Errorf("map source must be array, got %T", source)
	}

	result := make([]Value, 0, len(slice))
	for _, item := range slice {
		// Save current variables
		oldVal := ctx.Variables[n.Variable]
		ctx.Variables[n.Variable] = item

		transformed, err := n.Transform.Evaluate(ctx)
		if err != nil {
			return nil, err
		}

		result = append(result, transformed)

		// Restore variable
		if oldVal != nil {
			ctx.Variables[n.Variable] = oldVal
		} else {
			delete(ctx.Variables, n.Variable)
		}
	}

	return result, nil
}

func (n *All) Evaluate(ctx *Context) (Value, error) {
	source, err := n.Source.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	slice, ok := source.([]Value)
	if !ok {
		return nil, fmt.Errorf("all source must be array, got %T", source)
	}

	for _, item := range slice {
		// Save current variables
		oldVal := ctx.Variables[n.Variable]
		ctx.Variables[n.Variable] = item

		keep, err := n.Predicate.Evaluate(ctx)
		if err != nil {
			return nil, err
		}

		if !keep.(bool) {
			// Restore variable
			if oldVal != nil {
				ctx.Variables[n.Variable] = oldVal
			} else {
				delete(ctx.Variables, n.Variable)
			}
			return false, nil
		}

		// Restore variable
		if oldVal != nil {
			ctx.Variables[n.Variable] = oldVal
		} else {
			delete(ctx.Variables, n.Variable)
		}
	}

	return true, nil
}

func (n *Exists) Evaluate(ctx *Context) (Value, error) {
	source, err := n.Source.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	slice, ok := source.([]Value)
	if !ok {
		return nil, fmt.Errorf("exists source must be array, got %T", source)
	}

	for _, item := range slice {
		// Save current variables
		oldVal := ctx.Variables[n.Variable]
		ctx.Variables[n.Variable] = item

		keep, err := n.Predicate.Evaluate(ctx)
		if err != nil {
			return nil, err
		}

		if keep.(bool) {
			// Restore variable
			if oldVal != nil {
				ctx.Variables[n.Variable] = oldVal
			} else {
				delete(ctx.Variables, n.Variable)
			}
			return true, nil
		}

		// Restore variable
		if oldVal != nil {
			ctx.Variables[n.Variable] = oldVal
		} else {
			delete(ctx.Variables, n.Variable)
		}
	}

	return false, nil
}

func (n *Find) Evaluate(ctx *Context) (Value, error) {
	source, err := n.Source.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	slice, ok := source.([]Value)
	if !ok {
		return nil, fmt.Errorf("find source must be array, got %T", source)
	}

	for _, item := range slice {
		// Save current variables
		oldVal := ctx.Variables[n.Variable]
		ctx.Variables[n.Variable] = item

		found, err := n.Predicate.Evaluate(ctx)
		if err != nil {
			return nil, err
		}

		if found.(bool) {
			// Restore variable
			if oldVal != nil {
				ctx.Variables[n.Variable] = oldVal
			} else {
				delete(ctx.Variables, n.Variable)
			}
			return item, nil
		}

		// Restore variable
		if oldVal != nil {
			ctx.Variables[n.Variable] = oldVal
		} else {
			delete(ctx.Variables, n.Variable)
		}
	}

	return nil, nil
}

func (n *Size) Evaluate(ctx *Context) (Value, error) {
	expr, err := n.Expr.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	switch v := expr.(type) {
	case []Value:
		return float64(len(v)), nil
	case string:
		return float64(len(v)), nil
	case map[string]Value:
		return float64(len(v)), nil
	default:
		return nil, fmt.Errorf("size() requires array, string, or map, got %T", expr)
	}
}

func (n *First) Evaluate(ctx *Context) (Value, error) {
	expr, err := n.Expr.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	switch v := expr.(type) {
	case []Value:
		if len(v) == 0 {
			return nil, nil
		}
		return v[0], nil
	case string:
		if len(v) == 0 {
			return nil, nil
		}
		return string(v[0]), nil
	default:
		return nil, fmt.Errorf("first() requires array or string, got %T", expr)
	}
}

func (n *Last) Evaluate(ctx *Context) (Value, error) {
	expr, err := n.Expr.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	switch v := expr.(type) {
	case []Value:
		if len(v) == 0 {
			return nil, nil
		}
		return v[len(v)-1], nil
	case string:
		if len(v) == 0 {
			return nil, nil
		}
		return string(v[len(v)-1]), nil
	default:
		return nil, fmt.Errorf("last() requires array or string, got %T", expr)
	}
}

// Helper functions
func evaluateArgs(args []ASTNode, ctx *Context) ([]Value, error) {
	values := make([]Value, 0, len(args))
	for _, arg := range args {
		val, err := arg.Evaluate(ctx)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func callMethod(ctx *Context, receiver Value, method string, args []Value) (Value, error) {
	// String methods
	if str, ok := receiver.(string); ok {
		return callStringMethod(ctx, str, method, args)
	}

	// Array methods
	if arr, ok := receiver.([]Value); ok {
		return callArrayMethod(ctx, arr, method, args)
	}

	return nil, fmt.Errorf("method %s not available on %T", method, receiver)
}

// StringPool for zero-allocation string operations
type StringPool struct {
	pool sync.Pool
}

func NewStringPool() *StringPool {
	return &StringPool{
		pool: sync.Pool{
			New: func() any {
				return &strings.Builder{}
			},
		},
	}
}

func (p *StringPool) Get() *strings.Builder {
	return p.pool.Get().(*strings.Builder)
}

func (p *StringPool) Put(b *strings.Builder) {
	b.Reset()
	p.pool.Put(b)
}

// NewContext creates a new evaluation context
func NewContext() *Context {
	return &Context{
		Variables: make(map[string]Value),
		Functions: make(map[string]Function),
		timeNow:   func() time.Time { return time.Now() },
		pool:      NewStringPool(),
	}
}

// RegisterFunction registers a custom function
func (c *Context) RegisterFunction(name string, fn Function) {
	c.Functions[name] = fn
}

// RegisterMethod registers a custom method for a type
func (c *Context) RegisterMethod(receiverType, methodName string, handler MethodHandler) {
	// Implementation for method registration
	// This would be stored in a method registry
}

// Built-in functions registry
var builtinFunctions = map[string]func(context.Context, ...Value) (Value, error){
	// String functions
	"upper":        stringUpper,
	"lower":        stringLower,
	"trim":         stringTrim,
	"replace":      stringReplace,
	"split":        stringSplit,
	"matches":      stringMatches,
	"findAll":      stringFindAll,
	"replaceRegex": stringReplaceRegex,

	// Math functions
	"abs":   mathAbs,
	"ceil":  mathCeil,
	"floor": mathFloor,
	"round": mathRound,
	"sqrt":  mathSqrt,
	"pow":   mathPow,
	"min":   mathMin,
	"max":   mathMax,

	// Collection functions
	"sum":      collectionSum,
	"avg":      collectionAvg,
	"distinct": collectionDistinct,
	"flatten":  collectionFlatten,
	"size":     collectionSize,
	"first":    collectionFirst,
	"last":     collectionLast,
	"filter":   collectionFilter,
	"map":      collectionMap,
	"all":      collectionAll,
	"exists":   collectionExists,
	"find":     collectionFind,

	// JSON functions
	"toJson":   jsonToJson,
	"fromJson": jsonFromJson,

	// Time functions
	"now":         timeNow,
	"date":        timeDate,
	"timestamp":   timeTimestamp,
	"formatTime":  timeFormatTime,
	"addDuration": timeAddDuration,
	"subDuration": timeSubDuration,

	// Type functions
	"type":     typeType,
	"int":      typeInt,
	"double":   typeDouble,
	"string":   typeString,
	"toString": typeToString,
	"duration": typeDuration,
	"bytes":    typeBytes,
	"optional": typeOptional,
}

// Continue with builtin function implementations...
// This is getting quite long, let me break it into more manageable pieces.
