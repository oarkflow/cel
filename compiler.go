package cel

import (
	"sync"
	"time"
)

// Compiled expression system to compete with expr's compilation advantage
type CompiledExpression struct {
	ast      Expression
	metadata *ExpressionMetadata

	// Cached results for constant expressions
	cachedResult Value
	isCached     bool
}

type ExpressionMetadata struct {
	IsConstant      bool
	RequiredVars    []string
	UsedMethods     []string
	ComplexityScore int

	// Additional metadata for optimization
	HasSideEffects  bool
	PureFunction    bool
	CanBeVectorized bool
}

// ExpressionCompiler provides compilation and optimization of expressions
type ExpressionCompiler struct {
	cache   sync.Map // Thread-safe cache for compiled expressions
	enabled bool

	// Statistics for performance monitoring
	compilationCount int64
	cacheHitCount    int64
	cacheMissCount   int64

	// Cache configuration
	maxCacheSize int
	cacheTimeout time.Duration
}

var GlobalCompiler = &ExpressionCompiler{
	enabled:      true,
	maxCacheSize: 10000,
	cacheTimeout: 5 * time.Minute,
}

// Compile parses and optimizes an expression for repeated evaluation
func (ec *ExpressionCompiler) Compile(exprStr string) (*CompiledExpression, error) {
	if !ec.enabled {
		// Fallback to regular parsing
		parser := NewParser(exprStr)
		ast, err := parser.Parse()
		if err != nil {
			return nil, err
		}
		return &CompiledExpression{ast: ast}, nil
	}

	// Check cache first
	if cached, ok := ec.cache.Load(exprStr); ok {
		ec.cacheHitCount++
		return cached.(*CompiledExpression), nil
	}

	ec.cacheMissCount++

	// Parse and analyze expression
	parser := NewParser(exprStr)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	// Analyze expression metadata
	metadata := ec.analyzeExpression(ast)

	// Apply optimizations
	ast = ec.optimizeExpression(ast, metadata)

	// Create compiled expression
	compiled := &CompiledExpression{
		ast:      ast,
		metadata: metadata,
	}

	// For constant expressions, pre-evaluate them
	if metadata.IsConstant && !metadata.HasSideEffects {
		result, err := ast.Evaluate(NewContext())
		if err == nil {
			compiled.cachedResult = result
			compiled.isCached = true
		}
	}

	// Cache for future use
	ec.cache.Store(exprStr, compiled)
	ec.compilationCount++

	return compiled, nil
}

// Fast evaluation using compiled expression
func (ce *CompiledExpression) EvaluateCompiled(ctx *Context) (Value, error) {
	// If it's a constant expression with cached result, return it directly
	if ce.isCached {
		return ce.cachedResult, nil
	}

	// If it's a constant expression but not cached, evaluate and cache it
	if ce.metadata != nil && ce.metadata.IsConstant && !ce.metadata.HasSideEffects {
		result, err := ce.ast.Evaluate(ctx)
		if err == nil {
			ce.cachedResult = result
			ce.isCached = true
		}
		return result, err
	}

	return ce.ast.Evaluate(ctx)
}

// analyzeExpression examines the AST to gather optimization metadata
func (ec *ExpressionCompiler) analyzeExpression(expr Expression) *ExpressionMetadata {
	metadata := &ExpressionMetadata{
		RequiredVars:    make([]string, 0),
		UsedMethods:     make([]string, 0),
		HasSideEffects:  false,
		PureFunction:    true,
		CanBeVectorized: false,
	}

	// Special handling for literals
	if lit, ok := expr.(*Literal); ok {
		metadata.IsConstant = true
		// Check if the literal value is a function that might have side effects
		if _, isFunc := lit.Value.(func([]Value) (Value, error)); isFunc {
			// For now, assume functions have side effects
			// In a real implementation, we'd analyze the function body
			metadata.HasSideEffects = true
			metadata.PureFunction = false
		}
		return metadata
	}

	ec.walkExpression(expr, metadata)

	return metadata
}

func (ec *ExpressionCompiler) walkExpression(expr Expression, metadata *ExpressionMetadata) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *Variable:
		metadata.RequiredVars = append(metadata.RequiredVars, e.Name)
		metadata.ComplexityScore += 1
		// Variables make expressions non-constant
		metadata.IsConstant = false

	case *MethodCall:
		metadata.UsedMethods = append(metadata.UsedMethods, e.Method)
		metadata.ComplexityScore += 2
		ec.walkExpression(e.Object, metadata)
		for _, arg := range e.Args {
			ec.walkExpression(arg, metadata)
		}

		// Check if method has side effects
		if ec.hasSideEffects(e.Method) {
			metadata.HasSideEffects = true
			metadata.PureFunction = false
		}

	case *BinaryOp:
		metadata.ComplexityScore += 1
		ec.walkExpression(e.Left, metadata)
		ec.walkExpression(e.Right, metadata)

	case *Macro:
		metadata.ComplexityScore += 5 // Macros are more expensive
		ec.walkExpression(e.Collection, metadata)
		if e.Body != nil {
			ec.walkExpression(e.Body, metadata)
		}

		// Check if macro type can be vectorized
		switch e.Type {
		case "map", "filter":
			metadata.CanBeVectorized = true
		}

	case *Literal:
		// Literals are free
		if metadata.ComplexityScore == 0 && !metadata.HasSideEffects {
			metadata.IsConstant = true
		}

	case *FunctionCall:
		metadata.ComplexityScore += 3
		// Functions might have side effects
		if ec.hasSideEffects(e.Name) {
			metadata.HasSideEffects = true
			metadata.PureFunction = false
		}
		for _, arg := range e.Args {
			ec.walkExpression(arg, metadata)
		}

	case *UnaryOp:
		metadata.ComplexityScore += 1
		ec.walkExpression(e.Expr, metadata)

	case *FieldAccess:
		metadata.ComplexityScore += 1
		ec.walkExpression(e.Object, metadata)

	case *IndexAccess:
		metadata.ComplexityScore += 1
		ec.walkExpression(e.Object, metadata)
		ec.walkExpression(e.Index, metadata)

	case *TernaryOp:
		metadata.ComplexityScore += 2
		ec.walkExpression(e.Condition, metadata)
		ec.walkExpression(e.TrueExpr, metadata)
		ec.walkExpression(e.FalseExpr, metadata)

		// Add more cases as needed
	}
}

// hasSideEffects checks if a function/method has side effects
func (ec *ExpressionCompiler) hasSideEffects(name string) bool {
	// Functions that typically have side effects
	sideEffectFunctions := map[string]bool{
		"print": true, "println": true, "log": true, "write": true, "save": true,
		"now": true, "random": true, "uuid": true, // Functions that return different values
	}

	return sideEffectFunctions[name]
}

// optimizeExpression applies various optimizations to the AST
func (ec *ExpressionCompiler) optimizeExpression(expr Expression, metadata *ExpressionMetadata) Expression {
	// Constant folding - evaluate constant expressions at compile time
	if metadata.IsConstant && !metadata.HasSideEffects {
		// Try to evaluate the expression
		result, err := expr.Evaluate(NewContext())
		if err == nil {
			// Replace with a literal
			return &Literal{Value: result}
		}
	}

	// Dead code elimination - remove unused branches
	expr = ec.eliminateDeadCode(expr)

	// Common subexpression elimination
	expr = ec.eliminateCommonSubexpressions(expr)

	// Strength reduction - replace expensive operations with cheaper ones
	expr = ec.reduceStrength(expr)

	return expr
}

// eliminateDeadCode removes unused branches from the AST
func (ec *ExpressionCompiler) eliminateDeadCode(expr Expression) Expression {
	// For now, just return the expression unchanged
	// In a real implementation, we'd analyze conditional expressions
	// and remove branches that can never be executed
	return expr
}

// eliminateCommonSubexpressions removes duplicate computations
func (ec *ExpressionCompiler) eliminateCommonSubexpressions(expr Expression) Expression {
	// For now, just return the expression unchanged
	// In a real implementation, we'd identify and cache common subexpressions
	return expr
}

// reduceStrength replaces expensive operations with cheaper ones
func (ec *ExpressionCompiler) reduceStrength(expr Expression) Expression {
	// For now, just return the expression unchanged
	// In a real implementation, we might:
	// - Replace multiplication by powers of 2 with bit shifts
	// - Replace division by constants with multiplication
	// - Optimize string operations
	// - etc.
	return expr
}

// ClearCache clears the compilation cache
func (ec *ExpressionCompiler) ClearCache() {
	ec.cache = sync.Map{}
	ec.compilationCount = 0
	ec.cacheHitCount = 0
	ec.cacheMissCount = 0
}

// GetCacheStats returns cache statistics
func (ec *ExpressionCompiler) GetCacheStats() map[string]int64 {
	return map[string]int64{
		"compilationCount": ec.compilationCount,
		"cacheHitCount":    ec.cacheHitCount,
		"cacheMissCount":   ec.cacheMissCount,
		"hitRate":          int64(float64(ec.cacheHitCount) / float64(ec.cacheHitCount+ec.cacheMissCount) * 100),
	}
}

// Enable enables or disables compilation
func (ec *ExpressionCompiler) Enable(enabled bool) {
	ec.enabled = enabled
}

// SetMaxCacheSize sets the maximum cache size
func (ec *ExpressionCompiler) SetMaxCacheSize(size int) {
	ec.maxCacheSize = size
}

// SetCacheTimeout sets the cache timeout
func (ec *ExpressionCompiler) SetCacheTimeout(timeout time.Duration) {
	ec.cacheTimeout = timeout
}
