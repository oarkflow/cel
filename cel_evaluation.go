package cel

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Evaluate binary operations
func evaluateBinaryOp(op string, left, right Value, _ *Context) (Value, error) {
	switch op {
	case "+":
		return evaluateAdd(left, right)
	case "-":
		return evaluateSubtract(left, right)
	case "*":
		return evaluateMultiply(left, right)
	case "/":
		return evaluateDivide(left, right)
	case "%":
		return evaluateModulo(left, right)
	case "^":
		return evaluatePower(left, right)
	case "==":
		return evaluateEqual(left, right), nil
	case "!=":
		return !evaluateEqual(left, right), nil
	case "<":
		return evaluateLessThan(left, right), nil
	case "<=":
		return evaluateLessThanOrEqual(left, right), nil
	case ">":
		return evaluateGreaterThan(left, right), nil
	case ">=":
		return evaluateGreaterThanOrEqual(left, right), nil
	case "&&":
		return evaluateAnd(left, right), nil
	case "||":
		return evaluateOr(left, right), nil
	default:
		return nil, fmt.Errorf("unknown binary operator: %s", op)
	}
}

// Evaluate unary operations
func evaluateUnaryOp(op string, expr Value, _ *Context) (Value, error) {
	switch op {
	case "!":
		return evaluateNot(expr), nil
	case "-":
		return evaluateNegate(expr)
	default:
		return nil, fmt.Errorf("unknown unary operator: %s", op)
	}
}

// Arithmetic operations
func evaluateAdd(left, right Value) (Value, error) {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			return lv + rv, nil
		case int:
			return lv + float64(rv), nil
		case string:
			return fmt.Sprintf("%v%v", lv, rv), nil
		}
	case int:
		switch rv := right.(type) {
		case float64:
			return float64(lv) + rv, nil
		case int:
			return lv + rv, nil
		case string:
			return fmt.Sprintf("%v%v", lv, rv), nil
		}
	case string:
		return lv + fmt.Sprintf("%v", right), nil
	case time.Time:
		if dur, ok := right.(time.Duration); ok {
			return lv.Add(dur), nil
		}
	}

	return nil, fmt.Errorf("invalid operands for + operator: %T + %T", left, right)
}

func evaluateSubtract(left, right Value) (Value, error) {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			return lv - rv, nil
		case int:
			return lv - float64(rv), nil
		}
	case int:
		switch rv := right.(type) {
		case float64:
			return float64(lv) - rv, nil
		case int:
			return lv - rv, nil
		}
	case time.Time:
		if rv, ok := right.(time.Time); ok {
			return lv.Sub(rv), nil
		}
		if dur, ok := right.(time.Duration); ok {
			return lv.Add(-dur), nil
		}
	case time.Duration:
		if rv, ok := right.(time.Duration); ok {
			return lv - rv, nil
		}
	}

	return nil, fmt.Errorf("invalid operands for - operator: %T - %T", left, right)
}

func evaluateMultiply(left, right Value) (Value, error) {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			return lv * rv, nil
		case int:
			return lv * float64(rv), nil
		}
	case int:
		switch rv := right.(type) {
		case float64:
			return float64(lv) * rv, nil
		case int:
			return lv * rv, nil
		}
	}

	return nil, fmt.Errorf("invalid operands for * operator: %T * %T", left, right)
}

func evaluateDivide(left, right Value) (Value, error) {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			if rv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return lv / rv, nil
		case int:
			if rv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return lv / float64(rv), nil
		}
	case int:
		switch rv := right.(type) {
		case float64:
			if rv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return float64(lv) / rv, nil
		case int:
			if rv == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return lv / rv, nil
		}
	}

	return nil, fmt.Errorf("invalid operands for / operator: %T / %T", left, right)
}

func evaluateModulo(left, right Value) (Value, error) {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			return math.Mod(lv, rv), nil
		case int:
			return math.Mod(lv, float64(rv)), nil
		}
	case int:
		switch rv := right.(type) {
		case float64:
			return math.Mod(float64(lv), rv), nil
		case int:
			return lv % rv, nil
		}
	}

	return nil, fmt.Errorf("invalid operands for %% operator")
}

func evaluatePower(left, right Value) (Value, error) {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			return math.Pow(lv, rv), nil
		case int:
			return math.Pow(lv, float64(rv)), nil
		}
	case int:
		switch rv := right.(type) {
		case float64:
			return math.Pow(float64(lv), rv), nil
		case int:
			return math.Pow(float64(lv), float64(rv)), nil
		}
	}

	return nil, fmt.Errorf("invalid operands for ^ operator: %T ^ %T", left, right)
}

// Comparison operations
func evaluateEqual(left, right Value) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}
	return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right)
}

func evaluateLessThan(left, right Value) bool {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			return lv < rv
		case int:
			return lv < float64(rv)
		}
	case int:
		switch rv := right.(type) {
		case float64:
			return float64(lv) < rv
		case int:
			return lv < rv
		}
	case string:
		if rv, ok := right.(string); ok {
			return lv < rv
		}
	case time.Time:
		if rv, ok := right.(time.Time); ok {
			return lv.Before(rv)
		}
	}
	return false
}

func evaluateLessThanOrEqual(left, right Value) bool {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			return lv <= rv
		case int:
			return lv <= float64(rv)
		}
	case int:
		switch rv := right.(type) {
		case float64:
			return float64(lv) <= rv
		case int:
			return lv <= rv
		}
	case string:
		if rv, ok := right.(string); ok {
			return lv <= rv
		}
	case time.Time:
		if rv, ok := right.(time.Time); ok {
			return lv.Before(rv) || lv.Equal(rv)
		}
	}
	return false
}

func evaluateGreaterThan(left, right Value) bool {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			return lv > rv
		case int:
			return lv > float64(rv)
		}
	case int:
		switch rv := right.(type) {
		case float64:
			return float64(lv) > rv
		case int:
			return lv > rv
		}
	case string:
		if rv, ok := right.(string); ok {
			return lv > rv
		}
	case time.Time:
		if rv, ok := right.(time.Time); ok {
			return lv.After(rv)
		}
	}
	return false
}

func evaluateGreaterThanOrEqual(left, right Value) bool {
	switch lv := left.(type) {
	case float64:
		switch rv := right.(type) {
		case float64:
			return lv >= rv
		case int:
			return lv >= float64(rv)
		}
	case int:
		switch rv := right.(type) {
		case float64:
			return float64(lv) >= rv
		case int:
			return lv >= rv
		}
	case string:
		if rv, ok := right.(string); ok {
			return lv >= rv
		}
	case time.Time:
		if rv, ok := right.(time.Time); ok {
			return lv.After(rv) || lv.Equal(rv)
		}
	}
	return false
}

// Logical operations
func evaluateAnd(left, right Value) bool {
	lBool, ok1 := left.(bool)
	rBool, ok2 := right.(bool)
	if !ok1 || !ok2 {
		return false
	}
	return lBool && rBool
}

func evaluateOr(left, right Value) bool {
	lBool, ok1 := left.(bool)
	rBool, ok2 := right.(bool)
	if !ok1 || !ok2 {
		return false
	}
	return lBool || rBool
}

func evaluateNot(expr Value) bool {
	b, ok := expr.(bool)
	if !ok {
		return false
	}
	return !b
}

func evaluateNegate(expr Value) (Value, error) {
	switch v := expr.(type) {
	case float64:
		return -v, nil
	case int:
		return -v, nil
	}
	return nil, fmt.Errorf("cannot negate %T", expr)
}

// Performance monitoring
type EvaluationStats struct {
	Evaluations int64
	CacheHits   int64
	Allocations int64
	Duration    time.Duration
}

func (s *EvaluationStats) AddEvaluation(duration time.Duration) {
	s.Evaluations++
	s.Duration += duration
}

func (s *EvaluationStats) AddCacheHit() {
	s.CacheHits++
}

func (s *EvaluationStats) AddAllocation() {
	s.Allocations++
}

// Memory pool for frequently used values
type ValuePool struct {
	pool sync.Pool
}

func NewValuePool() *ValuePool {
	return &ValuePool{
		pool: sync.Pool{
			New: func() any {
				var v Value
				return &v
			},
		},
	}
}

func (p *ValuePool) Get() *Value {
	return p.pool.Get().(*Value)
}

func (p *ValuePool) Put(v *Value) {
	*v = nil // Clear the value
	p.pool.Put(v)
}

// Cached expression evaluation
type CachedExpression struct {
	expression *Expression
	cache      map[string]Value
	stats      EvaluationStats
}

func NewCachedExpression(expr *Expression) *CachedExpression {
	return &CachedExpression{
		expression: expr,
		cache:      make(map[string]Value),
	}
}

func (ce *CachedExpression) Evaluate(ctx *Context, cacheKey string) (Value, error) {
	if cached, ok := ce.cache[cacheKey]; ok {
		ce.stats.AddCacheHit()
		return cached, nil
	}

	result, err := ce.expression.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	ce.cache[cacheKey] = result
	ce.stats.AddEvaluation(time.Duration(0)) // Would use actual timing

	return result, nil
}

func (ce *CachedExpression) GetStats() EvaluationStats {
	return ce.stats
}
