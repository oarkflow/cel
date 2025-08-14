package cel

import (
	"testing"
)

func setupCELContext() *Context {
	ctx := NewContext()

	// Set up test data similar to examples
	users := []Value{
		map[string]Value{
			"id":         1,
			"name":       "Alice Johnson",
			"age":        30,
			"email":      "alice@techcorp.com",
			"salary":     85000.0,
			"department": "Engineering",
		},
		map[string]Value{
			"id":         2,
			"name":       "Bob Smith",
			"age":        25,
			"email":      "bob@startup.io",
			"salary":     45000.0,
			"department": "Marketing",
		},
	}

	ctx.Set("users", users)
	ctx.Set("threshold", 50000.0)
	return ctx
}

func TestComplexChainDetection(t *testing.T) {
	ctx := setupCELContext()
	expression := "users.filter(u, u.salary > threshold).map(u, u.name.upper()).join(', ')"

	parser := NewParser(expression)
	expr, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	t.Logf("Expression type: %T", expr)
	if methodCall, ok := expr.(*MethodCall); ok {
		chain := detectMethodChain(methodCall)
		t.Logf("Chain detected: %+v", chain)
		if chain != nil {
			t.Logf("Base object: %T", chain.BaseObject)
			t.Logf("Methods: %v", len(chain.Methods))
			for i, method := range chain.Methods {
				t.Logf("Method %d: %s with %d args", i, method.Method, len(method.Args))
			}
		}
	}

	result, err := expr.Evaluate(ctx)
	if err != nil {
		t.Fatalf("Evaluation error: %v", err)
	}

	t.Logf("Result: %v", result)
}
