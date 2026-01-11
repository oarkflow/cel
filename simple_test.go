package cel

import (
	"testing"
)

func TestSimpleArithmetic(t *testing.T) {
	ctx := NewContext()

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"1 + 2", 3.0},
		{"5 - 3", 2.0},
		{"4 * 2", 8.0},
		{"10 / 2", 5.0},
		{"2 ^ 3", 8.0},
		{"upper(\"hello\")", "HELLO"},
		{"lower(\"WORLD\")", "world"},
		{"abs(-5)", 5.0},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			parser := NewParser(test.expr)
			expr, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			result, err := expr.Evaluate(ctx)
			if err != nil {
				t.Fatalf("Evaluation failed: %v", err)
			}

			if result != test.expected {
				t.Errorf("Expected %v, got %v", test.expected, result)
			}
		})
	}
}

// Benchmarks with memory allocation tracking
func BenchmarkSimpleExpression(b *testing.B) {
	ctx := NewContext()
	ctx.Variables["x"] = 10.0
	ctx.Variables["y"] = 20.0

	parser := NewParser("x + y * 2")
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(ctx)
	}
}

func BenchmarkComplexExpression(b *testing.B) {
	ctx := NewContext()
	ctx.Variables["numbers"] = []Value{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}

	parser := NewParser("sum(filter(n, n > 5))")
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(ctx)
	}
}

func BenchmarkStringOperations(b *testing.B) {
	ctx := NewContext()
	ctx.Variables["text"] = "hello world"

	parser := NewParser("upper(trim(text))")
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(ctx)
	}
}

func BenchmarkNestedFunctions(b *testing.B) {
	ctx := NewContext()
	ctx.Variables["scores"] = []Value{85.5, 92.0, 78.3, 96.7, 89.1}

	parser := NewParser("avg(filter(s, s > 80))")
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(ctx)
	}
}

func BenchmarkArithmeticChain(b *testing.B) {
	ctx := NewContext()
	ctx.Variables["a"] = 10.0
	ctx.Variables["b"] = 5.0
	ctx.Variables["c"] = 2.0

	parser := NewParser("(a + b) * c - (a / b)")
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(ctx)
	}
}

func BenchmarkMapTransform(b *testing.B) {
	ctx := NewContext()
	ctx.Variables["data"] = []Value{1.0, 2.0, 3.0, 4.0, 5.0}

	parser := NewParser("sum(map(n, n * 2))")
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(ctx)
	}
}
