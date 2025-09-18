package cel

import (
	"testing"

	"github.com/oarkflow/expr"
)

// Benchmark data
var benchmarkUsers = []Value{
	map[string]Value{
		"id":         1,
		"name":       "Alice Johnson",
		"age":        30,
		"email":      "alice@techcorp.com",
		"roles":      []Value{"admin", "user", "developer"},
		"active":     true,
		"salary":     85000.0,
		"department": "Engineering",
	},
	map[string]Value{
		"id":         2,
		"name":       "Bob Smith",
		"age":        25,
		"email":      "bob@startup.io",
		"roles":      []Value{"user", "intern"},
		"active":     true,
		"salary":     45000.0,
		"department": "Marketing",
	},
	map[string]Value{
		"id":         3,
		"name":       "Charlie Brown",
		"age":        35,
		"email":      "charlie@corp.com",
		"roles":      []Value{"manager", "user"},
		"active":     false,
		"salary":     95000.0,
		"department": "Engineering",
	},
	map[string]Value{
		"id":         4,
		"name":       "Diana Prince",
		"age":        28,
		"email":      "diana@wonder.com",
		"roles":      []Value{"user", "analyst"},
		"active":     true,
		"salary":     65000.0,
		"department": "Finance",
	},
	map[string]Value{
		"id":         5,
		"name":       "Eve Wilson",
		"age":        32,
		"email":      "eve@startup.io",
		"roles":      []Value{"developer", "user"},
		"active":     true,
		"salary":     78000.0,
		"department": "Engineering",
	},
}

// Benchmark expressions
var benchmarkExpressions = []string{
	// Simple arithmetic
	"2 + 3 * 4",
	"(2 + 3) * 4",
	"abs(-42)",
	"sqrt(16)",
	"pow(2, 3)",

	// String operations
	"upper('hello world')",
	"lower('HELLO WORLD')",
	"'hello world'.upper()",
	"'HELLO WORLD'.lower()",
	"'hello world'.replace('world', 'Go')",

	// Collection operations
	"users.filter(u, u.age > 25)",
	"users.map(u, u.name)",
	"users.map(u, u.salary * 1.1)",
	"users.all(u, u.age >= 18)",
	"users.exists(u, u.salary > 80000)",
	"users.find(u, u.department == 'Engineering')",

	// Complex expressions
	"users.filter(u, u.active && u.salary > 50000).map(u, u.name).join(', ')",
	"users.map(u, u.salary).sum()",
	"users.map(u, u.age).avg()",
	"users.groupBy(u, u.department)",

	// Method chaining
	"users.map(u, u.name).split(' ').map(parts, parts.first()).join(', ')",
	// "users.flatMap(u, split(u.name, ' ')).reverse().join(' -> ')", // Skip due to split function issue

	// List comprehensions
	"[u.name | u in users]",
	"[u.name | u in users, u.age > 25]",
	"[u.salary * 1.1 | u in users, u.department == 'Engineering']",
}

// Benchmark CEL implementation
func BenchmarkCEL(b *testing.B) {
	ctx := NewContext()
	ctx.Set("users", benchmarkUsers)

	for _, exprStr := range benchmarkExpressions {
		b.Run("CEL_"+exprStr, func(b *testing.B) {
			parser := NewParser(exprStr)
			expr, err := parser.Parse()
			if err != nil {
				b.Fatalf("Parse error: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := expr.Evaluate(ctx)
				if err != nil {
					b.Fatalf("Eval error: %v", err)
				}
			}
		})
	}
}

// Benchmark expr-lang/expr implementation
func BenchmarkExpr(b *testing.B) {
	env := map[string]interface{}{
		"users": benchmarkUsers,
	}

	for _, exprStr := range benchmarkExpressions {
		b.Run("Expr_"+exprStr, func(b *testing.B) {
			program, err := expr.Compile(exprStr, expr.Env(env))
			if err != nil {
				b.Fatalf("Compile error: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := expr.Run(program, env)
				if err != nil {
					b.Fatalf("Run error: %v", err)
				}
			}
		})
	}
}

// Benchmark compilation time
func BenchmarkCELCompilation(b *testing.B) {
	for _, exprStr := range benchmarkExpressions {
		b.Run("CEL_Compile_"+exprStr, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				parser := NewParser(exprStr)
				_, err := parser.Parse()
				if err != nil {
					b.Fatalf("Parse error: %v", err)
				}
			}
		})
	}
}

func BenchmarkExprCompilation(b *testing.B) {
	env := map[string]interface{}{
		"users": benchmarkUsers,
	}

	for _, exprStr := range benchmarkExpressions {
		b.Run("Expr_Compile_"+exprStr, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := expr.Compile(exprStr, expr.Env(env))
				if err != nil {
					b.Fatalf("Compile error: %v", err)
				}
			}
		})
	}
}

// Memory benchmark
func BenchmarkCELMemory(b *testing.B) {
	ctx := NewContext()
	ctx.Set("users", benchmarkUsers)

	exprStr := "users.filter(u, u.age > 25).map(u, u.salary * 1.1).sum()"
	parser := NewParser(exprStr)
	expr, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse error: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := expr.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Eval error: %v", err)
		}
	}
}

func BenchmarkExprMemory(b *testing.B) {
	env := map[string]interface{}{
		"users": benchmarkUsers,
	}

	exprStr := "users.filter(u, u.age > 25).map(u, u.salary * 1.1).sum()"
	program, err := expr.Compile(exprStr, expr.Env(env))
	if err != nil {
		b.Fatalf("Compile error: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := expr.Run(program, env)
		if err != nil {
			b.Fatalf("Run error: %v", err)
		}
	}
}

// Large dataset benchmark
func BenchmarkCELLargeDataset(b *testing.B) {
	// Create a larger dataset
	largeUsers := make([]Value, 1000)
	for i := 0; i < 1000; i++ {
		largeUsers[i] = map[string]Value{
			"id":     i,
			"name":   "User " + string(rune(i%26+65)),
			"age":    20 + (i % 50),
			"salary": 30000.0 + float64(i%50000),
			"active": i%2 == 0,
		}
	}

	ctx := NewContext()
	ctx.Set("users", largeUsers)

	exprStr := "users.filter(u, u.active && u.age > 25).map(u, u.salary).sum()"
	parser := NewParser(exprStr)
	expr, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse error: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := expr.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Eval error: %v", err)
		}
	}
}

func BenchmarkExprLargeDataset(b *testing.B) {
	// Create a larger dataset
	largeUsers := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		largeUsers[i] = map[string]interface{}{
			"id":     i,
			"name":   "User " + string(rune(i%26+65)),
			"age":    20 + (i % 50),
			"salary": 30000.0 + float64(i%50000),
			"active": i%2 == 0,
		}
	}

	env := map[string]interface{}{
		"users": largeUsers,
	}

	exprStr := "users.filter(u, u.active && u.age > 25).map(u, u.salary).sum()"
	program, err := expr.Compile(exprStr, expr.Env(env))
	if err != nil {
		b.Fatalf("Compile error: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := expr.Run(program, env)
		if err != nil {
			b.Fatalf("Run error: %v", err)
		}
	}
}

// String operations benchmark
func BenchmarkCELStringOps(b *testing.B) {
	ctx := NewContext()
	ctx.Set("text", "  Hello, World! This is a test string for benchmarking.  ")

	expressions := []string{
		"text.trim()",
		"text.upper()",
		"text.lower()",
		"text.replace('test', 'benchmark')",
		"text.split(' ').join('-')",
		"text.trim().upper().replace(' ', '_')",
	}

	for _, exprStr := range expressions {
		b.Run("CEL_String_"+exprStr, func(b *testing.B) {
			parser := NewParser(exprStr)
			expr, err := parser.Parse()
			if err != nil {
				b.Fatalf("Parse error: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := expr.Evaluate(ctx)
				if err != nil {
					b.Fatalf("Eval error: %v", err)
				}
			}
		})
	}
}

func BenchmarkExprStringOps(b *testing.B) {
	env := map[string]interface{}{
		"text": "  Hello, World! This is a test string for benchmarking.  ",
	}

	expressions := []string{
		"text.trim()",
		"text.upper()",
		"text.lower()",
		"text.replace('test', 'benchmark')",
		"text.split(' ').join('-')",
		"text.trim().upper().replace(' ', '_')",
	}

	for _, exprStr := range expressions {
		b.Run("Expr_String_"+exprStr, func(b *testing.B) {
			program, err := expr.Compile(exprStr, expr.Env(env))
			if err != nil {
				b.Fatalf("Compile error: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := expr.Run(program, env)
				if err != nil {
					b.Fatalf("Run error: %v", err)
				}
			}
		})
	}
}
