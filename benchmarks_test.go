package cel

import (
	"testing"

	"github.com/google/cel-go/cel"
)

// Test data for benchmarks
var benchmarkData = map[string]interface{}{
	"name":     "Alice",
	"age":      30,
	"isActive": true,
	"scores":   []interface{}{85.5, 92.0, 78.3, 96.7, 89.1},
	"numbers":  []interface{}{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
}

// Our CEL Implementation Benchmarks
func BenchmarkOurCEL_SimpleArithmetic(b *testing.B) {
	ctx := NewContext()
	for k, v := range benchmarkData {
		ctx.Variables[k] = v
	}

	expression := "1 + 2 * 3"
	parser := NewParser(expression)
	compiled, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := compiled.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkOurCEL_ComplexArithmetic(b *testing.B) {
	ctx := NewContext()
	for k, v := range benchmarkData {
		ctx.Variables[k] = v
	}

	expression := "(10 + 5) * 2 - 3"
	parser := NewParser(expression)
	compiled, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := compiled.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkOurCEL_LogicComparison(b *testing.B) {
	ctx := NewContext()
	for k, v := range benchmarkData {
		ctx.Variables[k] = v
	}

	expression := "age > 25 && isActive"
	parser := NewParser(expression)
	compiled, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := compiled.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkOurCEL_UnaryLogic(b *testing.B) {
	ctx := NewContext()
	for k, v := range benchmarkData {
		ctx.Variables[k] = v
	}

	expression := "!(age < 18)"
	parser := NewParser(expression)
	compiled, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := compiled.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkOurCEL_StringUpper(b *testing.B) {
	ctx := NewContext()
	for k, v := range benchmarkData {
		ctx.Variables[k] = v
	}

	expression := "upper(name)"
	parser := NewParser(expression)
	compiled, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := compiled.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkOurCEL_CollectionSum(b *testing.B) {
	ctx := NewContext()
	for k, v := range benchmarkData {
		ctx.Variables[k] = v
	}

	expression := "sum(numbers)"
	parser := NewParser(expression)
	compiled, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := compiled.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkOurCEL_FilterOperation(b *testing.B) {
	ctx := NewContext()
	for k, v := range benchmarkData {
		ctx.Variables[k] = v
	}

	expression := "filter(n, numbers, n > 5)"
	parser := NewParser(expression)
	compiled, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := compiled.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkOurCEL_MapOperation(b *testing.B) {
	ctx := NewContext()
	for k, v := range benchmarkData {
		ctx.Variables[k] = v
	}

	expression := "map(n, numbers, n * 2)"
	parser := NewParser(expression)
	compiled, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := compiled.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkOurCEL_ComplexNested(b *testing.B) {
	ctx := NewContext()
	for k, v := range benchmarkData {
		ctx.Variables[k] = v
	}

	expression := "sum(filter(n, numbers, n > 5))"
	parser := NewParser(expression)
	compiled, err := parser.Parse()
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := compiled.Evaluate(ctx)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

// Google CEL Implementation Benchmarks
func BenchmarkGoogleCEL_SimpleArithmetic(b *testing.B) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("isActive", cel.BoolType),
		cel.Variable("scores", cel.ListType(cel.DoubleType)),
		cel.Variable("numbers", cel.ListType(cel.DoubleType)),
	)
	if err != nil {
		b.Fatalf("Environment creation failed: %v", err)
	}

	expression := "1 + 2 * 3"
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("Compile failed: %v", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program creation failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, _, err := program.Eval(benchmarkData)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkGoogleCEL_ComplexArithmetic(b *testing.B) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("isActive", cel.BoolType),
		cel.Variable("scores", cel.ListType(cel.DoubleType)),
		cel.Variable("numbers", cel.ListType(cel.DoubleType)),
	)
	if err != nil {
		b.Fatalf("Environment creation failed: %v", err)
	}

	expression := "(10 + 5) * 2 - 3"
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("Compile failed: %v", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program creation failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, _, err := program.Eval(benchmarkData)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkGoogleCEL_LogicComparison(b *testing.B) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("isActive", cel.BoolType),
		cel.Variable("scores", cel.ListType(cel.DoubleType)),
		cel.Variable("numbers", cel.ListType(cel.DoubleType)),
	)
	if err != nil {
		b.Fatalf("Environment creation failed: %v", err)
	}

	expression := "age > 25 && isActive"
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("Compile failed: %v", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program creation failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, _, err := program.Eval(benchmarkData)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkGoogleCEL_UnaryLogic(b *testing.B) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("isActive", cel.BoolType),
		cel.Variable("scores", cel.ListType(cel.DoubleType)),
		cel.Variable("numbers", cel.ListType(cel.DoubleType)),
	)
	if err != nil {
		b.Fatalf("Environment creation failed: %v", err)
	}

	expression := "!(age < 18)"
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("Compile failed: %v", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program creation failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, _, err := program.Eval(benchmarkData)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkGoogleCEL_StringUpper(b *testing.B) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("isActive", cel.BoolType),
		cel.Variable("scores", cel.ListType(cel.DoubleType)),
		cel.Variable("numbers", cel.ListType(cel.DoubleType)),
	)
	if err != nil {
		b.Fatalf("Environment creation failed: %v", err)
	}

	expression := "name.upperCase()"
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("Compile failed: %v", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program creation failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, _, err := program.Eval(benchmarkData)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkGoogleCEL_CollectionSum(b *testing.B) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("isActive", cel.BoolType),
		cel.Variable("scores", cel.ListType(cel.DoubleType)),
		cel.Variable("numbers", cel.ListType(cel.DoubleType)),
	)
	if err != nil {
		b.Fatalf("Environment creation failed: %v", err)
	}

	expression := "numbers.reduce(0.0, acc, x, acc + x)"
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("Compile failed: %v", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program creation failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, _, err := program.Eval(benchmarkData)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkGoogleCEL_FilterOperation(b *testing.B) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("isActive", cel.BoolType),
		cel.Variable("scores", cel.ListType(cel.DoubleType)),
		cel.Variable("numbers", cel.ListType(cel.DoubleType)),
	)
	if err != nil {
		b.Fatalf("Environment creation failed: %v", err)
	}

	expression := "numbers.filter(n, n > 5.0)"
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("Compile failed: %v", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program creation failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, _, err := program.Eval(benchmarkData)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkGoogleCEL_MapOperation(b *testing.B) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("isActive", cel.BoolType),
		cel.Variable("scores", cel.ListType(cel.DoubleType)),
		cel.Variable("numbers", cel.ListType(cel.DoubleType)),
	)
	if err != nil {
		b.Fatalf("Environment creation failed: %v", err)
	}

	expression := "numbers.map(n, n * 2)"
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("Compile failed: %v", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program creation failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, _, err := program.Eval(benchmarkData)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}

func BenchmarkGoogleCEL_ComplexNested(b *testing.B) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("isActive", cel.BoolType),
		cel.Variable("scores", cel.ListType(cel.DoubleType)),
		cel.Variable("numbers", cel.ListType(cel.DoubleType)),
	)
	if err != nil {
		b.Fatalf("Environment creation failed: %v", err)
	}

	expression := "numbers.filter(n, n > 5.0).reduce(0.0, acc, x, acc + x)"
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("Compile failed: %v", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program creation failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, _, err := program.Eval(benchmarkData)
		if err != nil {
			b.Fatalf("Evaluation failed: %v", err)
		}
		_ = result
	}
}
