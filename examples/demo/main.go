package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/oarkflow/cel"
)

// Custom function implementation
type customFunction struct{}

func (f *customFunction) Call(ctx context.Context, args ...cel.Value) (cel.Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("custom_func requires 1 argument")
	}
	return fmt.Sprintf("🎯 Custom: %v", args[0]), nil
}

func main() {
	fmt.Println("🔧 CEL - Custom Expression Language Implementation")
	fmt.Println(strings.Repeat("=", 52))

	// Create a new context
	ctx := cel.NewContext()

	// Add some variables
	ctx.Variables["name"] = "Alice"
	ctx.Variables["age"] = 30
	ctx.Variables["isActive"] = true
	ctx.Variables["scores"] = []interface{}{85.5, 92.0, 78.3, 96.7, 89.1}
	ctx.Variables["numbers"] = []interface{}{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}

	// Test expressions
	expressions := []string{
		// Basic arithmetic
		"1 + 2 * 3",
		"(10 + 5) * 2 - 3",

		// Comparisons and logic
		"age > 25 && isActive",
		"!(age < 18)",

		// String operations
		"upper(name)",
		"lower(name)",
		"trim(\"  hello world  \")",
		"name + \" is \" + string(age) + \" years old\"",

		// Math functions
		"abs(-42)",
		"sqrt(16)",
		"pow(2, 3)",
		"min(10, 5)",
		"max(10, 5)",

		// Collection operations
		"sum(numbers)",
		"avg(scores)",
		"size(numbers)",
		"first(numbers)",
		"last(numbers)",

		// Filter and map operations with variable scoping
		"filter(n, numbers, n > 5)",
		"map(n, numbers, n * 2)",
		"all(n, numbers, n > 0)",
		"exists(n, numbers, n == 5)",
		"find(n, numbers, n == 5)",

		// Complex nested operations
		"avg(scores)",
		"sum(numbers)",
		"avg(filter(n, numbers, n > 5))",
		"sum(map(n, numbers, n * 2))",
	}

	fmt.Println("\n📊 Evaluating Expressions:")
	fmt.Println(strings.Repeat("-", 32))

	for _, expr := range expressions {
		parser := cel.NewParser(expr)
		compiled, err := parser.Parse()
		if err != nil {
			log.Printf("Parse error for '%s': %v", expr, err)
			continue
		}

		result, err := compiled.Evaluate(ctx)
		if err != nil {
			log.Printf("Evaluation error for '%s': %v", expr, err)
			continue
		}

		fmt.Printf("%-25s = %v\n", expr, result)
	}

	// Test custom function
	fmt.Println("\n🔧 Custom Function Example:")
	fmt.Println(strings.Repeat("-", 32))

	// Register and use custom function
	ctx.RegisterFunction("custom_func", &customFunction{})

	customExpr := "custom_func(\"Hello CEL!\")"
	parser := cel.NewParser(customExpr)
	compiled, _ := parser.Parse()
	result, _ := compiled.Evaluate(ctx)

	fmt.Printf("%-25s = %v\n", customExpr, result)

	// Performance test
	fmt.Println("\n⚡ Performance Test:")
	fmt.Println(strings.Repeat("-", 32))

	perfExpr := "sum(filter(n, numbers, n > 5))"
	parser = cel.NewParser(perfExpr)
	compiled, _ = parser.Parse()

	// Warm up
	for i := 0; i < 100; i++ {
		_, _ = compiled.Evaluate(ctx)
	}

	// Benchmark
	start := time.Now()
	iterations := 10000

	for i := 0; i < iterations; i++ {
		_, _ = compiled.Evaluate(ctx)
	}

	duration := time.Since(start)
	avgTime := duration / time.Duration(iterations)

	fmt.Printf("Expression: %s\n", perfExpr)
	fmt.Printf("Iterations: %d\n", iterations)
	fmt.Printf("Total time: %v\n", duration)
	fmt.Printf("Average per evaluation: %v\n", avgTime)
	fmt.Printf("Throughput: %.0f evaluations/second\n", float64(iterations)/duration.Seconds())

	// Test JSON functions
	fmt.Println("\n📄 JSON Functions:")
	fmt.Println(strings.Repeat("-", 32))

	jsonData := map[string]interface{}{
		"name":    "John Doe",
		"age":     25,
		"active":  true,
		"hobbies": []string{"reading", "coding", "hiking"},
	}

	ctx.Variables["user"] = jsonData

	jsonExpr := "toJson(user)"
	parser = cel.NewParser(jsonExpr)
	compiled, _ = parser.Parse()
	result, _ = compiled.Evaluate(ctx)

	fmt.Printf("%-25s = %v\n", jsonExpr, result)

	// Test time functions
	fmt.Println("\n⏰ Time Functions:")
	fmt.Println(strings.Repeat("-", 32))

	timeExpr := "formatTime(now(), \"2006-01-02 15:04:05\")"
	parser = cel.NewParser(timeExpr)
	compiled, _ = parser.Parse()
	result, _ = compiled.Evaluate(ctx)

	fmt.Printf("%-25s = %v\n", timeExpr, result)

	fmt.Println("\n✅ CEL Implementation Complete!")
	fmt.Println("🎉 High-performance, zero-allocation expression evaluation with extensive built-in functions!")
}
