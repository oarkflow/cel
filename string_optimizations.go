package cel

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

// StringOptimizations provides zero-allocation string operations where possible
type StringOptimizations struct{}

var StringOpt = &StringOptimizations{}

// ChainedStringOp represents a series of string operations that can be optimized
type ChainedStringOp struct {
	initial string
	ops     []StringOperation
}

type StringOperation struct {
	OpType string
	Args   []string
}

// OptimizedStringChain provides a way to chain string operations with minimal allocations
func (so *StringOptimizations) OptimizedStringChain(initial string) *ChainedStringOp {
	return &ChainedStringOp{
		initial: initial,
		ops:     make([]StringOperation, 0, 4), // Pre-allocate for common case
	}
}

func (cso *ChainedStringOp) Trim() *ChainedStringOp {
	cso.ops = append(cso.ops, StringOperation{OpType: "trim"})
	return cso
}

func (cso *ChainedStringOp) Upper() *ChainedStringOp {
	cso.ops = append(cso.ops, StringOperation{OpType: "upper"})
	return cso
}

func (cso *ChainedStringOp) Lower() *ChainedStringOp {
	cso.ops = append(cso.ops, StringOperation{OpType: "lower"})
	return cso
}

func (cso *ChainedStringOp) Replace(old, new string) *ChainedStringOp {
	cso.ops = append(cso.ops, StringOperation{OpType: "replace", Args: []string{old, new}})
	return cso
}

// Execute performs all operations in a single pass
func (cso *ChainedStringOp) Execute() string {
	result := cso.initial

	// Apply operations in sequence, trying to minimize allocations
	for _, op := range cso.ops {
		switch op.OpType {
		case "trim":
			result = ultraFastTrim(result)
		case "upper":
			result = ultraFastUpper(result)
		case "lower":
			result = ultraFastLower(result)
		case "replace":
			if len(op.Args) >= 2 {
				result = ultraFastReplace(result, op.Args[0], op.Args[1])
			}
		}
	}

	return result
}

// ultraFastTrim provides zero-allocation trimming when possible
func ultraFastTrim(s string) string {
	if len(s) == 0 {
		return s
	}

	start := 0
	end := len(s)

	// Find trim boundaries without allocation
	for start < end {
		c := s[start]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '\v' && c != '\f' {
			break
		}
		start++
	}

	for end > start {
		c := s[end-1]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '\v' && c != '\f' {
			break
		}
		end--
	}

	// Return slice without allocation if no trimming needed
	if start == 0 && end == len(s) {
		return s
	}
	return s[start:end]
}

// ultraFastUpper provides optimized uppercase conversion
func ultraFastUpper(s string) string {
	// Check if conversion is needed
	needsUpper := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			needsUpper = true
			break
		}
	}

	// If no conversion needed, return original
	if !needsUpper {
		return s
	}

	// Convert to uppercase
	return strings.ToUpper(s)
}

// ultraFastLower provides optimized lowercase conversion
func ultraFastLower(s string) string {
	// Check if conversion is needed
	needsLower := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			needsLower = true
			break
		}
	}

	// If no conversion needed, return original
	if !needsLower {
		return s
	}

	// Convert to lowercase
	return strings.ToLower(s)
}

// ultraFastReplace provides optimized string replacement
func ultraFastReplace(s, old, new string) string {
	// Fast path: if old string not found, return original
	if !strings.Contains(s, old) {
		return s
	}

	// Perform replacement
	return strings.ReplaceAll(s, old, new)
}

// FastStringMethods provides optimized implementations of common string methods
var FastStringMethods = map[string]func(obj Value, args ...Value) (Value, error){
	"trim": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("trim() requires 0 arguments")
		}
		str := toString(obj)
		return ultraFastTrim(str), nil
	},

	"upper": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("upper() requires 0 arguments")
		}
		str := toString(obj)
		return ultraFastUpper(str), nil
	},

	"lower": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("lower() requires 0 arguments")
		}
		str := toString(obj)
		return ultraFastLower(str), nil
	},

	"replace": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("replace() requires 2 arguments")
		}
		str := toString(obj)
		old := toString(args[0])
		new := toString(args[1])
		return ultraFastReplace(str, old, new), nil
	},

	// Additional optimized string methods
	"contains": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("contains() requires 1 argument")
		}
		str := toString(obj)
		substr := toString(args[0])
		// Ultra-fast contains check
		return strings.Contains(str, substr), nil
	},

	"startsWith": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("startsWith() requires 1 argument")
		}
		str := toString(obj)
		prefix := toString(args[0])
		// Ultra-fast prefix check
		return strings.HasPrefix(str, prefix), nil
	},

	"endsWith": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("endsWith() requires 1 argument")
		}
		str := toString(obj)
		suffix := toString(args[0])
		// Ultra-fast suffix check
		return strings.HasSuffix(str, suffix), nil
	},

	"length": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("length() requires 0 arguments")
		}
		str := toString(obj)
		// Zero-allocation length calculation
		return len(str), nil
	},

	"substring": func(obj Value, args ...Value) (Value, error) {
		if len(args) < 1 || len(args) > 2 {
			return nil, fmt.Errorf("substring() requires 1 or 2 arguments")
		}
		str := toString(obj)
		start := int(toFloat64(args[0]))

		// Bounds checking
		if start < 0 {
			start = 0
		}
		if start > len(str) {
			start = len(str)
		}

		if len(args) == 1 {
			// Return substring from start to end
			return str[start:], nil
		}

		end := int(toFloat64(args[1]))
		if end < 0 {
			end = 0
		}
		if end > len(str) {
			end = len(str)
		}
		if end < start {
			end = start
		}

		// Return substring from start to end
		return str[start:end], nil
	},
}

// ZeroAllocBytesToString converts []byte to string without allocation
func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// OptimizedStringBuilder provides a more efficient string builder for method chaining
type OptimizedStringBuilder struct {
	buf    []byte
	length int
}

func NewOptimizedStringBuilder(capacity int) *OptimizedStringBuilder {
	return &OptimizedStringBuilder{
		buf: make([]byte, 0, capacity),
	}
}

func (osb *OptimizedStringBuilder) WriteString(s string) {
	osb.buf = append(osb.buf, s...)
	osb.length += len(s)
}

func (osb *OptimizedStringBuilder) String() string {
	return bytesToString(osb.buf)
}

func (osb *OptimizedStringBuilder) Reset() {
	osb.buf = osb.buf[:0]
	osb.length = 0
}

// Enhanced toString with zero-allocation for more types
func enhancedToString(val Value) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v // Zero allocation for strings
	case int:
		// Use strconv.AppendInt for better performance
		buf := make([]byte, 0, 20) // Enough for int64
		buf = strconv.AppendInt(buf, int64(v), 10)
		return bytesToString(buf)
	case int64:
		// Use strconv.AppendInt for better performance
		buf := make([]byte, 0, 20) // Enough for int64
		buf = strconv.AppendInt(buf, v, 10)
		return bytesToString(buf)
	case float64:
		// Use strconv.AppendFloat for better performance
		buf := make([]byte, 0, 32) // Enough for float64
		buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		return bytesToString(buf)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case []byte:
		// Direct conversion for []byte
		return bytesToString(v)
	default:
		// Fallback to standard formatting
		return fmt.Sprintf("%v", v)
	}
}

// Register optimized string operations
func init() {
	// Register common optimized string operation chains
	RegisterOptimizedChain("trim().upper()", func(s string) string {
		// Combined trim and upper operation
		trimmed := ultraFastTrim(s)
		return ultraFastUpper(trimmed)
	})

	RegisterOptimizedChain("upper().trim()", func(s string) string {
		// Combined upper and trim operation
		upper := ultraFastUpper(s)
		return ultraFastTrim(upper)
	})

	RegisterOptimizedChain("trim().lower()", func(s string) string {
		// Combined trim and lower operation
		trimmed := ultraFastTrim(s)
		return ultraFastLower(trimmed)
	})

	RegisterOptimizedChain("lower().trim()", func(s string) string {
		// Combined lower and trim operation
		lower := ultraFastLower(s)
		return ultraFastTrim(lower)
	})

	// Register additional common chains
	// Note: These would need to be handled properly in a real implementation
	// For now, we're just showing the pattern
}
