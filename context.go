package cel

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-reflect"

	"github.com/oarkflow/json"
)

// Context holds variables and functions available during evaluation
type Context struct {
	Variables map[string]Value
	Functions map[string]func([]Value) (Value, error)
}

// Pre-registered built-in functions to avoid re-registration
var builtinFunctions map[string]func([]Value) (Value, error)
var builtinFunctionsOnce sync.Once

// Initialize built-in functions once
func initBuiltinFunctions() {
	builtinFunctions = map[string]func([]Value) (Value, error){
		// Basic functions
		"length": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("length() requires 1 argument")
			}

			switch v := args[0].(type) {
			case []Value:
				return len(v), nil
			case map[string]Value:
				return len(v), nil
			case string:
				return len(v), nil
			default:
				rv := reflect.ValueOf(v)
				if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.Map {
					return rv.Len(), nil
				}
			}
			return 0, nil
		},

		// String functions
		"upper": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("upper() requires 1 argument")
			}
			str := toString(args[0])
			return ultraFastUpper(str), nil
		},

		"lower": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("lower() requires 1 argument")
			}
			str := toString(args[0])
			return ultraFastLower(str), nil
		},

		"trim": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("trim() requires 1 argument")
			}
			str := toString(args[0])
			return ultraFastTrim(str), nil
		},

		"split": func(args []Value) (Value, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("split() requires 2 arguments")
			}
			parts := strings.Split(toString(args[0]), toString(args[1]))
			result := make([]Value, len(parts))
			for i, part := range parts {
				result[i] = part
			}
			return result, nil
		},

		"replace": func(args []Value) (Value, error) {
			if len(args) != 3 {
				return nil, fmt.Errorf("replace() requires 3 arguments")
			}
			str := toString(args[0])
			old := toString(args[1])
			new := toString(args[2])
			return ultraFastReplace(str, old, new), nil
		},

		// Math functions
		"abs": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("abs() requires 1 argument")
			}
			return math.Abs(toFloat64(args[0])), nil
		},

		"ceil": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("ceil() requires 1 argument")
			}
			return math.Ceil(toFloat64(args[0])), nil
		},

		"floor": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("floor() requires 1 argument")
			}
			return math.Floor(toFloat64(args[0])), nil
		},

		"round": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("round() requires 1 argument")
			}
			return math.Round(toFloat64(args[0])), nil
		},

		"min": func(args []Value) (Value, error) {
			if len(args) < 2 {
				return nil, fmt.Errorf("min() requires at least 2 arguments")
			}
			min := toFloat64(args[0])
			for i := 1; i < len(args); i++ {
				val := toFloat64(args[i])
				if val < min {
					min = val
				}
			}
			return min, nil
		},

		"max": func(args []Value) (Value, error) {
			if len(args) < 2 {
				return nil, fmt.Errorf("max() requires at least 2 arguments")
			}
			max := toFloat64(args[0])
			for i := 1; i < len(args); i++ {
				val := toFloat64(args[i])
				if val > max {
					max = val
				}
			}
			return max, nil
		},

		"sqrt": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("sqrt() requires 1 argument")
			}
			return math.Sqrt(toFloat64(args[0])), nil
		},

		"pow": func(args []Value) (Value, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("pow() requires 2 arguments")
			}
			return math.Pow(toFloat64(args[0]), toFloat64(args[1])), nil
		},

		// Aggregation functions
		"sum": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("sum() requires 1 argument")
			}

			items := toValueSlice(args[0])
			if items == nil {
				return nil, fmt.Errorf("sum() requires a collection")
			}

			sum := 0.0
			for _, item := range items {
				sum += toFloat64(item)
			}
			return sum, nil
		},

		"avg": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("avg() requires 1 argument")
			}

			items := toValueSlice(args[0])
			if items == nil {
				return nil, fmt.Errorf("avg() requires a collection")
			}

			if len(items) == 0 {
				return 0.0, nil
			}

			sum := 0.0
			for _, item := range items {
				sum += toFloat64(item)
			}
			return sum / float64(len(items)), nil
		},

		// Type functions
		"type": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("type() requires 1 argument")
			}
			return getType(args[0]), nil
		},

		"int": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("int() requires 1 argument")
			}
			return int64(toFloat64(args[0])), nil
		},

		"double": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("double() requires 1 argument")
			}
			return toFloat64(args[0]), nil
		},

		"string": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("string() requires 1 argument")
			}
			return toString(args[0]), nil
		},

		"toString": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("toString() requires 1 argument")
			}
			return toString(args[0]), nil
		},

		// Advanced type functions
		"duration": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("duration() requires 1 argument")
			}
			str := toString(args[0])
			d, err := time.ParseDuration(str)
			if err != nil {
				return nil, err
			}
			return Duration{D: d}, nil
		},

		"timestamp": func(args []Value) (Value, error) {
			switch len(args) {
			case 0:
				return Timestamp{T: time.Now()}, nil
			case 1:
				str := toString(args[0])
				// Try multiple formats
				formats := []string{
					time.RFC3339,
					time.RFC3339Nano,
					"2006-01-02T15:04:05Z",
					"2006-01-02 15:04:05",
					"2006-01-02",
				}
				for _, format := range formats {
					if t, err := time.Parse(format, str); err == nil {
						return Timestamp{T: t}, nil
					}
				}
				return nil, fmt.Errorf("cannot parse timestamp: %s", str)
			default:
				return nil, fmt.Errorf("timestamp() requires 0 or 1 argument")
			}
		},

		"bytes": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("bytes() requires 1 argument")
			}
			str := toString(args[0])
			return Bytes{data: []byte(str)}, nil
		},

		"optional": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("optional() requires 1 argument")
			}
			return Optional{Value: args[0], Valid: args[0] != nil}, nil
		},

		// Time/Date functions
		"now": func(args []Value) (Value, error) {
			if len(args) != 0 {
				return nil, fmt.Errorf("now() requires 0 arguments")
			}
			return Timestamp{T: time.Now()}, nil
		},

		"date": func(args []Value) (Value, error) {
			if len(args) != 3 {
				return nil, fmt.Errorf("date() requires 3 arguments (year, month, day)")
			}
			year := int(toFloat64(args[0]))
			month := int(toFloat64(args[1]))
			day := int(toFloat64(args[2]))
			return Timestamp{T: time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)}, nil
		},

		"getYear": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("getYear() requires 1 argument")
			}
			if ts, ok := args[0].(Timestamp); ok {
				return ts.T.Year(), nil
			}
			return nil, fmt.Errorf("getYear() requires a timestamp")
		},

		"getMonth": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("getMonth() requires 1 argument")
			}
			if ts, ok := args[0].(Timestamp); ok {
				return int(ts.T.Month()), nil
			}
			return nil, fmt.Errorf("getMonth() requires a timestamp")
		},

		"getDay": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("getDay() requires 1 argument")
			}
			if ts, ok := args[0].(Timestamp); ok {
				return ts.T.Day(), nil
			}
			return nil, fmt.Errorf("getDay() requires a timestamp")
		},

		"getHour": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("getHour() requires 1 argument")
			}
			if ts, ok := args[0].(Timestamp); ok {
				return ts.T.Hour(), nil
			}
			return nil, fmt.Errorf("getHour() requires a timestamp")
		},

		"addDuration": func(args []Value) (Value, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("addDuration() requires 2 arguments")
			}
			ts, ok1 := args[0].(Timestamp)
			dur, ok2 := args[1].(Duration)
			if !ok1 || !ok2 {
				return nil, fmt.Errorf("addDuration() requires timestamp and duration")
			}
			return Timestamp{T: ts.T.Add(dur.D)}, nil
		},

		"subDuration": func(args []Value) (Value, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("subDuration() requires 2 arguments")
			}
			ts, ok1 := args[0].(Timestamp)
			dur, ok2 := args[1].(Duration)
			if !ok1 || !ok2 {
				return nil, fmt.Errorf("subDuration() requires timestamp and duration")
			}
			return Timestamp{T: ts.T.Add(-dur.D)}, nil
		},

		"formatTime": func(args []Value) (Value, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("formatTime() requires 2 arguments")
			}
			ts, ok := args[0].(Timestamp)
			if !ok {
				return nil, fmt.Errorf("formatTime() first argument must be timestamp")
			}
			format := toString(args[1])
			return ts.T.Format(format), nil
		},

		// Regex functions
		"matches": func(args []Value) (Value, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("matches() requires 2 arguments")
			}
			str := toString(args[0])
			pattern := toString(args[1])
			matched, err := regexp.MatchString(pattern, str)
			if err != nil {
				return nil, err
			}
			return matched, nil
		},

		"findAll": func(args []Value) (Value, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("findAll() requires 2 arguments")
			}
			str := toString(args[0])
			pattern := toString(args[1])
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, err
			}
			matches := re.FindAllString(str, -1)
			result := make([]Value, len(matches))
			for i, match := range matches {
				result[i] = match
			}
			return result, nil
		},

		"replaceRegex": func(args []Value) (Value, error) {
			if len(args) != 3 {
				return nil, fmt.Errorf("replaceRegex() requires 3 arguments")
			}
			str := toString(args[0])
			pattern := toString(args[1])
			replacement := toString(args[2])
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, err
			}
			return re.ReplaceAllString(str, replacement), nil
		},

		// JSON functions
		"toJson": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("toJson() requires 1 argument")
			}
			jsonBytes, err := json.Marshal(args[0])
			if err != nil {
				return nil, err
			}
			return string(jsonBytes), nil
		},

		"fromJson": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("fromJson() requires 1 argument")
			}
			jsonStr := toString(args[0])
			var result any
			err := json.Unmarshal([]byte(jsonStr), &result)
			if err != nil {
				return nil, err
			}
			return convertJsonValue(result), nil
		},

		// Advanced collection operations
		"groupBy": func(args []Value) (Value, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("groupBy() requires 2 arguments")
			}

			collection := toValueSlice(args[0])
			if collection == nil {
				return nil, fmt.Errorf("groupBy() first argument must be a collection")
			}

			keyExpr := args[1]
			if fn, ok := keyExpr.(func(Value) Value); ok {
				groups := make(map[string][]Value)
				for _, item := range collection {
					key := toString(fn(item))
					groups[key] = append(groups[key], item)
				}

				result := make(map[string]Value)
				for k, v := range groups {
					result[k] = v
				}
				return result, nil
			}
			return nil, fmt.Errorf("groupBy() second argument must be a function")
		},

		"distinct": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("distinct() requires 1 argument")
			}

			collection := toValueSlice(args[0])
			if collection == nil {
				return nil, fmt.Errorf("distinct() requires a collection")
			}

			seen := make(map[string]bool)
			var result []Value
			for _, item := range collection {
				key := toString(item)
				if !seen[key] {
					seen[key] = true
					result = append(result, item)
				}
			}
			return result, nil
		},

		"flatten": func(args []Value) (Value, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("flatten() requires 1 argument")
			}

			collection := toValueSlice(args[0])
			if collection == nil {
				return nil, fmt.Errorf("flatten() requires a collection")
			}

			var result []Value
			var flattenRecursive func(items []Value)
			flattenRecursive = func(items []Value) {
				for _, item := range items {
					if subCollection := toValueSlice(item); subCollection != nil {
						flattenRecursive(subCollection)
					} else {
						result = append(result, item)
					}
				}
			}

			flattenRecursive(collection)
			return result, nil
		},
	}
}

// NewContext creates a new context with built-in functions
func NewContext() *Context {
	// Initialize built-in functions once
	builtinFunctionsOnce.Do(initBuiltinFunctions)

	// Try to get a pooled context first
	ctx := GetPooledContext()
	if ctx != nil {
		// Reference the global built-in functions map
		ctx.Functions = builtinFunctions
		return ctx
	}

	// Fallback to creating new context
	ctx = &Context{
		Variables: make(map[string]Value),
		Functions: builtinFunctions, // Reference the global map
	}

	return ctx
}

func (c *Context) Set(name string, value Value) {
	c.Variables[name] = value
}

func (c *Context) SetVariables(vrs map[string]Value) {
	for name, value := range vrs {
		c.Variables[name] = value
	}
}

func (c *Context) Get(name string) (Value, bool) {
	val, exists := c.Variables[name]
	return val, exists
}

// Register all built-in functions (deprecated)
func (c *Context) registerBuiltins() {
	// This is now handled by the global builtinFunctions map
}

// Optimized context operations
func (c *Context) MergeVariables(other map[string]Value) {
	// Bulk merge operation for better performance
	for k, v := range other {
		c.Variables[k] = v
	}
}
