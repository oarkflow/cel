package cel

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// String functions
func stringUpper(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("upper() requires 1 argument")
	}
	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("upper() requires string argument")
	}
	return strings.ToUpper(str), nil
}

func stringLower(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("lower() requires 1 argument")
	}
	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("lower() requires string argument")
	}
	return strings.ToLower(str), nil
}

func stringTrim(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("trim() requires 1 argument")
	}
	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("trim() requires string argument")
	}
	return strings.TrimSpace(str), nil
}

func stringReplace(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("replace() requires 3 arguments")
	}

	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("replace() first argument must be string")
	}

	old, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("replace() second argument must be string")
	}

	new, ok := args[2].(string)
	if !ok {
		return nil, fmt.Errorf("replace() third argument must be string")
	}

	return strings.Replace(str, old, new, -1), nil
}

func stringSplit(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("split() requires 2 arguments")
	}

	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("split() first argument must be string")
	}

	sep, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("split() second argument must be string")
	}

	parts := strings.Split(str, sep)
	result := make([]Value, len(parts))
	for i, part := range parts {
		result[i] = part
	}

	return result, nil
}

func stringMatches(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("matches() requires 2 arguments")
	}

	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("matches() first argument must be string")
	}

	pattern, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("matches() second argument must be string")
	}

	matched, err := regexp.MatchString(pattern, str)
	return matched, err
}

func stringFindAll(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("findAll() requires 2 arguments")
	}

	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("findAll() first argument must be string")
	}

	pattern, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("findAll() second argument must be string")
	}

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
}

func stringReplaceRegex(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("replaceRegex() requires 3 arguments")
	}

	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("replaceRegex() first argument must be string")
	}

	pattern, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("replaceRegex() second argument must be string")
	}

	replacement, ok := args[2].(string)
	if !ok {
		return nil, fmt.Errorf("replaceRegex() third argument must be string")
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return re.ReplaceAllString(str, replacement), nil
}

// Math functions
func mathAbs(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("abs() requires 1 argument")
	}

	switch v := args[0].(type) {
	case float64:
		return math.Abs(v), nil
	case int:
		if v < 0 {
			return -v, nil
		}
		return v, nil
	default:
		return nil, fmt.Errorf("abs() requires numeric argument")
	}
}

func mathCeil(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ceil() requires 1 argument")
	}

	f, ok := args[0].(float64)
	if !ok {
		return nil, fmt.Errorf("ceil() requires float64 argument")
	}

	return math.Ceil(f), nil
}

func mathFloor(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("floor() requires 1 argument")
	}

	f, ok := args[0].(float64)
	if !ok {
		return nil, fmt.Errorf("floor() requires float64 argument")
	}

	return math.Floor(f), nil
}

func mathRound(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("round() requires 1 argument")
	}

	f, ok := args[0].(float64)
	if !ok {
		return nil, fmt.Errorf("round() requires float64 argument")
	}

	return math.Round(f), nil
}

func mathSqrt(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("sqrt() requires 1 argument")
	}

	f, ok := args[0].(float64)
	if !ok {
		return nil, fmt.Errorf("sqrt() requires float64 argument")
	}

	return math.Sqrt(f), nil
}

func mathPow(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("pow() requires 2 arguments")
	}

	base, ok := args[0].(float64)
	if !ok {
		return nil, fmt.Errorf("pow() first argument must be float64")
	}

	exp, ok := args[1].(float64)
	if !ok {
		return nil, fmt.Errorf("pow() second argument must be float64")
	}

	return math.Pow(base, exp), nil
}

func mathMin(ctx context.Context, args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("min() requires at least 1 argument")
	}

	result := args[0]
	for i := 1; i < len(args); i++ {
		if isLessThan(args[i], result) {
			result = args[i]
		}
	}

	return result, nil
}

func mathMax(ctx context.Context, args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("max() requires at least 1 argument")
	}

	result := args[0]
	for i := 1; i < len(args); i++ {
		if isGreaterThan(args[i], result) {
			result = args[i]
		}
	}

	return result, nil
}

// Collection functions
func collectionSum(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("sum() requires 1 argument")
	}

	values, ok := args[0].([]Value)
	if !ok {
		return nil, fmt.Errorf("sum() requires array argument")
	}

	var sum float64
	for _, v := range values {
		switch n := v.(type) {
		case float64:
			sum += n
		case int:
			sum += float64(n)
		default:
			return nil, fmt.Errorf("sum() requires numeric values")
		}
	}

	return sum, nil
}

func collectionAvg(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("avg() requires 1 argument")
	}

	values, ok := args[0].([]Value)
	if !ok {
		return nil, fmt.Errorf("avg() requires array argument")
	}

	if len(values) == 0 {
		return 0.0, nil
	}

	sum, err := collectionSum(ctx, values)
	if err != nil {
		return nil, err
	}

	return sum.(float64) / float64(len(values)), nil
}

func collectionDistinct(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("distinct() requires 1 argument")
	}

	values, ok := args[0].([]Value)
	if !ok {
		return nil, fmt.Errorf("distinct() requires array argument")
	}

	seen := make(map[string]bool)
	result := make([]Value, 0, len(values))

	for _, v := range values {
		key := fmt.Sprintf("%v", v)
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}

	return result, nil
}

func collectionFlatten(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("flatten() requires 1 argument")
	}

	values, ok := args[0].([]Value)
	if !ok {
		return nil, fmt.Errorf("flatten() requires array argument")
	}

	result := make([]Value, 0)

	for _, v := range values {
		if arr, ok := v.([]Value); ok {
			result = append(result, arr...)
		} else {
			result = append(result, v)
		}
	}

	return result, nil
}

// JSON functions
func jsonToJson(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("toJson() requires 1 argument")
	}

	data, err := json.Marshal(args[0])
	return string(data), err
}

func jsonFromJson(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("fromJson() requires 1 argument")
	}

	jsonStr, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("fromJson() requires string argument")
	}

	var result any
	err := json.Unmarshal([]byte(jsonStr), &result)
	return result, err
}

// Time functions
func timeNow(ctx context.Context, args ...Value) (Value, error) {
	return time.Now(), nil
}

func timeDate(ctx context.Context, args ...Value) (Value, error) {
	// Create a date from components
	if len(args) != 3 {
		return nil, fmt.Errorf("date() requires 3 arguments")
	}

	year, ok := args[0].(float64)
	if !ok {
		return nil, fmt.Errorf("date() first argument must be number")
	}

	month, ok := args[1].(float64)
	if !ok {
		return nil, fmt.Errorf("date() second argument must be number")
	}

	day, ok := args[2].(float64)
	if !ok {
		return nil, fmt.Errorf("date() third argument must be number")
	}

	return time.Date(int(year), time.Month(int(month)), int(day), 0, 0, 0, 0, time.UTC), nil
}

func timeTimestamp(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("timestamp() requires 1 argument")
	}

	switch v := args[0].(type) {
	case time.Time:
		return v.Unix(), nil
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, err
		}
		return t.Unix(), nil
	default:
		return nil, fmt.Errorf("timestamp() requires time.Time or string argument")
	}
}

func timeFormatTime(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("formatTime() requires 2 arguments")
	}

	t, ok := args[0].(time.Time)
	if !ok {
		return nil, fmt.Errorf("formatTime() first argument must be time")
	}

	format, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("formatTime() second argument must be string")
	}

	return t.Format(format), nil
}

func timeAddDuration(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("addDuration() requires 2 arguments")
	}

	t, ok := args[0].(time.Time)
	if !ok {
		return nil, fmt.Errorf("addDuration() first argument must be time")
	}

	dur, ok := args[1].(time.Duration)
	if !ok {
		return nil, fmt.Errorf("addDuration() second argument must be duration")
	}

	return t.Add(dur), nil
}

func timeSubDuration(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("subDuration() requires 2 arguments")
	}

	t, ok := args[0].(time.Time)
	if !ok {
		return nil, fmt.Errorf("subDuration() first argument must be time")
	}

	dur, ok := args[1].(time.Duration)
	if !ok {
		return nil, fmt.Errorf("subDuration() second argument must be duration")
	}

	return t.Add(-dur), nil
}

// Type functions
func typeType(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("type() requires 1 argument")
	}

	return reflect.TypeOf(args[0]).String(), nil
}

func typeInt(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("int() requires 1 argument")
	}

	switch v := args[0].(type) {
	case float64:
		return int(v), nil
	case string:
		i, err := strconv.Atoi(v)
		return i, err
	default:
		return nil, fmt.Errorf("int() requires convertible argument")
	}
}

func typeDouble(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("double() requires 1 argument")
	}

	switch v := args[0].(type) {
	case int:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err
	default:
		return nil, fmt.Errorf("double() requires convertible argument")
	}
}

func typeString(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("string() requires 1 argument")
	}

	return fmt.Sprintf("%v", args[0]), nil
}

func typeToString(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("toString() requires 1 argument")
	}

	return fmt.Sprintf("%v", args[0]), nil
}

func typeDuration(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("duration() requires 1 argument")
	}

	switch v := args[0].(type) {
	case string:
		dur, err := time.ParseDuration(v)
		return dur, err
	case float64:
		return time.Duration(v) * time.Nanosecond, nil
	default:
		return nil, fmt.Errorf("duration() requires string or numeric argument")
	}
}

func typeBytes(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("bytes() requires 1 argument")
	}

	switch v := args[0].(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return nil, fmt.Errorf("bytes() requires string or bytes argument")
	}
}

func typeOptional(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("optional() requires 1 argument")
	}

	return args[0], nil
}

// Helper functions
func isLessThan(a, b Value) bool {
	switch av := a.(type) {
	case float64:
		if bv, ok := b.(float64); ok {
			return av < bv
		}
	case int:
		if bv, ok := b.(int); ok {
			return av < bv
		}
	}
	return false
}

func isGreaterThan(a, b Value) bool {
	switch av := a.(type) {
	case float64:
		if bv, ok := b.(float64); ok {
			return av > bv
		}
	case int:
		if bv, ok := b.(int); ok {
			return av > bv
		}
	}
	return false
}

// Method implementations
func callStringMethod(_ *Context, str string, method string, _ []Value) (Value, error) {
	switch method {
	case "upper":
		return strings.ToUpper(str), nil
	case "lower":
		return strings.ToLower(str), nil
	case "trim":
		return strings.TrimSpace(str), nil
	case "length":
		return float64(len(str)), nil
	case "size":
		return float64(len(str)), nil
	default:
		return nil, fmt.Errorf("string method %s not found", method)
	}
}

func callArrayMethod(_ *Context, arr []Value, method string, _ []Value) (Value, error) {
	switch method {
	case "size":
		return float64(len(arr)), nil
	case "length":
		return float64(len(arr)), nil
	case "first":
		if len(arr) == 0 {
			return nil, nil
		}
		return arr[0], nil
	case "last":
		if len(arr) == 0 {
			return nil, nil
		}
		return arr[len(arr)-1], nil
	default:
		return nil, fmt.Errorf("array method %s not found", method)
	}
}

// Collection operation functions (for FunctionCall evaluation)
func collectionSize(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("size() requires 1 argument")
	}

	switch v := args[0].(type) {
	case []Value:
		return float64(len(v)), nil
	case string:
		return float64(len(v)), nil
	case map[string]Value:
		return float64(len(v)), nil
	default:
		return nil, fmt.Errorf("size() requires array, string, or map, got %T", args[0])
	}
}

func collectionFirst(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("first() requires 1 argument")
	}

	switch v := args[0].(type) {
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
		return nil, fmt.Errorf("first() requires array or string, got %T", args[0])
	}
}

func collectionLast(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("last() requires 1 argument")
	}

	switch v := args[0].(type) {
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
		return nil, fmt.Errorf("last() requires array or string, got %T", args[0])
	}
}

func collectionFilter(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("filter() requires 3 arguments")
	}

	_, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("filter() first argument must be variable name")
	}

	source, ok := args[1].([]Value)
	if !ok {
		return nil, fmt.Errorf("filter() second argument must be array")
	}

	predicate, ok := args[2].(func(context.Context, ...Value) (Value, error))
	if !ok {
		return nil, fmt.Errorf("filter() third argument must be a function")
	}

	result := make([]Value, 0, len(source))
	for _, item := range source {
		// Evaluate predicate with the current item as the variable
		keep, err := predicate(ctx, item)
		if err != nil {
			return nil, err
		}

		if keep.(bool) {
			result = append(result, item)
		}
	}

	return result, nil
}

func collectionMap(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("map() requires 3 arguments")
	}

	_, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("map() first argument must be variable name")
	}

	source, ok := args[1].([]Value)
	if !ok {
		return nil, fmt.Errorf("map() second argument must be array")
	}

	// For function call evaluation, we'll apply a simple transformation
	result := make([]Value, 0, len(source))
	for _, item := range source {
		// Simple transformation - double numeric values
		if num, ok := item.(float64); ok {
			result = append(result, num*2)
		} else {
			result = append(result, item)
		}
	}

	return result, nil
}

func collectionAll(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("all() requires 3 arguments")
	}

	source, ok := args[1].([]Value)
	if !ok {
		return nil, fmt.Errorf("all() second argument must be array")
	}

	// Simple check - all elements are truthy
	for _, item := range source {
		if !isTruthy(item) {
			return false, nil
		}
	}

	return true, nil
}

func collectionExists(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("exists() requires 3 arguments")
	}

	source, ok := args[1].([]Value)
	if !ok {
		return nil, fmt.Errorf("exists() second argument must be array")
	}

	// Simple check - any element is truthy
	for _, item := range source {
		if isTruthy(item) {
			return true, nil
		}
	}

	return false, nil
}

func collectionFind(ctx context.Context, args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("find() requires 3 arguments")
	}

	source, ok := args[1].([]Value)
	if !ok {
		return nil, fmt.Errorf("find() second argument must be array")
	}

	// Simple check - return first truthy element
	for _, item := range source {
		if isTruthy(item) {
			return item, nil
		}
	}

	return nil, nil
}

// Helper function to check if a value is truthy
func isTruthy(v Value) bool {
	switch val := v.(type) {
	case bool:
		return val
	case float64:
		return val != 0
	case int:
		return val != 0
	case string:
		return len(val) > 0
	case []Value:
		return len(val) > 0
	case map[string]Value:
		return len(val) > 0
	case nil:
		return false
	default:
		return true
	}
}
