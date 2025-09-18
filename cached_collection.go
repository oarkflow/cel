package cel

import (
	"sort"
	"strings"
	"sync"
)

// CachedCollections provides zero-allocation collection operations where possible
type CachedCollections struct{}

var Cached = &CachedCollections{}

// Reusable slice pools sized for different use cases
var (
	smallSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Value, 0, 8)
		},
	}
	mediumSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Value, 0, 32)
		},
	}
	largeSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Value, 0, 128)
		},
	}
	xlargeSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Value, 0, 512)
		},
	}

	// Context pool for reuse
	ultraContextPool = sync.Pool{
		New: func() interface{} {
			return &Context{
				Variables: make(map[string]Value, 8),
			}
		},
	}
)

// getOptimalSlice returns the best-sized slice from pools
func getOptimalSlice(size int) []Value {
	switch {
	case size <= 8:
		return smallSlicePool.Get().([]Value)[:0]
	case size <= 32:
		return mediumSlicePool.Get().([]Value)[:0]
	case size <= 128:
		return largeSlicePool.Get().([]Value)[:0]
	default:
		return xlargeSlicePool.Get().([]Value)[:0]
	}
}

// putSliceBack returns slice to appropriate pool
func putSliceBack(slice []Value) {
	if slice == nil {
		return
	}
	cap := cap(slice)
	switch {
	case cap <= 8:
		smallSlicePool.Put(slice)
	case cap <= 32:
		mediumSlicePool.Put(slice)
	case cap <= 128:
		largeSlicePool.Put(slice)
	case cap <= 512:
		xlargeSlicePool.Put(slice)
	}
}

// getUltraContext returns a reusable context
func getUltraContext() *Context {
	ctx := ultraContextPool.Get().(*Context)
	// Clear existing data
	for k := range ctx.Variables {
		delete(ctx.Variables, k)
	}
	return ctx
}

// putUltraContext returns context to pool
func putUltraContext(ctx *Context) {
	ultraContextPool.Put(ctx)
}

// CachedFilter performs filtering with minimal allocations
func (ufc *CachedCollections) Filter(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	if len(items) == 0 {
		return items, nil
	}

	// Get optimal slice and context
	filtered := getOptimalSlice(len(items) / 2) // Conservative estimate
	defer putSliceBack(filtered)

	newCtx := getUltraContext()
	defer putUltraContext(newCtx)

	// Set up context efficiently
	newCtx.Functions = baseCtx.Functions
	for k, v := range baseCtx.Variables {
		newCtx.Variables[k] = v
	}

	// Filter items
	for _, item := range items {
		newCtx.Variables[variable] = item
		result, err := body.Evaluate(newCtx)
		if err != nil {
			return nil, err
		}
		if toBool(result) {
			filtered = append(filtered, item)
		}
	}

	// Create result slice (can't return pooled slice)
	result := make([]Value, len(filtered))
	copy(result, filtered)

	return result, nil
}

// CachedMap performs mapping with pre-allocated result
func (ufc *CachedCollections) Map(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	if len(items) == 0 {
		return items, nil
	}

	// Pre-allocate exact size (no reallocation needed)
	mapped := make([]Value, len(items))

	newCtx := getUltraContext()
	defer putUltraContext(newCtx)

	// Set up context efficiently
	newCtx.Functions = baseCtx.Functions
	for k, v := range baseCtx.Variables {
		newCtx.Variables[k] = v
	}

	// Map items
	for i, item := range items {
		newCtx.Variables[variable] = item
		result, err := body.Evaluate(newCtx)
		if err != nil {
			return nil, err
		}
		mapped[i] = result
	}

	return mapped, nil
}

// ParallelMap performs mapping in parallel for large collections
func (ufc *CachedCollections) ParallelMap(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	if len(items) == 0 {
		return items, nil
	}

	// Only use parallel processing for large collections
	if len(items) < 1000 {
		return ufc.Map(items, variable, body, baseCtx)
	}

	// Pre-allocate exact size
	mapped := make([]Value, len(items))

	// Number of goroutines (adjust based on CPU cores)
	numWorkers := 4
	if len(items) < numWorkers*10 {
		numWorkers = len(items) / 10
		if numWorkers < 1 {
			numWorkers = 1
		}
	}

	// Channel for distributing work
	type workItem struct {
		index int
		item  Value
	}
	workChan := make(chan workItem, len(items))
	resultChan := make(chan struct {
		index int
		value Value
		err   error
	}, len(items))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerCtx := getUltraContext()
			defer putUltraContext(workerCtx)

			// Set up context
			workerCtx.Functions = baseCtx.Functions
			for k, v := range baseCtx.Variables {
				workerCtx.Variables[k] = v
			}

			for work := range workChan {
				workerCtx.Variables[variable] = work.item
				result, err := body.Evaluate(workerCtx)
				resultChan <- struct {
					index int
					value Value
					err   error
				}{work.index, result, err}
			}
		}()
	}

	// Send work
	go func() {
		for i, item := range items {
			workChan <- workItem{i, item}
		}
		close(workChan)
	}()

	// Collect results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results
	for result := range resultChan {
		if result.err != nil {
			return nil, result.err
		}
		mapped[result.index] = result.value
	}

	return mapped, nil
}

// CachedJoin performs string joining with pre-calculated buffer size
func (ufc *CachedCollections) Join(items []Value, separator string) string {
	if len(items) == 0 {
		return ""
	}

	if len(items) == 1 {
		return toString(items[0])
	}

	// Pre-calculate total length to avoid buffer growth
	totalLen := (len(items) - 1) * len(separator)
	stringItems := make([]string, len(items)) // Convert once
	for i, item := range items {
		s := toString(item)
		stringItems[i] = s
		totalLen += len(s)
	}

	// Use efficient builder
	result := make([]byte, 0, totalLen)
	result = append(result, stringItems[0]...)

	for i := 1; i < len(stringItems); i++ {
		result = append(result, separator...)
		result = append(result, stringItems[i]...)
	}

	return string(result)
}

// OptimizedSort performs sorting with quicksort algorithm for better performance
func (ufc *CachedCollections) Sort(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	if len(items) <= 1 {
		return items, nil
	}

	// Create a copy to sort
	sorted := make([]Value, len(items))
	copy(sorted, items)

	// For small collections, use insertion sort
	if len(sorted) < 10 {
		return ufc.insertionSort(sorted, variable, body, baseCtx)
	}

	// For larger collections, use quicksort
	return ufc.quickSort(sorted, variable, body, baseCtx)
}

// insertionSort performs insertion sort for small collections
func (ufc *CachedCollections) insertionSort(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	ctx := getUltraContext()
	defer putUltraContext(ctx)

	// Set up context
	ctx.Functions = baseCtx.Functions
	for k, v := range baseCtx.Variables {
		ctx.Variables[k] = v
	}

	for i := 1; i < len(items); i++ {
		key := items[i]
		j := i - 1

		// Compare key with each element on the left until an element smaller than it is found
		for j >= 0 {
			ctx.Variables[variable] = key
			keyVal, err := body.Evaluate(ctx)
			if err != nil {
				return nil, err
			}

			ctx.Variables[variable] = items[j]
			jVal, err := body.Evaluate(ctx)
			if err != nil {
				return nil, err
			}

			if compare(keyVal, jVal) >= 0 {
				break
			}

			items[j+1] = items[j]
			j = j - 1
		}
		items[j+1] = key
	}

	return items, nil
}

// quickSort performs quicksort for larger collections
func (ufc *CachedCollections) quickSort(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	ctx := getUltraContext()
	defer putUltraContext(ctx)

	// Set up context
	ctx.Functions = baseCtx.Functions
	for k, v := range baseCtx.Variables {
		ctx.Variables[k] = v
	}

	// Define a less function for sorting
	less := func(i, j int) bool {
		ctx.Variables[variable] = items[i]
		iVal, err := body.Evaluate(ctx)
		if err != nil {
			return false // In case of error, maintain original order
		}

		ctx.Variables[variable] = items[j]
		jVal, err := body.Evaluate(ctx)
		if err != nil {
			return false // In case of error, maintain original order
		}

		return compare(iVal, jVal) < 0
	}

	// Use Go's built-in sort with our custom less function
	sort.Slice(items, less)

	return items, nil
}

// OptimizedDistinct removes duplicate values efficiently
func (ufc *CachedCollections) Distinct(items []Value) []Value {
	if len(items) <= 1 {
		return items
	}

	// Use a map to track seen values
	seen := make(map[string]bool, len(items))
	result := make([]Value, 0, len(items))

	for _, item := range items {
		key := toString(item)
		if !seen[key] {
			seen[key] = true
			result = append(result, item)
		}
	}

	return result
}

// OptimizedFlatten flattens nested collections efficiently
func (ufc *CachedCollections) Flatten(items []Value) []Value {
	if len(items) == 0 {
		return items
	}

	// Pre-allocate with estimated size
	result := make([]Value, 0, len(items)*2)

	// Recursive flatten function
	var flattenRecursive func([]Value)
	flattenRecursive = func(items []Value) {
		for _, item := range items {
			if subCollection := toValueSlice(item); subCollection != nil {
				flattenRecursive(subCollection)
			} else {
				result = append(result, item)
			}
		}
	}

	flattenRecursive(items)
	return result
}

// OptimizedMethodChain detects and optimizes common method chains
type OptimizedMethodChain struct {
	ops []chainOp
}

type chainOp struct {
	method string
	args   []Value
}

// DetectChainOptimization attempts to optimize complex method chains
func DetectChainOptimization(obj Value, method string, args []Value) (Value, bool, error) {
	// For now, focus on collection -> string chains (filter/map -> join)
	if slice, ok := obj.([]Value); ok {
		switch method {
		case "join":
			if len(args) == 1 {
				// Simple join implementation
				if len(slice) == 0 {
					return "", true, nil
				}
				if len(slice) == 1 {
					return toString(slice[0]), true, nil
				}

				separator := toString(args[0])
				var result strings.Builder
				for i, item := range slice {
					if i > 0 {
						result.WriteString(separator)
					}
					result.WriteString(toString(item))
				}
				return result.String(), true, nil
			}
		case "distinct":
			if len(args) == 0 {
				return Cached.Distinct(slice), true, nil
			}
		case "flatten":
			if len(args) == 0 {
				return Cached.Flatten(slice), true, nil
			}
		}
	}
	return nil, false, nil
}
