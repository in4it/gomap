package context

import (
	"github.com/in4it/gomap/pkg/dataset"
	"github.com/in4it/gomap/pkg/types"
)

// FlatMap expects a function with one value as input and outputs
// one or more values in a slice
func (c *Context) FlatMap(fn types.FlatMapFunction) *Context {
	c.AddStep(dataset.NewFlatMap(fn))
	return c
}

// Map expects a function with one value as input and outputs
// one value
func (c *Context) Map(fn types.MapFunction) *Context {
	c.AddStep(dataset.NewMap(fn))
	return c
}

// MapToKV expects a function with one value and outputs a key
// and value
func (c *Context) MapToKV(fn types.MapToKVFunction) *Context {
	c.AddStep(dataset.NewMapToKV(fn))
	return c
}

// ReduceByKey can only run on a Key-Value pair. ReduceByKey groups
// the values with the same key. ReduceByKey expects a function with
// 2 values and needs to return 1 "reduced" value
func (c *Context) ReduceByKey(fn types.ReduceByKeyFunction) *Context {
	c.AddStep(dataset.NewReduceByKey(fn))
	return c
}
