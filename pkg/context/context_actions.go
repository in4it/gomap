package context

import (
	"github.com/in4it/gomap/pkg/dataset"
	"github.com/in4it/gomap/pkg/types"
)

func (c *Context) FlatMap(fn types.FlatMapFunction) *Context {
	c.AddStep(dataset.NewFlatMap(fn))
	return c
}
func (c *Context) Map(fn types.MapFunction) *Context {
	c.AddStep(dataset.NewMap(fn))
	return c
}
func (c *Context) MapToKV(fn types.MapToKVFunction) *Context {
	c.AddStep(dataset.NewMapToKV(fn))
	return c
}
func (c *Context) ReduceByKey(fn types.ReduceByKeyFunction) *Context {
	c.AddStep(dataset.NewReduceByKey(fn))
	return c
}
