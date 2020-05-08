package context

import (
	"fmt"
	"reflect"

	"github.com/in4it/gomap/pkg/types"
)

func (d *Context) AddStep(s Step) {
	d.steps = append(d.steps, s)
}

func copySteps(input []Step) []Step {
	res := make([]Step, len(input))
	for k, v := range input {
		switch t := reflect.TypeOf(v).String(); t {
		case "*context.FlatMap":
			res[k] = &FlatMap{
				function: v.getFunction().(types.FlatMapFunction),
			}
		case "*context.MapToKV":
			res[k] = &MapToKV{
				function: v.getFunction().(types.MapToKVFunction),
			}
		case "*context.ReduceByKey":
			res[k] = &ReduceByKey{
				function: v.getFunction().(types.ReduceByKeyFunction),
			}
		case "*context.Map":
			res[k] = &Map{
				function: v.getFunction().(types.MapFunction),
			}
		default:
			panic(fmt.Errorf("Unrecognized type: %s", reflect.TypeOf(v)))
		}
	}
	return res
}
