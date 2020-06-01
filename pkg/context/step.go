package context

import (
	"fmt"
	"reflect"

	"github.com/in4it/gomap/pkg/dataset"
	"github.com/in4it/gomap/pkg/types"
)

func (d *Context) addStep(s dataset.Step) {
	d.steps = append(d.steps, s)
}

func copySteps(input []dataset.Step) []dataset.Step {
	res := make([]dataset.Step, len(input))
	for k, v := range input {
		switch t := reflect.TypeOf(v).String(); t {
		case "*dataset.FlatMap":
			res[k] = &dataset.FlatMap{
				Function: v.GetFunction().(types.FlatMapFunction),
			}
		case "*dataset.MapToKV":
			res[k] = &dataset.MapToKV{
				Function: v.GetFunction().(types.MapToKVFunction),
			}
		case "*dataset.ReduceByKey":
			res[k] = &dataset.ReduceByKey{
				Function: v.GetFunction().(types.ReduceByKeyFunction),
			}
		case "*dataset.Map":
			res[k] = &dataset.Map{
				Function: v.GetFunction().(types.MapFunction),
			}
		case "*dataset.Filter":
			res[k] = &dataset.Filter{
				Function: v.GetFunction().(types.FilterFunction),
			}
		default:
			panic(fmt.Errorf("Unrecognized type: %s", reflect.TypeOf(v)))
		}
	}
	return res
}
