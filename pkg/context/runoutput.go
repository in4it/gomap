package context

import (
	"fmt"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

// RunOutput contains all the contexts with their respective output
type RunOutput struct {
	Contexts []*Context
}

// Print prints the output using fmt.Printf
func (r *RunOutput) Print() {
	key, value := r.GetKV()
	for k := range key {
		fmt.Printf("%s: %s\n", string(key[k]), value[k])
	}
}

// Get retrieves all the values from the output
func (r *RunOutput) Get() []types.RawOutput {
	ret := []types.RawOutput{}
	for _, context := range r.Contexts {
		if context.outputType == "value" {
			for {
				moreRecords, record, err := utils.ReadRecord(&context.outputValue)
				if err != nil {
					panic(err)
				}
				if !moreRecords {
					break
				}
				ret = append(ret, record)
			}
		}
	}
	return ret
}

// GetKV retrieves all key/value pairs from the output
func (r *RunOutput) GetKV() ([]types.RawOutput, []types.RawOutput) {
	keys := []types.RawOutput{}
	values := []types.RawOutput{}
	for _, context := range r.Contexts {
		if context.outputType == "kv" {
			for {
				moreRecords, record, err := utils.ReadRecord(&context.outputKey)
				if err != nil {
					panic(err)
				}
				if !moreRecords {
					break
				}
				keys = append(keys, record)
			}
			for {
				moreRecords, record, err := utils.ReadRecord(&context.outputValue)
				if err != nil {
					panic(err)
				}
				if !moreRecords {
					break
				}
				values = append(values, record)
			}
		}
	}
	return keys, values
}

// Foreach lets you pass a function to iterate over the output.
// The function passed to foreach is executed for every unique key.
func (r *RunOutput) Foreach(fn types.ForeachFunction) {
	for _, context := range r.Contexts {
		switch context.outputType {
		case "kv":
			for {
				moreRecords, keyRecord, err := utils.ReadRecord(&context.outputKey)
				if err != nil {
					panic(err)
				}
				if !moreRecords {
					break
				}
				moreValueRecords, valueRecord, err := utils.ReadRecord(&context.outputValue)
				if err != nil {
					panic(err)
				}
				if !moreValueRecords {
					break
				}
				fn(keyRecord, valueRecord)
			}
		case "value":
			for {
				moreValueRecords, valueRecord, err := utils.ReadRecord(&context.outputValue)
				if err != nil {
					panic(err)
				}
				if !moreValueRecords {
					break
				}
				fn([]byte{}, valueRecord)
			}
		default:
			panic("OutputType not recognized")
		}
	}
}
