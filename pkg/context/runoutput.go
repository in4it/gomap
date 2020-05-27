package context

import (
	"fmt"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

type RunOutput struct {
	Contexts []*Context
}

func (r *RunOutput) Print() {
	key, value := r.GetKV()
	for k := range key {
		fmt.Printf("%s: %s\n", string(key[k]), value[k])
	}
}
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
func (r *RunOutput) Foreach(fn types.ForeachFunction) {
	for _, context := range r.Contexts {
		if context.outputType == "kv" {
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
		}
	}
}
