package context

import (
	"bufio"
	"fmt"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

type RunOutput struct {
	Contexts []*Context
}

func (r *RunOutput) Print() {
	for _, context := range r.Contexts {
		keyScanner := bufio.NewScanner(&context.outputKey)
		valueScanner := bufio.NewScanner(&context.outputValue)

		for keyScanner.Scan() {
			valueScanner.Scan()
			fmt.Println(keyScanner.Text() + "," + valueScanner.Text())
		}
		if err := keyScanner.Err(); err != nil {
			panic(err)
		}
		if err := valueScanner.Err(); err != nil {
			panic(err)
		}
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
