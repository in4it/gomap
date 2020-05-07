package context

import (
	"bufio"
	"fmt"

	"github.com/in4it/gomap/pkg/types"
)

type RunOutput struct {
	Contexts []*Context
}

func (r *RunOutput) Print() {
	for _, context := range r.Contexts {
		scanner := bufio.NewScanner(&context.output)
		keyScanner := bufio.NewScanner(&context.outputKey)
		valueScanner := bufio.NewScanner(&context.outputValue)

		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
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
		scanner := bufio.NewScanner(&context.output)

		for scanner.Scan() {
			ret = append(ret, scanner.Bytes())
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
	}
	return ret
}

func (r *RunOutput) GetKV() ([]types.RawOutput, []types.RawOutput) {
	keys := []types.RawOutput{}
	values := []types.RawOutput{}
	for _, context := range r.Contexts {
		keyScanner := bufio.NewScanner(&context.outputKey)
		valueScanner := bufio.NewScanner(&context.outputValue)

		for keyScanner.Scan() {
			valueScanner.Scan()
			keys = append(keys, keyScanner.Bytes())
			values = append(values, valueScanner.Bytes())
		}
		if err := keyScanner.Err(); err != nil {
			panic(err)
		}
		if err := valueScanner.Err(); err != nil {
			panic(err)
		}
	}
	return keys, values
}
