package dataset

import (
	"bytes"

	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

type MapToKV struct {
	Function    types.MapToKVFunction
	inputFile   input.Input
	outputKey   bytes.Buffer
	outputValue bytes.Buffer
	invoked     int
}

func NewMapToKV(fn types.MapToKVFunction) *MapToKV {
	return &MapToKV{
		Function: fn,
	}
}
func (m *MapToKV) Do(partition, totalPartitions int) error {
	for m.inputFile.Scan() {
		m.invoked++
		_, inputValue := m.inputFile.Bytes()
		key, value := m.Function(inputValue)
		m.outputKey.Write(utils.PutRecord(key))
		m.outputValue.Write(utils.PutRecord(value))
	}

	if _, err := m.inputFile.Err(); err != nil {
		return err
	}
	return nil
}

func (m *MapToKV) GetOutputKV() (bytes.Buffer, bytes.Buffer) {
	return m.outputKey, m.outputValue
}
func (m *MapToKV) GetOutputType() string {
	return "kv"
}

func (m *MapToKV) GetStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
func (m *MapToKV) GetStepType() string {
	return "maptokv"
}
func (m *MapToKV) GetFunction() interface{} {
	return m.Function
}
func (m *MapToKV) SetInput(inputFile input.Input) {
	m.inputFile = inputFile
}
