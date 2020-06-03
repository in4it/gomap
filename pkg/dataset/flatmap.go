package dataset

import (
	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
	"github.com/in4it/gomap/pkg/writers"
)

type FlatMap struct {
	Function  types.FlatMapFunction
	inputFile input.Input
	output    writers.WriterReader
	invoked   int
}

func NewFlatMap(fn types.FlatMapFunction) *FlatMap {
	return &FlatMap{
		Function: fn,
	}
}
func (m *FlatMap) Do(partition, totalPartitions int) error {
	for m.inputFile.Scan() {
		_, value := m.inputFile.Bytes()
		m.invoked++
		for _, output := range m.Function(value) {
			m.output.Write(utils.PutRecord(output))
		}
	}

	if _, err := m.inputFile.Err(); err != nil {
		return err
	}
	return nil
}

func (m *FlatMap) GetOutputKV() (writers.WriterReader, writers.WriterReader) {
	return nil, m.output
}
func (m *FlatMap) GetOutputType() string {
	return "value"
}

func (m *FlatMap) GetStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
func (m *FlatMap) GetStepType() string {
	return "flatmap"
}
func (m *FlatMap) GetFunction() interface{} {
	return m.Function
}
func (m *FlatMap) SetInput(inputFile input.Input) {
	m.inputFile = inputFile
}
func (m *FlatMap) SetOutputKV(keyWriter writers.WriterReader, valueWriter writers.WriterReader) {
	m.output = valueWriter
}
