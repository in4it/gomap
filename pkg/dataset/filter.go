package dataset

import (
	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
	"github.com/in4it/gomap/pkg/writers"
)

type Filter struct {
	Function  types.FilterFunction
	inputFile input.Input
	output    writers.WriterReader
	invoked   int
}

func NewFilter(fn types.FilterFunction) *Filter {
	return &Filter{
		Function: fn,
	}
}
func (m *Filter) Do(partition, totalPartitions int) error {
	for m.inputFile.Scan() {
		m.invoked++
		_, value := m.inputFile.Bytes()
		if m.Function(value) {
			m.output.Write(utils.PutRecord(value))
		}
	}

	if _, err := m.inputFile.Err(); err != nil {
		return err
	}
	return nil
}

func (m *Filter) GetOutputKV() (writers.WriterReader, writers.WriterReader) {
	m.output.Close()
	return nil, m.output
}
func (m *Filter) GetOutputType() string {
	return "value"
}

func (m *Filter) GetStepType() string {
	return "filter"
}
func (m *Filter) GetFunction() interface{} {
	return m.Function
}
func (m *Filter) SetInput(inputFile input.Input) {
	m.inputFile = inputFile
}
func (m *Filter) SetOutputKV(keyWriter writers.WriterReader, valueWriter writers.WriterReader) {
	keyWriter.Cleanup()
	m.output = valueWriter
}

func (m *Filter) GetStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
