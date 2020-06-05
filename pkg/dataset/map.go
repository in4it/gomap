package dataset

import (
	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
	"github.com/in4it/gomap/pkg/writers"
)

type Map struct {
	Function  types.MapFunction
	inputFile input.Input
	output    writers.WriterReader
	invoked   int
}

func NewMap(fn types.MapFunction) *Map {
	return &Map{
		Function: fn,
	}
}
func (m *Map) Do(partition, totalPartitions int) error {
	for m.inputFile.Scan() {
		_, value := m.inputFile.Bytes()
		res := m.Function(value)
		m.output.Write(utils.PutRecord(res))
	}

	if _, err := m.inputFile.Err(); err != nil {
		return err
	}
	return nil
}

func (m *Map) GetOutputKV() (writers.WriterReader, writers.WriterReader) {
	m.output.Close()
	return nil, m.output
}
func (m *Map) GetOutputType() string {
	return "value"
}

func (m *Map) GetStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
func (m *Map) GetStepType() string {
	return "map"
}

func (m *Map) GetFunction() interface{} {
	return m.Function
}
func (m *Map) SetInput(inputFile input.Input) {
	m.inputFile = inputFile
}
func (m *Map) SetOutputKV(keyWriter writers.WriterReader, valueWriter writers.WriterReader) {
	keyWriter.Cleanup()
	m.output = valueWriter
}
