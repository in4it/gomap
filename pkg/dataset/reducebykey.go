package dataset

import (
	"bytes"

	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
	"github.com/in4it/gomap/pkg/writers"
)

type ReduceByKey struct {
	Function    types.ReduceByKeyFunction
	inputFile   input.Input
	outputKey   writers.WriterReader
	outputValue writers.WriterReader
	invoked     int
}

func NewReduceByKey(fn types.ReduceByKeyFunction) *ReduceByKey {
	return &ReduceByKey{
		Function: fn,
	}
}
func (m *ReduceByKey) Do(partition, totalPartitions int) error {
	reduced := make(map[string][]byte)

	for m.inputFile.Scan() {
		key, value := m.inputFile.Bytes()
		m.invoked++
		if reducedValue, ok := reduced[string(key)]; ok {
			reduced[string(key)] = m.Function(reducedValue, value)
		} else {
			reduced[string(key)] = value
		}
	}

	for key, value := range reduced {
		m.outputKey.Write(utils.PutRecord([]byte(key)))
		m.outputValue.Write(utils.PutRecord(value))
	}
	err1, err2 := m.inputFile.Err()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func (m *ReduceByKey) GetOutput() bytes.Buffer {
	return bytes.Buffer{}
}
func (m *ReduceByKey) GetOutputKV() (writers.WriterReader, writers.WriterReader) {
	return m.outputKey, m.outputValue
}
func (m *ReduceByKey) GetOutputType() string {
	return "kv"
}
func (m *ReduceByKey) GetStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
func (m *ReduceByKey) GetStepType() string {
	return "reducebykey"
}
func (m *ReduceByKey) GetFunction() interface{} {
	return m.Function
}
func (m *ReduceByKey) SetInput(inputFile input.Input) {
	m.inputFile = inputFile
}
func (m *ReduceByKey) SetOutputKV(keyWriter writers.WriterReader, valueWriter writers.WriterReader) {
	m.outputKey = keyWriter
	m.outputValue = valueWriter
}
