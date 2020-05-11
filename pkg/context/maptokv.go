package context

import (
	"bytes"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

type MapToKV struct {
	function    types.MapToKVFunction
	inputFile   *Input
	outputKey   bytes.Buffer
	outputValue bytes.Buffer
	invoked     int
}

func (c *Context) MapToKV(fn types.MapToKVFunction) *Context {
	c.AddStep(newMapToKV(fn))
	return c
}
func newMapToKV(fn types.MapToKVFunction) *MapToKV {
	return &MapToKV{
		function: fn,
	}
}
func (m *MapToKV) do(partition, totalPartitions int) error {
	for m.inputFile.Scan() {
		m.invoked++
		_, inputValue := m.inputFile.Bytes()
		key, value := m.function(inputValue)
		m.outputKey.Write(utils.PutRecord(key))
		m.outputValue.Write(utils.PutRecord(value))
	}

	if _, err := m.inputFile.Err(); err != nil {
		return err
	}
	return nil
}

func (m *MapToKV) getOutputKV() (bytes.Buffer, bytes.Buffer) {
	return m.outputKey, m.outputValue
}
func (m *MapToKV) getOutputType() string {
	return "kv"
}

func (m *MapToKV) getStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
func (m *MapToKV) getStepType() string {
	return "maptokv"
}
func (m *MapToKV) getFunction() interface{} {
	return m.function
}
func (m *MapToKV) setInput(inputFile *Input) {
	m.inputFile = inputFile
}
