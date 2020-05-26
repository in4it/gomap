package context

import (
	"bytes"

	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

type Map struct {
	function  types.MapFunction
	inputFile input.Input
	output    bytes.Buffer
	invoked   int
}

func (c *Context) Map(fn types.MapFunction) *Context {
	c.AddStep(newMap(fn))
	return c
}
func newMap(fn types.MapFunction) *Map {
	return &Map{
		function: fn,
	}
}
func (m *Map) do(partition, totalPartitions int) error {
	for m.inputFile.Scan() {
		_, value := m.inputFile.Bytes()
		res := m.function(value)
		m.output.Write(utils.PutRecord(res))
	}

	if _, err := m.inputFile.Err(); err != nil {
		return err
	}
	return nil
}

func (m *Map) getOutputKV() (bytes.Buffer, bytes.Buffer) {
	return bytes.Buffer{}, m.output
}
func (m *Map) getOutputType() string {
	return "value"
}

func (m *Map) getStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
func (m *Map) getStepType() string {
	return "map"
}

func (m *Map) getFunction() interface{} {
	return m.function
}
func (m *Map) setInput(inputFile input.Input) {
	m.inputFile = inputFile
}
