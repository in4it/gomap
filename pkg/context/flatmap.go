package context

import (
	"bytes"

	"github.com/in4it/gomap/pkg/types"
)

type FlatMap struct {
	function  types.FlatMapFunction
	inputFile *Input
	output    bytes.Buffer
	invoked   int
}

func (c *Context) FlatMap(fn types.FlatMapFunction) *Context {
	c.AddStep(newFlatMap(fn))
	return c
}
func newFlatMap(fn types.FlatMapFunction) *FlatMap {
	return &FlatMap{
		function: fn,
	}
}
func (m *FlatMap) do(partition, totalPartitions int) error {
	for m.inputFile.Scan() {
		_, value := m.inputFile.Bytes()
		m.invoked++
		for _, output := range m.function(value) {
			m.output.WriteString(string(output) + "\n")
		}
	}

	if _, err := m.inputFile.Err(); err != nil {
		return err
	}
	return nil
}

func (m *FlatMap) getOutput() bytes.Buffer {
	return m.output
}
func (m *FlatMap) getOutputKV() (bytes.Buffer, bytes.Buffer) {
	return bytes.Buffer{}, bytes.Buffer{}
}
func (m *FlatMap) getOutputType() string {
	return "value"
}

func (m *FlatMap) getStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
func (m *FlatMap) getStepType() string {
	return "flatmap"
}
func (m *FlatMap) getFunction() interface{} {
	return m.function
}
func (m *FlatMap) setInput(inputFile *Input) {
	m.inputFile = inputFile
}
