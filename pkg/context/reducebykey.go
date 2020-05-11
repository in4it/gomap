package context

import (
	"bytes"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

type ReduceByKey struct {
	function    types.ReduceByKeyFunction
	inputFile   *Input
	outputKey   bytes.Buffer
	outputValue bytes.Buffer
	invoked     int
}

func (c *Context) ReduceByKey(fn types.ReduceByKeyFunction) *Context {
	c.AddStep(newReduceByKey(fn))
	return c
}
func newReduceByKey(fn types.ReduceByKeyFunction) *ReduceByKey {
	return &ReduceByKey{
		function: fn,
	}
}
func (m *ReduceByKey) do(partition, totalPartitions int) error {
	m.outputKey = bytes.Buffer{}
	m.outputValue = bytes.Buffer{}

	reduced := make(map[string][]byte)

	for m.inputFile.Scan() {
		key, value := m.inputFile.Bytes()
		m.invoked++
		if reducedValue, ok := reduced[string(key)]; ok {
			reduced[string(key)] = m.function(reducedValue, value)
		} else {
			reduced[string(key)] = utils.PutRecord(value)
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

func (m *ReduceByKey) getOutput() bytes.Buffer {
	return bytes.Buffer{}
}
func (m *ReduceByKey) getOutputKV() (bytes.Buffer, bytes.Buffer) {
	return m.outputKey, m.outputValue
}
func (m *ReduceByKey) getOutputType() string {
	return "kv"
}
func (m *ReduceByKey) getStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
func (m *ReduceByKey) getStepType() string {
	return "reducebykey"
}
func (m *ReduceByKey) getFunction() interface{} {
	return m.function
}
func (m *ReduceByKey) setInput(inputFile *Input) {
	m.inputFile = inputFile
}
