package context

import (
	"bufio"
	"bytes"
	"strings"

	"github.com/in4it/gomap/pkg/types"
)

type ReduceByKey struct {
	function     types.ReduceByKeyFunction
	scannerKey   *bufio.Scanner
	scannerValue *bufio.Scanner
	outputKey    bytes.Buffer
	outputValue  bytes.Buffer
	outputType   string
	invoked      int
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
	m.outputType = "kv"
	m.outputKey = bytes.Buffer{}
	m.outputValue = bytes.Buffer{}

	reduced := make(map[string][]byte)

	for m.scannerKey.Scan() {
		m.scannerValue.Scan()
		key := m.scannerKey.Bytes()
		value := m.scannerValue.Bytes()
		m.invoked++
		if reducedValue, ok := reduced[string(key)]; ok {
			b := m.scannerValue.Bytes()
			reduced[string(key)] = m.function(reducedValue, b)
		} else {
			reduced[string(key)] = []byte(strings.TrimSuffix(string(value), "\n"))
		}
	}

	for key, value := range reduced {
		m.outputKey.Write([]byte(key + "\n"))
		m.outputValue.Write(append(value, []byte("\n")...))
	}

	if err := m.scannerKey.Err(); err != nil {
		return err
	}
	if err := m.scannerValue.Err(); err != nil {
		return err
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
	return m.outputType
}
func (m *ReduceByKey) setScanner(scanner *bufio.Scanner) {
}
func (m *ReduceByKey) setScannerKV(scannerKey, scannerValue *bufio.Scanner) {
	m.scannerKey = scannerKey
	m.scannerValue = scannerValue
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
