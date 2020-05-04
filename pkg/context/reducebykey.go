package context

import (
	"bufio"
	"bytes"
)

type ReduceByKey struct {
	function     ReduceByKeyFunction
	scannerKey   *bufio.Scanner
	scannerValue *bufio.Scanner
	outputKey    bytes.Buffer
	outputValue  bytes.Buffer
	outputType   string
}

func (c *Context) ReduceByKey(fn ReduceByKeyFunction) *Context {
	c.AddStep(newReduceByKey(fn))
	return c
}
func newReduceByKey(fn ReduceByKeyFunction) *ReduceByKey {
	return &ReduceByKey{
		function: fn,
	}
}
func (m *ReduceByKey) do() error {
	m.outputType = "kv"
	reduced := make(map[string][]byte)
	for m.scannerKey.Scan() {
		m.scannerValue.Scan()
		key := m.scannerKey.Bytes()
		value := m.scannerValue.Bytes()
		if reducedValue, ok := reduced[string(key)]; ok {
			reduced[string(key)] = m.function(reducedValue, m.scannerValue.Bytes())
		} else {
			reduced[string(key)] = value
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
