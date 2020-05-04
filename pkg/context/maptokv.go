package context

import (
	"bufio"
	"bytes"
)

type MapToKV struct {
	function    MapToKVFunction
	scanner     *bufio.Scanner
	output      bytes.Buffer
	outputKey   bytes.Buffer
	outputValue bytes.Buffer
	outputType  string
}

func (c *Context) MapToKV(fn MapToKVFunction) *Context {
	c.AddStep(newMapToKV(fn))
	return c
}
func newMapToKV(fn MapToKVFunction) *MapToKV {
	return &MapToKV{
		function: fn,
	}
}
func (m *MapToKV) do() error {
	m.outputType = "kv"
	for m.scanner.Scan() {
		key, value := m.function(m.scanner.Bytes())
		m.outputKey.Write(append(key, []byte("\n")...))
		m.outputValue.Write(append(value, []byte("\n")...))
	}

	if err := m.scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (m *MapToKV) getOutput() bytes.Buffer {
	return m.output
}

func (m *MapToKV) getOutputKV() (bytes.Buffer, bytes.Buffer) {
	return m.outputKey, m.outputValue
}
func (m *MapToKV) getOutputType() string {
	return m.outputType
}

func (m *MapToKV) setScanner(scanner *bufio.Scanner) {
	m.scanner = scanner
}
func (m *MapToKV) setScannerKV(scannerKey, scannerValue *bufio.Scanner) {
}
