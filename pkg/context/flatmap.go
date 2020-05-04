package context

import (
	"bufio"
	"bytes"
)

type FlatMap struct {
	function FlatMapFunction
	scanner  *bufio.Scanner
	output   bytes.Buffer
	invoked  int
}

func (c *Context) FlatMap(fn FlatMapFunction) *Context {
	c.AddStep(newFlatMap(fn))
	return c
}
func newFlatMap(fn FlatMapFunction) *FlatMap {
	return &FlatMap{
		function: fn,
	}
}
func (m *FlatMap) do() error {
	for m.scanner.Scan() {
		m.invoked++
		for _, output := range m.function(m.scanner.Bytes()) {
			m.output.WriteString(string(output) + "\n")
		}
	}

	if err := m.scanner.Err(); err != nil {
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

func (m *FlatMap) setScanner(scanner *bufio.Scanner) {
	m.scanner = scanner
}
func (m *FlatMap) setScannerKV(scannerKey, scannerValue *bufio.Scanner) {
}
func (m *FlatMap) getStats() Stats {
	return Stats{
		invoked: m.invoked,
	}
}
