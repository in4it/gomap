package context

import (
	"bufio"
	"bytes"
)

type FlatMap struct {
	function FlatMapFunction
	scanner  *bufio.Scanner
	output   bytes.Buffer
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

func (m *FlatMap) setScanner(scanner *bufio.Scanner) {
	m.scanner = scanner

}
