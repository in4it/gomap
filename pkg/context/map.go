package context

import (
	"bufio"
	"bytes"
)

type Map struct {
	function MapFunction
	scanner  *bufio.Scanner
	output   bytes.Buffer
}

func (c *Context) Map(fn MapFunction) *Context {
	c.AddStep(newMap(fn))
	return c
}
func newMap(fn MapFunction) *Map {
	return &Map{
		function: fn,
	}
}
func (m *Map) do() error {
	for m.scanner.Scan() {
		for _, output := range m.function(m.scanner.Text()) {
			m.output.WriteString(output + "\n")
		}
	}

	if err := m.scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (m *Map) getOutput() bytes.Buffer {
	return m.output
}

func (m *Map) setScanner(scanner *bufio.Scanner) {
	m.scanner = scanner

}
