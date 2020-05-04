package context

import (
	"bufio"
	"bytes"
)

type MapToKV struct {
	function MapToKVFunction
	scanner  *bufio.Scanner
	output   bytes.Buffer
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
	for m.scanner.Scan() {
		key, value := m.function(m.scanner.Bytes())
		m.output.WriteString(string(key) + "," + string(value) + "\n")
	}

	if err := m.scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (m *MapToKV) getOutput() bytes.Buffer {
	return m.output
}

func (m *MapToKV) setScanner(scanner *bufio.Scanner) {
	m.scanner = scanner

}
