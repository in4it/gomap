package context

import (
	"bufio"
	"bytes"

	"github.com/in4it/gomap/pkg/types"
)

type MapToKV struct {
	function    types.MapToKVFunction
	scanner     *bufio.Scanner
	inputFile   *InputFile
	output      bytes.Buffer
	outputKey   bytes.Buffer
	outputValue bytes.Buffer
	outputType  string
	invoked     int
}

func (c *Context) MapToKV(fn types.MapToKVFunction) *Context {
	c.AddStep(newMapToKV(fn))
	return c
}
func newMapToKV(fn types.MapToKVFunction) *MapToKV {
	return &MapToKV{
		function: fn,
	}
}
func (m *MapToKV) do(partition, totalPartitions int) error {
	m.outputType = "kv"
	for m.inputFile.Scan() {
		m.invoked++
		_, inputValue := m.inputFile.Bytes()
		key, value := m.function(inputValue)
		m.outputKey.Write(append(key, []byte("\n")...))
		m.outputValue.Write(append(value, []byte("\n")...))
	}

	if _, err := m.inputFile.Err(); err != nil {
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

func (m *MapToKV) getStats() StepStats {
	return StepStats{
		invoked: m.invoked,
	}
}
func (m *MapToKV) getStepType() string {
	return "maptokv"
}
func (m *MapToKV) getFunction() interface{} {
	return m.function
}
func (m *MapToKV) setInputFile(inputFile *InputFile) {
	m.inputFile = inputFile
}
