package context

import (
	"bufio"
	"bytes"
)

type Step interface {
	do() error
	getOutput() bytes.Buffer
	setScanner(scanner *bufio.Scanner)
	setScannerKV(scannerKey, scannerValue *bufio.Scanner)
	getOutputKV() (bytes.Buffer, bytes.Buffer)
	getOutputType() string
	getStats() Stats
}

type Context struct {
	config      string
	err         error
	input       string
	steps       []Step
	output      bytes.Buffer
	outputKey   bytes.Buffer
	outputValue bytes.Buffer
	outputType  string
}

type Stats struct {
	invoked int
}
