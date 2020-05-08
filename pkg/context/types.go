package context

import (
	"bufio"
	"bytes"
)

type Context struct {
	config      string
	err         error
	input       string
	inputType   string
	inputSchema interface{}
	steps       []Step
	output      bytes.Buffer
	outputKey   bytes.Buffer
	outputValue bytes.Buffer
	outputType  string
}

type Step interface {
	do(partition, totalPartitions int) error
	getOutput() bytes.Buffer
	setScanner(scanner *bufio.Scanner)
	setScannerKV(scannerKey, scannerValue *bufio.Scanner)
	getOutputKV() (bytes.Buffer, bytes.Buffer)
	getOutputType() string
	getStats() StepStats
	getStepType() string
	getFunction() interface{}
}

type StepStats struct {
	invoked int
}
