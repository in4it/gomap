package context

import (
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
	setInput(inputFile *Input)
	getOutputKV() (bytes.Buffer, bytes.Buffer)
	getOutputType() string
	getStats() StepStats
	getStepType() string
	getFunction() interface{}
}

type StepStats struct {
	invoked int
}

type fileToProcess struct {
	filename string
	fileType string
	schema   interface{}
}
