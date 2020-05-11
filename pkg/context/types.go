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
	outputKey   bytes.Buffer
	outputValue bytes.Buffer
	outputType  string
}

type Step interface {
	do(partition, totalPartitions int) error
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
