package context

import (
	"bufio"
	"bytes"
)

type RawInput []byte
type RawOutput []byte

type FlatMapFunction func(RawInput) []RawOutput
type MapFunction func(RawInput) RawOutput
type MapToKVFunction func(RawInput) (RawOutput, RawOutput)
type ReduceByKeyFunction func(RawInput, RawInput) RawOutput

type Step interface {
	do() error
	getOutput() bytes.Buffer
	setScanner(scanner *bufio.Scanner)
	setScannerKV(scannerKey, scannerValue *bufio.Scanner)
	getOutputKV() (bytes.Buffer, bytes.Buffer)
	getOutputType() string
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
