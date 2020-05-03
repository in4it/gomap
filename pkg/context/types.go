package context

import (
	"bufio"
	"bytes"
)

type RawInput []byte
type RawOutput []byte

type KeyValueStringInt struct {
	Key   string
	Value int
}

type FlatMapFunction func(RawInput) []RawOutput
type MapFunction func(RawInput) RawOutput

type Step interface {
	do() error
	getOutput() bytes.Buffer
	setScanner(scanner *bufio.Scanner)
}

type Context struct {
	config string
	err    error
	input  string
	steps  []Step
	output bytes.Buffer
}
