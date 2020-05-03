package context

import (
	"bufio"
	"bytes"
)

type MapFunction func(string) []string

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
