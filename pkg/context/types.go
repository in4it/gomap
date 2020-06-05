package context

import (
	"github.com/in4it/gomap/pkg/dataset"
	"github.com/in4it/gomap/pkg/writers"
)

type Config struct {
	bufferWriter writers.WriterReader
}

type Context struct {
	config      Config
	err         error
	input       string
	inputType   string
	inputSchema interface{}
	steps       []dataset.Step
	outputKey   writers.Reader
	outputValue writers.Reader
	outputType  string
}
