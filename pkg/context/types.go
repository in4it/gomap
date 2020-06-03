package context

import (
	"github.com/in4it/gomap/pkg/dataset"
	"github.com/in4it/gomap/pkg/writers"
)

type Context struct {
	config      string
	err         error
	input       string
	inputType   string
	inputSchema interface{}
	steps       []dataset.Step
	outputKey   writers.Reader
	outputValue writers.Reader
	outputType  string
}
