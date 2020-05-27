package context

import (
	"bytes"

	"github.com/in4it/gomap/pkg/dataset"
)

type Context struct {
	config      string
	err         error
	input       string
	inputType   string
	inputSchema interface{}
	steps       []dataset.Step
	outputKey   bytes.Buffer
	outputValue bytes.Buffer
	outputType  string
}
