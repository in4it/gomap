package dataset

import (
	"bytes"

	"github.com/in4it/gomap/pkg/input"
)

type StepStats struct {
	invoked int
}

type Step interface {
	Do(partition, totalPartitions int) error
	SetInput(inputFile input.Input)
	GetOutputKV() (bytes.Buffer, bytes.Buffer)
	GetOutputType() string
	GetStats() StepStats
	GetStepType() string
	GetFunction() interface{}
}
