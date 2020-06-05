package dataset

import (
	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/writers"
)

type StepStats struct {
	invoked int
}

type Step interface {
	Do(partition, totalPartitions int) error
	SetInput(inputFile input.Input)
	SetOutputKV(keyWriter writers.WriterReader, valueWriter writers.WriterReader)
	GetOutputKV() (writers.WriterReader, writers.WriterReader)
	GetOutputType() string
	GetStats() StepStats
	GetStepType() string
	GetFunction() interface{}
}
