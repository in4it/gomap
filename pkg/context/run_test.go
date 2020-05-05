package context

import (
	"strings"
	"testing"
)

func TestRunSingleFile(t *testing.T) {
	c := New()
	c.Read("testdata/sentences.txt").FlatMap(func(str RawInput) []RawOutput {
		return StringArrayToBytes(strings.Split(string(str), " "))
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}

func TestRunSingleFileKV(t *testing.T) {
	c := New()
	c.Read("testdata/sentences.txt").FlatMap(func(str RawInput) []RawOutput {
		return StringArrayToBytes(strings.Split(string(str), " "))
	}).MapToKV(func(input RawInput) (RawOutput, RawOutput) {
		return RawInputToRawOutput(input), StringToRawOutput("1")
	}).ReduceByKey(func(a, b RawInput) RawOutput {
		return IntToRawOutput(RawInputToInt(a) + RawInputToInt(b))
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}

func TestMultipleFiles(t *testing.T) {
	c := New()
	c.Read("testdata/multi-file-sentences").FlatMap(func(str RawInput) []RawOutput {
		return StringArrayToBytes(strings.Split(string(str), " "))
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}
