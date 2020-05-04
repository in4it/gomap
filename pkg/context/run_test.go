package context

import (
	"strings"
	"testing"
)

func TestRunSingleFile(t *testing.T) {
	c := NewContext()
	c.read("testdata/sentences.txt").FlatMap(func(str RawInput) []RawOutput {
		return StringArrayToBytes(strings.Split(string(str), " "))
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}

func TestRunSingleFileKV(t *testing.T) {
	c := NewContext()
	c.read("testdata/sentences.txt").FlatMap(func(str RawInput) []RawOutput {
		return StringArrayToBytes(strings.Split(string(str), " "))
	}).MapToKV(func(str RawInput) (RawOutput, RawOutput) {
		return []byte(str), []byte("1")
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}

func TestMultipleFiles(t *testing.T) {
	c := NewContext()
	c.read("testdata/multi-file-sentences").FlatMap(func(str RawInput) []RawOutput {
		return StringArrayToBytes(strings.Split(string(str), " "))
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}
