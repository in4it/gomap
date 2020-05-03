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
	}).Map(func(str RawInput) RawOutput {
		kv := KeyValueStringInt{
			Key:   string(str),
			Value: 1,
		}
		return RawEncode(kv)
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
