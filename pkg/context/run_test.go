package context

import (
	"strings"
	"testing"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

func TestRunSingleFile(t *testing.T) {
	c := New()
	output := c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToBytes(strings.Split(string(str), " "))
	}).Run().Get()

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
	expected := "this\nis\na\nsentence\nthis\nis\nanother\nsentence\n"

	if output != expected {
		t.Errorf("wrong output: %s\nexpected: %s", output, expected)
	}
}

func TestRunSingleFileKV(t *testing.T) {
	c := New()
	outputKeys, _ := c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToBytes(strings.Split(string(str), " "))
	}).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		return utils.RawInputToRawOutput(input), utils.StringToRawOutput("1")
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().GetKV()

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
	expectedKeys := "is\na\nsentence\nanother\nthis\n"

	for _, v1 := range strings.Split(expectedKeys, "\n") {
		found := false
		for _, v2 := range strings.Split(outputKeys, "\n") {
			if v1 == v2 {
				found = true
			}

		}
		if !found {
			t.Errorf("Key not found: %s", v1)
			return
		}
	}

}

func TestMultipleFiles(t *testing.T) {
	c := New()
	c.Read("testdata/multi-file-sentences").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToBytes(strings.Split(string(str), " "))
	}).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		return utils.RawInputToRawOutput(input), utils.StringToRawOutput("1")
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error1: %s", c.err)
	}
}
