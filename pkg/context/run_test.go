package context

import (
	"strings"
	"testing"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

func TestRunSingleFile(t *testing.T) {
	c := New()
	c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToBytes(strings.Split(string(str), " "))
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}

func TestRunSingleFileKV(t *testing.T) {
	c := New()
	c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToBytes(strings.Split(string(str), " "))
	}).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		return utils.RawInputToRawOutput(input), utils.StringToRawOutput("1")
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}

func TestMultipleFiles(t *testing.T) {
	c := New()
	c.Read("testdata/multi-file-sentences").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToBytes(strings.Split(string(str), " "))
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}
