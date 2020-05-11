package context

import (
	"fmt"
	"strings"
	"testing"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

func TestRunSingleFile(t *testing.T) {
	c := New()
	output := c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
	}).Run().Get()

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
	expected := "this\nis\na\nsentence\nthis\nis\nanother\nsentence"

	for _, v1 := range strings.Split(expected, "\n") {
		found := false
		for _, v2 := range output {
			if v1 == string(v2) {
				found = true
			}

		}
		if !found {
			t.Errorf("Not found: %s", v1)
			return
		}
	}
}

type MapObject struct {
	Word      string
	WordUpper string
}

func TestMapObject(t *testing.T) {
	c := New()
	output := c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
	}).Map(func(input types.RawInput) types.RawOutput {
		rawEncode := utils.RawEncode(MapObject{Word: string(input), WordUpper: strings.ToUpper(string(input))})
		return rawEncode
	}).Run().Get()

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
	expected := "this\nis\na\nsentence\nthis\nis\nanother\nsentence"

	for _, v1 := range strings.Split(expected, "\n") {
		found := false
		for _, v2 := range output {
			var line MapObject
			err := utils.RawDecode(v2, &line)
			if err != nil {
				t.Errorf("Error: %s\n", err)
				return
			}
			if v1 == line.Word && strings.ToUpper(v1) == line.WordUpper {
				found = true
			}

		}
		if !found {
			t.Errorf("Not found: %s", v1)
			return
		}
	}
}

func TestRunSingleFileKV(t *testing.T) {
	c := New()
	keys, values := c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
	}).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		return utils.RawInputToRawOutput(input), utils.StringToRawOutput("1")
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().GetKV()

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}

	output := make(map[string]string)

	for k, key := range keys {
		output[string(key)] = string(values[k])
	}
	fmt.Printf("%+v\n", output)

	expected := map[string]string{
		"is":       "2",
		"a":        "1",
		"sentence": "2",
		"another":  "1",
		"this":     "2",
	}

	for k1, v1 := range expected {
		found := false
		for k2, v2 := range output {
			if v1 == v2 && k1 == k2 {
				found = true
			}

		}
		if !found {
			t.Errorf("Not found: %s: %s", k1, v1)
			return
		}
	}

}

func TestMultipleFiles(t *testing.T) {
	c := New()
	keys, values := c.Read("testdata/multi-file-sentences").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
	}).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		return utils.RawInputToRawOutput(input), utils.StringToRawOutput("1")
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().GetKV()
	if c.err != nil {
		t.Errorf("Error1: %s", c.err)
	}

	output := make(map[string]string)

	for k, key := range keys {
		output[string(key)] = string(values[k])
	}

	expected := map[string]string{
		"is":              "3",
		"sentence":        "3",
		"another":         "4",
		"more":            "9",
		"file":            "2",
		"sentences":       "1",
		"(sentence1.txt)": "1",
		"(sentence2.txt)": "1",
		"(sentence3.txt)": "1",
		"this":            "3",
		"a":               "1",
		"in":              "2",
	}

	for k1, v1 := range expected {
		found := false
		for k2, v2 := range output {
			if v1 == v2 && k1 == k2 {
				found = true
			}

		}
		if !found {
			t.Errorf("Not found: %s: %s", k1, v1)
			return
		}
	}
}

type ParquetLine struct {
	Word  string `parquet:"name=word, type=UTF8"`
	Count int64  `parquet:"name=count, type=INT64"`
}

func TestRunSingleParquetFile(t *testing.T) {
	c := New()

	keys, values := c.ReadParquet("testdata/words.parquet", new(ParquetLine)).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		var line ParquetLine
		err := utils.RawDecode(input, &line)
		if err != nil {
			panic(err)
		}
		return utils.StringToRawOutput(line.Word), utils.Int64ToRawOutput(line.Count)
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().GetKV()

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}

	output := make(map[string]string)

	for k, key := range keys {
		output[string(key)] = string(values[k])
	}
	expected := map[string]string{
		"is":       "10",
		"sentence": "2",
		"a":        "26",
		"this":     "6",
	}
	for k1, v1 := range expected {
		found := false
		for k2, v2 := range output {
			if v1 == v2 && k1 == k2 {
				found = true
			}

		}
		if !found {
			t.Errorf("Not found: %s: %s", k1, v1)
			return
		}
	}
}
