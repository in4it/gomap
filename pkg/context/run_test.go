package context

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
	"github.com/in4it/gomap/pkg/writers"
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
		return
	}

	output := make(map[string]string)

	for k, key := range keys {
		output[string(key)] = string(values[k])
	}

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

func TestRunParquetFile(t *testing.T) {
	expected := []map[string]string{}

	// first scenario
	scenarios := []string{"testdata/words.parquet"}
	expected = append(expected, map[string]string{
		"is":       "10",
		"sentence": "2",
		"a":        "26",
		"this":     "6",
	})
	// second scenario
	if os.Getenv("S3_TESTFILE_PARQUET") != "" {
		fmt.Printf("S3_TESTFILE_PARQUET found, including s3 parquet test\n")
		scenarios = append(scenarios, os.Getenv("S3_TESTFILE_PARQUET"))
		expected = append(expected, map[string]string{
			"is":       "10",
			"sentence": "2",
			"a":        "26",
			"this":     "6",
		})
	}
	// third scenario
	if os.Getenv("S3_TESTDIR_PARQUET") != "" {
		fmt.Printf("S3_TESTDIR_PARQUET found, including s3 parquet directory test\n")
		scenarios = append(scenarios, os.Getenv("S3_TESTDIR_PARQUET"))
		expected = append(expected, map[string]string{
			"is":       "20",
			"sentence": "4",
			"a":        "52",
			"this":     "12",
		})
	}

	for scenario, inputFile := range scenarios {
		c := New()
		keys, values := c.ReadParquet(inputFile, new(ParquetLine)).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
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

		for k1, v1 := range expected[scenario] {
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
}

func TestS3Input(t *testing.T) {
	expected := map[string]string{
		"is":       "2",
		"a":        "1",
		"sentence": "2",
		"another":  "1",
		"this":     "2",
	}

	// skip test if s3 testfile is not set
	if os.Getenv("S3_TESTFILE") == "" {
		t.Skip()
		return
	}
	c := New()
	c.Read(os.Getenv("S3_TESTFILE")).FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
	}).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		return utils.RawInputToRawOutput(input), utils.StringToRawOutput("1")
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().Foreach(func(key, value types.RawOutput) {
		found := false
		for k1, v1 := range expected {
			if k1 == string(key) && v1 == string(value) {
				found = true
			}
		}
		if !found {
			t.Errorf("Not found: %s: %s", string(key), string(value))
			return
		}
	})

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}

}

func TestParquetPartition(t *testing.T) {
	if os.Getenv("S3_TESTDIR_PARQUET") == "" {
		t.Skip()
	}

	found := false

	c := New()
	c.ReadParquet(os.Getenv("S3_TESTDIR_PARQUET"), new(ParquetLine)).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		var line ParquetLine
		err := utils.RawDecode(input, &line)
		if err != nil {
			panic(err)
		}
		return utils.StringToRawOutput(line.Word), utils.RawEncode([]ParquetLine{line})
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		var line1 []ParquetLine
		var line2 []ParquetLine
		err := utils.RawDecode(a, &line1)
		if err != nil {
			panic(err)
		}
		err = utils.RawDecode(b, &line2)
		if err != nil {
			panic(err)
		}
		return utils.RawEncode(append(line1, line2...))
	}).Run().Foreach(func(key, value types.RawOutput) {
		var lines []ParquetLine
		err := utils.RawDecode(value, &lines)
		if err != nil {
			panic(err)
		}
		// TODO: check all instead of one element
		if string(key) == "this" && len(lines) == 8 {
			found = true
		}

	})

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
		return
	}

	if !found {
		t.Errorf("Expected element not found")
	}
}

func TestFilter(t *testing.T) {
	c := New()
	c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
	}).Filter(func(input types.RawInput) bool {
		if string(input) == "this" {
			return true
		}
		return false
	}).Run().Foreach(func(key, value types.RawOutput) {
		found := false
		if string(value) == "this" {
			found = true
		}
		if !found {
			t.Errorf("Not found: %s: %s", string(key), string(value))
			return
		}
	})

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}

}

func TestSingleFileSpillToDisk(t *testing.T) {

	expected := map[string]string{
		"is":       "2",
		"a":        "1",
		"sentence": "2",
		"another":  "1",
		"this":     "2",
	}

	c := New()
	writer, err := writers.NewMemoryAndDiskWriter(10)
	if err != nil {
		t.Errorf("Couldn't initialize new memory and disk writer: %s", err)
	}
	c.SetConfig(Config{
		bufferWriter: writer,
	})
	fmt.Printf("Writing to tmp dir: %s\n", os.TempDir())
	c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
	}).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		return utils.RawInputToRawOutput(input), utils.StringToRawOutput("1")
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().Foreach(func(key, value types.RawOutput) {
		found := false
		for k1, v1 := range expected {
			if k1 == string(key) && v1 == string(value) {
				found = true
			}
		}
		if !found {
			t.Errorf("Not found: %s: %s", string(key), string(value))
			return
		}
	})

	if c.err != nil {
		t.Errorf("Error: %s", c.err)
		return
	}
}
