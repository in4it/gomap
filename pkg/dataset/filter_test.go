package dataset

import (
	"testing"

	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
	"github.com/in4it/gomap/pkg/writers"
)

func TestFilter(t *testing.T) {
	buf := writers.NewMemoryWriter()

	buf.Write(append(utils.PutStringRecord("this is a sentence"), utils.PutStringRecord("this is another sentence")...))
	inputData := input.NewValue(buf)
	m := Filter{
		Function: func(str types.RawInput) bool {
			if string(str) == "this is a sentence" {
				return true
			}
			return false
		},
		inputFile: inputData,
		output:    writers.NewMemoryWriter(),
	}
	if err := m.Do(0, 1); err != nil {
		t.Errorf("do() error: %s", err)
		return
	}
	_, output := m.GetOutputKV()

	res := ""

	for {
		moreRecords, record, err := utils.ReadRecord(output)
		if err != nil {
			panic(err)
		}
		if !moreRecords {
			break
		}
		res += string(record)
	}

	expected := "this is a sentence"

	if res != expected {
		t.Errorf("expected result is wrong: => %s\nexepcted: =>%s\n", res, expected)
	}
}
