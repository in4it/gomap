package dataset

import (
	"testing"

	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
	"github.com/in4it/gomap/pkg/writers"
)

func TestReduceByKey(t *testing.T) {
	keyBuf := writers.NewMemoryWriter()
	valueBuf := writers.NewMemoryWriter()

	inputKeys := []string{"this", "this", "sentence", "sentence"}
	inputValues := []string{"1", "2", "3", "4"}

	for _, v := range inputKeys {
		keyBuf.Write(utils.PutStringRecord(v))
	}
	for _, v := range inputValues {
		valueBuf.Write(utils.PutStringRecord(v))
	}

	inputFile := input.NewKeyValue(keyBuf, valueBuf)
	m := ReduceByKey{
		Function: func(a, b types.RawInput) types.RawOutput {
			return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
		},
		inputFile:   inputFile,
		outputKey:   writers.NewMemoryWriter(),
		outputValue: writers.NewMemoryWriter(),
	}
	if err := m.Do(0, 1); err != nil {
		t.Errorf("do() error: %s", err)
		return
	}
	key, values := m.GetOutputKV()

	res := ""
	resValues := ""

	for {
		moreRecords, record, err := utils.ReadRecord(key)
		if err != nil {
			panic(err)
		}
		if !moreRecords {
			break
		}
		res += string(record) + "\n"
	}
	for {
		moreRecords, record, err := utils.ReadRecord(values)
		if err != nil {
			panic(err)
		}
		if !moreRecords {
			break
		}
		resValues += string(record) + "\n"
	}

	expectedKeys := "this\nsentence\n"
	expectedValues := "3\n7\n"

	if res != expectedKeys {
		t.Errorf("expected result key is wrong: => %s\nexepcted: =>%s\n", res, expectedKeys)
	}
	if resValues != expectedValues {
		t.Errorf("expected result value is wrong: => %s\nexepcted: =>%s\n", res, expectedValues)
	}
}
