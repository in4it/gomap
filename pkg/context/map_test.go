package context

import (
	"bytes"
	"strings"
	"testing"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

func TestMap(t *testing.T) {
	var input bytes.Buffer

	input.Write(append(utils.PutStringRecord("this is a sentence"), utils.PutStringRecord("this is another sentence")...))
	inputFile := Input{
		currentType: "value",
		bufferValue: &input,
	}
	m := FlatMap{
		function: func(str types.RawInput) []types.RawOutput {
			return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
		},
		inputFile: &inputFile,
	}
	if err := m.do(0, 1); err != nil {
		t.Errorf("do() error: %s", err)
		return
	}
	_, output := m.getOutputKV()

	res := ""

	for {
		moreRecords, record, err := utils.ReadRecord(&output)
		if err != nil {
			panic(err)
		}
		if !moreRecords {
			break
		}
		res += string(record) + "\n"
	}

	expected := "this\nis\na\nsentence\nthis\nis\nanother\nsentence\n"

	if res != expected {
		t.Errorf("expected result is wrong: => %s\nexepcted: =>%s\n", res, expected)
	}
}
