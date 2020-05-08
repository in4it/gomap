package context

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

func TestMap(t *testing.T) {
	var input bytes.Buffer

	input.WriteString("this is a sentence\nthis is another sentence")
	m := FlatMap{
		function: func(str types.RawInput) []types.RawOutput {
			return utils.StringArrayToBytes(strings.Split(string(str), " "))

		},
		scanner: bufio.NewScanner(&input),
	}
	if err := m.do(0, 1); err != nil {
		t.Errorf("do() error: %s", err)
		return
	}
	output := m.getOutput()

	scanner := bufio.NewScanner(&output)
	res := ""
	for scanner.Scan() {
		res += scanner.Text() + "\n"
	}

	expected := "this\nis\na\nsentence\nthis\nis\nanother\nsentence\n"

	if res != expected {
		t.Errorf("expected result is wrong: %s\nexepcted:%s\n", res, expected)
	}
}
