package context

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestMap(t *testing.T) {
	var input bytes.Buffer

	input.WriteString("this is a sentence\nthis is another sentence")
	m := Map{
		function: func(str string) []string {
			return strings.Split(str, " ")
		},
		scanner: bufio.NewScanner(&input),
	}
	if err := m.do(); err != nil {
		t.Errorf("do() error: %s", err)
		return
	}
	output := m.getOutput()

	scanner := bufio.NewScanner(&output)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}

}
