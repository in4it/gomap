package context

import (
	"strings"
	"testing"
)

func TestRunSingleFile(t *testing.T) {
	c := NewContext()
	c.read("testdata/sentences.txt").Map(func(str string) []string {
		return strings.Split(str, " ")
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}

func TestMultipleFiles(t *testing.T) {
	c := NewContext()
	c.read("testdata/multi-file-sentences").Map(func(str string) []string {
		return strings.Split(str, " ")
	}).Run().Print()
	if c.err != nil {
		t.Errorf("Error: %s", c.err)
	}
}
