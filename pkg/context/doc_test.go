package context

import (
	"fmt"
	"os"
	"strings"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

// Example reads a text file with sentences, splits the input in words,
// converts it into a key value pair with the word as key and value "1".
// Then ReduceByKey will group the key together and take a sum of the values
// The end-result (a wordcount of the file sentences.txt) will be printed.
func Example() {
	c := New()
	c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
	}).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		return utils.RawInputToRawOutput(input), utils.StringToRawOutput("1")
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().Print()
	if c.GetError() != nil {
		fmt.Printf("Error: %s", c.GetError())
		os.Exit(1)
	}
}
