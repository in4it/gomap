package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	//"net/http"
	//_ "net/http/pprof"

	"github.com/in4it/gomap/pkg/context"
	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/utils"
)

func main() {
	var (
		input string
		debug bool
	)

	flag.StringVar(&input, "input", "", "input file name")
	flag.BoolVar(&debug, "debug", false, "debug on or off")

	flag.Parse()

	if debug {
		//http.ListenAndServe("localhost:8080", nil)
	}

	if !isS3Prefix(input) && !fileExists(input) {
		fmt.Printf("Input file doesn't exist: %s\n", input)
		os.Exit(1)
	}

	c := context.New()
	c.Read(input).FlatMap(func(str types.RawInput) []types.RawOutput {
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
func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func isS3Prefix(input string) bool {
	if len(input) >= 5 && input[:5] == "s3://" {
		return true
	}
	return false
}
