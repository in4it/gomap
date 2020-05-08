package utils

import (
	"bytes"
	"encoding/gob"
	"strconv"

	"github.com/in4it/gomap/pkg/types"
)

func StringArrayToBytes(input []string) []types.RawOutput {
	output := make([]types.RawOutput, len(input))
	for k, v := range input {
		output[k] = []byte(v)
	}
	return output
}

func RawEncode(input interface{}) []byte {
	var ret bytes.Buffer
	enc := gob.NewEncoder(&ret)
	enc.Encode(input)
	return ret.Bytes()
}

func RawInputToInt(input types.RawInput) int {
	res, err := strconv.Atoi(string(input))
	if err != nil {
		panic(err)
	}
	return res
}
func IntToRawOutput(input int) types.RawOutput {
	return []byte(strconv.Itoa(input))
}
func StringToRawOutput(input string) types.RawOutput {
	return []byte(input)
}
func RawInputToRawOutput(input []byte) types.RawOutput {
	return input
}
