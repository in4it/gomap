package context

import (
	"bytes"
	"encoding/gob"
	"strconv"
)

func StringArrayToBytes(input []string) []RawOutput {
	output := make([]RawOutput, len(input))
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

func RawInputToInt(input RawInput) int {
	res, err := strconv.Atoi(string(input))
	if err != nil {
		panic(err)
	}
	return res
}
func IntToRawOutput(input int) RawOutput {
	return []byte(strconv.Itoa(input))
}
func StringToRawOutput(input string) RawOutput {
	return []byte(input)
}
func RawInputToRawOutput(input []byte) RawOutput {
	return input
}
