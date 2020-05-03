package context

import (
	"bytes"
	"encoding/gob"
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
