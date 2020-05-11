package utils

import (
	"encoding/binary"
	"strconv"

	"github.com/in4it/gomap/pkg/types"
	"github.com/vmihailenco/msgpack"
)

const UTILS_HEADERLENGTH = 8

func StringArrayToBytes(input []string) []types.RawOutput {
	output := make([]types.RawOutput, len(input))
	for k, v := range input {
		output[k] = []byte(v)
	}
	return output
}

func RawEncode(item interface{}) []byte {
	b, err := msgpack.Marshal(&item)
	if err != nil {
		panic(err)
	}
	return b
}
func RawDecode(input []byte, item interface{}) error {
	return msgpack.Unmarshal(input, &item)
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
func Int64ToRawOutput(input int64) types.RawOutput {
	return []byte(strconv.FormatInt(input, 10))
}
func StringToRawOutput(input string) types.RawOutput {
	return []byte(input)
}
func RawInputToRawOutput(input []byte) types.RawOutput {
	return input
}
func UnmarshalRawInput(input []byte, item interface{}) error {
	return msgpack.Unmarshal(input, &item)
}

func PutRecord(data []byte) []byte {
	b := make([]byte, UTILS_HEADERLENGTH)
	binary.LittleEndian.PutUint32(b, uint32(len(data)))
	return append(b, data...)
}

func GetRecordLength(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}
