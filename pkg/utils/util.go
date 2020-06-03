package utils

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/in4it/gomap/pkg/types"
	"github.com/in4it/gomap/pkg/writers"
	"github.com/vmihailenco/msgpack"
)

const UTILS_HEADERLENGTH = 8

func StringArrayToRawOutput(input []string) []types.RawOutput {
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
func PutStringRecord(str string) []byte {
	b := make([]byte, UTILS_HEADERLENGTH)
	binary.LittleEndian.PutUint32(b, uint32(len(str)))
	return append(b, str...)
}

func GetRecordLength(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func ReadRecord(input writers.Reader) (bool, []byte, error) {
	header := make([]byte, UTILS_HEADERLENGTH)
	n, err := input.Read(header)
	if err != nil {
		if err == io.EOF {
			return false, []byte{}, nil
		}
		return false, []byte{}, err
	}
	if n == 0 {
		fmt.Printf("no bytes read\n")
	}
	recordsize := GetRecordLength(header)

	outputRecord := make([]byte, recordsize)
	n, err = input.Read(outputRecord)
	if n == 0 {
		fmt.Printf("Error while reading record: no bytes read\n")
		return false, []byte{}, nil
	}
	if err != nil {
		fmt.Printf("Error while reading record: %s", err)
		return false, outputRecord, err
	}

	return true, outputRecord, nil
}
