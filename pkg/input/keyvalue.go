package input

import (
	"github.com/in4it/gomap/pkg/utils"
	"github.com/in4it/gomap/pkg/writers"
)

type KeyValue struct {
	bufferKey       writers.WriterReader
	bufferValue     writers.WriterReader
	keyRecordSize   uint32
	keyRecordErr    error
	valueRecordSize uint32
	valueRecordErr  error
	keyRecord       []byte
	valueRecord     []byte
}

func NewKeyValue(key, value writers.WriterReader) Input {
	return &KeyValue{
		bufferKey:   key,
		bufferValue: value,
	}
}

func (i *KeyValue) Init() error {
	return nil
}
func (i *KeyValue) Close() {
}
func (i *KeyValue) Scan() bool {
	return i.readRecordFromKey() && i.readRecordFromValue()

}

func (i *KeyValue) Bytes() ([]byte, []byte) {
	return i.keyRecord, i.valueRecord
}
func (i *KeyValue) Err() (error, error) {
	return i.keyRecordErr, i.valueRecordErr
}
func (i *KeyValue) readRecordFromKey() bool {
	var ret bool
	var err error
	ret, i.keyRecord, err = utils.ReadRecord(i.bufferKey)
	if err != nil {
		i.keyRecordErr = err
	}
	return ret
}
func (i *KeyValue) readRecordFromValue() bool {
	var ret bool
	var err error
	ret, i.valueRecord, err = utils.ReadRecord(i.bufferValue)
	if err != nil {
		i.valueRecordErr = err
	}
	return ret
}

func (i *KeyValue) GetType() string {
	return "kv"
}
