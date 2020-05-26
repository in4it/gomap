package input

import (
	"bytes"

	"github.com/in4it/gomap/pkg/utils"
)

type Value struct {
	bufferValue     *bytes.Buffer
	valueRecordSize uint32
	valueRecordErr  error
	keyRecord       []byte
	valueRecord     []byte
}

func NewValue(value *bytes.Buffer) Input {
	return &Value{bufferValue: value}
}

func (i *Value) Init() error {
	return nil
}
func (i *Value) Close() {

}
func (i *Value) Scan() bool {

	return i.readRecordFromValue()

}

func (i *Value) readRecordFromValue() bool {
	var ret bool
	var err error
	ret, i.valueRecord, err = utils.ReadRecord(i.bufferValue)
	if err != nil {
		i.valueRecordErr = err
	}
	return ret
}
func (i *Value) Bytes() ([]byte, []byte) {
	return []byte{}, i.valueRecord
}
func (i *Value) Err() (error, error) {
	return nil, i.valueRecordErr
}
func (i *Value) GetType() string {
	return "value"
}
