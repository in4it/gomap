package context

import (
	"bufio"
	"bytes"
	"fmt"
	"os"

	"github.com/in4it/gomap/pkg/utils"
	"github.com/vmihailenco/msgpack"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	parquet "github.com/xitongsys/parquet-go/source"
)

type Input struct {
	currentType       string
	osFile            *os.File
	fileScanner       *bufio.Scanner
	bufferKey         *bytes.Buffer
	bufferValue       *bytes.Buffer
	keyRecordSize     uint32
	keyRecordErr      error
	valueRecordSize   uint32
	valueRecordErr    error
	keyRecord         []byte
	valueRecord       []byte
	fileToProcess     fileToProcess
	parquetFileReader parquet.ParquetFile
	parquetReader     *reader.ParquetReader
	parquetRecord     interface{}
	parquetErr        error
}

func NewInput(fileToProcess fileToProcess) *Input {
	return &Input{
		fileToProcess: fileToProcess,
	}
}

func (i *Input) InitFile() error {
	if i.fileToProcess.fileType == "parquet" {
		var err error
		i.parquetFileReader, err = local.NewLocalFileReader(i.fileToProcess.filename)
		if err != nil {
			return err
		}
		i.parquetReader, err = reader.NewParquetReader(i.parquetFileReader, i.fileToProcess.schema, 4)
		if err != nil {
			return err
		}
		i.currentType = "parquet"
	} else {
		file, err := os.Open(i.fileToProcess.filename)
		if err != nil {
			return err
		}
		i.fileScanner = bufio.NewScanner(file)
		i.osFile = file
		i.currentType = "file"
	}
	return nil
}
func (i *Input) Close() {
	switch i.fileToProcess.fileType {
	case "parquet":
		i.parquetReader.ReadStop()
		i.parquetFileReader.Close()
	case "file":
		i.osFile.Close()
	}
}
func (i *Input) Scan() bool {
	if i.currentType == "file" {
		return i.fileScanner.Scan()
	} else if i.currentType == "parquet" {
		record, err := i.parquetReader.ReadByNumber(1)
		if err != nil {
			i.parquetErr = err
			return false
		}
		if len(record) == 1 {
			i.parquetRecord = record[0]
			return true
		}
		return false
	} else if i.currentType == "kv" {
		return i.readRecordFromKey() && i.readRecordFromValue()
	} else if i.currentType == "value" {
		return i.readRecordFromValue()
	}
	return false
}
func (i *Input) readRecordFromKey() bool {
	var ret bool
	var err error
	ret, i.keyRecord, err = utils.ReadRecord(i.bufferKey)
	if err != nil {
		i.keyRecordErr = err
	}
	return ret
}
func (i *Input) readRecordFromValue() bool {
	var ret bool
	var err error
	ret, i.valueRecord, err = utils.ReadRecord(i.bufferValue)
	if err != nil {
		i.valueRecordErr = err
	}
	return ret
}
func (i *Input) SetBufferKey(key *bytes.Buffer) {
	i.bufferKey = key
}
func (i *Input) SetBufferValue(value *bytes.Buffer) {
	i.bufferValue = value
}

func (i *Input) Bytes() ([]byte, []byte) {
	switch i.currentType {
	case "file":
		return []byte{}, i.fileScanner.Bytes()
	case "parquet":
		b, err := msgpack.Marshal(&i.parquetRecord)
		if err != nil {
			panic(err)
		}
		return []byte{}, b
	case "value":
		return []byte{}, i.valueRecord
	case "kv":
		return i.keyRecord, i.valueRecord
	}
	return []byte{}, []byte{}
}
func (i *Input) Err() (error, error) {
	if i.currentType == "file" {
		return nil, i.fileScanner.Err()
	} else if i.currentType == "value" {
		return nil, i.valueRecordErr
	} else if i.currentType == "parquet" {
		return nil, i.parquetErr
	} else if i.currentType == "kv" {
		return i.keyRecordErr, i.valueRecordErr
	}
	return fmt.Errorf("output type not recognized"), fmt.Errorf("output type not recognized")
}
