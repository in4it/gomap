package context

import (
	"bufio"
	"os"

	"github.com/vmihailenco/msgpack"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	parquet "github.com/xitongsys/parquet-go/source"
)

type Input struct {
	currentType       string
	osFile            *os.File
	fileScanner       *bufio.Scanner
	keyScanner        *bufio.Scanner
	valueScanner      *bufio.Scanner
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
		return i.keyScanner.Scan() && i.valueScanner.Scan()
	} else if i.currentType == "value" {
		return i.valueScanner.Scan()
	}
	return false
}
func (i *Input) SetScanner(value *bufio.Scanner) {
	i.valueScanner = value
}
func (i *Input) SetScannerKV(key, value *bufio.Scanner) {
	i.keyScanner = key
	i.valueScanner = value
}
func (i *Input) Bytes() ([]byte, []byte) {
	switch i.currentType {
	case "file":
		return []byte{}, i.fileScanner.Bytes()
	case "parquet":
		/*records := reflect.ValueOf(i.fileToProcess.schema).Elem()
		records.Set(reflect.MakeSlice(records.Type(), 1, 1))
		elemType := records.Type().Elem()
		v := reflect.New(elemType)
		err := i.parquetReader.Read(v)*/
		b, err := msgpack.Marshal(&i.parquetRecord)
		if err != nil {
			panic(err)
		}
		return []byte{}, b
	case "value":
		return []byte{}, i.valueScanner.Bytes()
	}
	return i.keyScanner.Bytes(), i.valueScanner.Bytes()
}
func (i *Input) Err() (error, error) {
	if i.currentType == "file" {
		return nil, i.fileScanner.Err()
	} else if i.currentType == "value" {
		return nil, i.valueScanner.Err()
	} else if i.currentType == "parquet" {
		return nil, i.parquetErr
	} else {
		return i.keyScanner.Err(), i.valueScanner.Err()
	}
}
