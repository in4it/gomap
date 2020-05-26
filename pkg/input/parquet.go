package input

import (
	"github.com/vmihailenco/msgpack"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	parquet "github.com/xitongsys/parquet-go/source"
)

type ParquetFile struct {
	fileToProcess     FileToProcess
	parquetFileReader parquet.ParquetFile
	parquetReader     *reader.ParquetReader
	parquetRecord     interface{}
	parquetErr        error
}

func NewParquetFile(fileToProcess FileToProcess) Input {
	return &ParquetFile{
		fileToProcess: fileToProcess,
	}
}

func (i *ParquetFile) Init() error {
	var err error
	i.parquetFileReader, err = local.NewLocalFileReader(i.fileToProcess.filename)
	if err != nil {
		return err
	}
	i.parquetReader, err = reader.NewParquetReader(i.parquetFileReader, i.fileToProcess.schema, 4)
	if err != nil {
		return err
	}

	return nil
}
func (i *ParquetFile) Close() {
	i.parquetReader.ReadStop()
	i.parquetFileReader.Close()
}
func (i *ParquetFile) Scan() bool {
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
}

func (i *ParquetFile) Bytes() ([]byte, []byte) {
	b, err := msgpack.Marshal(&i.parquetRecord)
	if err != nil {
		panic(err)
	}
	return []byte{}, b
}
func (i *ParquetFile) Err() (error, error) {
	return nil, i.parquetErr
}
func (i *ParquetFile) GetType() string {
	return "parquet"
}
