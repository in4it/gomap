package context

import (
	"bufio"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	parquet "github.com/xitongsys/parquet-go/source"
)

type InputFile struct {
	currentType       string
	osFile            *os.File
	fileScanner       *bufio.Scanner
	keyScanner        *bufio.Scanner
	valueScanner      *bufio.Scanner
	fileToProcess     fileToProcess
	parquetFileReader parquet.ParquetFile
	parquetReader     *reader.ParquetReader
}

func NewInputFile(fileToProcess fileToProcess) *InputFile {
	return &InputFile{
		fileToProcess: fileToProcess,
	}
}

func (i *InputFile) InitFile() error {
	if i.fileToProcess.fileType == "parquet" {
		var err error
		i.parquetFileReader, err = local.NewLocalFileReader("output/flat.parquet")
		if err != nil {
			return err
		}
		i.parquetReader, err = reader.NewParquetReader(i.parquetFileReader, i.fileToProcess.schema, 4)
		if err != nil {
			return err
		}
	} else {
		file, err := os.Open(i.fileToProcess.filename)
		if err != nil {
			return err
		}
		i.fileScanner = bufio.NewScanner(file)
		i.osFile = file
	}
	return nil
}
func (i *InputFile) Close() {
	if i.fileToProcess.fileType == "parquet" {
		i.parquetReader.ReadStop()
		i.parquetFileReader.Close()
	} else {
		i.osFile.Close()
	}
}
func (i *InputFile) Scan() bool {
	if i.currentType == "file" {
		return i.fileScanner.Scan()
	} else if i.currentType == "maptokv" {
		return i.keyScanner.Scan() && i.valueScanner.Scan()
	} else if i.currentType == "reducebykey" {
		return i.keyScanner.Scan() && i.valueScanner.Scan()
	} else if i.currentType == "value" {
		return i.valueScanner.Scan()
	}
	return false
}
func (i *InputFile) SetScanner(value *bufio.Scanner) {
	i.valueScanner = value
}
func (i *InputFile) SetScannerKV(key, value *bufio.Scanner) {
	i.keyScanner = key
	i.valueScanner = value
}
func (i *InputFile) Bytes() ([]byte, []byte) {
	if i.currentType == "file" {
		return []byte{}, i.fileScanner.Bytes()
	} else if i.currentType == "value" {
		return []byte{}, i.valueScanner.Bytes()
	} else {
		return i.keyScanner.Bytes(), i.valueScanner.Bytes()
	}
}
func (i *InputFile) Err() (error, error) {
	if i.currentType == "file" {
		return nil, i.fileScanner.Err()
	} else if i.currentType == "value" {
		return nil, i.valueScanner.Err()
	} else {
		return i.keyScanner.Err(), i.valueScanner.Err()
	}
}
