package context

import (
	"bufio"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	parquet "github.com/xitongsys/parquet-go/source"
)

type InputFile struct {
	osFile            *os.File
	fileToProcess     fileToProcess
	parquetFileReader parquet.ParquetFile
	parquetReader     *reader.ParquetReader
}

func NewInputFile(step Step, fileToProcess fileToProcess) *InputFile {
	return &InputFile{
		fileToProcess: fileToProcess,
	}

}

func (i *InputFile) InitFile(step Step) error {
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
		step.setScanner(bufio.NewScanner(file))
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
