package input

import (
	"bufio"
	"os"
)

type LocalFile struct {
	osFile        *os.File
	fileScanner   *bufio.Scanner
	fileToProcess FileToProcess
}

func NewLocalFile(fileToProcess FileToProcess) Input {
	return &LocalFile{
		fileToProcess: fileToProcess,
	}
}

func (i *LocalFile) Init() error {
	file, err := os.Open(i.fileToProcess.filename)
	if err != nil {
		return err
	}
	i.fileScanner = bufio.NewScanner(file)
	i.osFile = file
	return nil
}
func (i *LocalFile) Close() {
	i.osFile.Close()
}
func (i *LocalFile) Scan() bool {
	return i.fileScanner.Scan()
}

func (i *LocalFile) Bytes() ([]byte, []byte) {
	return []byte{}, i.fileScanner.Bytes()
}
func (i *LocalFile) Err() (error, error) {
	return nil, i.fileScanner.Err()
}
func (i *LocalFile) GetType() string {
	return "file"
}
