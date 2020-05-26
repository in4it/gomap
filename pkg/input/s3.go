package input

import (
	"bufio"
	"fmt"

	"github.com/in4it/gomap/pkg/cloudproviders/aws"
	"github.com/in4it/gomap/pkg/utils"
)

type S3File struct {
	scanner       *bufio.Scanner
	fileToProcess FileToProcess
	key           string
	s3            *aws.S3
}

func NewS3File(fileToProcess FileToProcess) Input {
	bucket, key, err := utils.GetS3BucketNameAndKey(fileToProcess.filename)
	if err != nil {
		panic(err)
	}
	return &S3File{
		fileToProcess: fileToProcess,
		s3:            aws.NewS3(aws.S3Config{Bucket: bucket}),
		key:           key,
	}
}

func (i *S3File) Init() error {
	var err error
	i.scanner, err = i.s3.GetObjectScanner(i.key)
	if err != nil {
		return fmt.Errorf("Error while reading %s: %s", i.key, err)
	}
	return nil
}
func (i *S3File) Close() {
}
func (i *S3File) Scan() bool {
	return i.scanner.Scan()
}

func (i *S3File) Bytes() ([]byte, []byte) {
	return []byte{}, i.scanner.Bytes()
}
func (i *S3File) Err() (error, error) {
	return nil, i.scanner.Err()
}
func (i *S3File) GetType() string {
	return "s3file"
}
