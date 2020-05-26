package input

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/in4it/gomap/pkg/cloudproviders/aws"
)

type S3File struct {
	scanner       *bufio.Scanner
	fileToProcess FileToProcess
	key           string
	s3            *aws.S3
}

func NewS3File(fileToProcess FileToProcess) Input {
	bucket, key, err := getS3BucketNameAndKey(fileToProcess.filename)
	if err != nil {
		panic(err)
	}
	return &S3File{
		fileToProcess: fileToProcess,
		s3:            aws.NewS3(aws.S3Config{Bucket: bucket}),
		key:           key,
	}
}

func getS3BucketNameAndKey(filename string) (string, string, error) {
	if len(filename) < 6 || filename[:5] != "s3://" {
		return "", "", fmt.Errorf("Invalid s3 URL: %s", filename)
	}
	pos := strings.IndexRune(filename[5:], '/')
	if pos == -1 {
		return "", "", fmt.Errorf("Invalid s3 URL: %s", filename)
	}
	bucketName := filename[5 : 5+pos]
	return bucketName, filename[len(bucketName)+5:], nil
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
