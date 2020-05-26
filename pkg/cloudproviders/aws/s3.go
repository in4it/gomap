package aws

import (
	"bufio"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/juju/loggo"
)

var (
	readLogger = loggo.GetLogger("storage.s3")
)

type S3 struct {
	config S3Config
	svc    *s3.S3
	sess   *session.Session
}

type S3Config struct {
	Region string
	Bucket string
}

func NewS3(config S3Config) *S3 {
	region, err := GetBucketRegion(config.Bucket)
	if err != nil {
		readLogger.Errorf("getBucketRegion: %s", err)
	}

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		readLogger.Errorf("Couldn't initialize S3: %s", err)

	}
	svc := s3.New(sess)

	return &S3{config: config, svc: svc, sess: sess}
}

func (s *S3) ListObjects(prefix string) ([]string, error) {
	var (
		s3Objects []string
		err       error
	)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	}
	pageNum := 0
	err = s.svc.ListObjectsV2Pages(input,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			pageNum++
			for _, item := range page.Contents {

				s3Objects = append(s3Objects, aws.StringValue(item.Key))

			}
			return pageNum <= 1000
		})

	if err != nil {
		return s3Objects, err
	}
	return s3Objects, nil
}
func (s *S3) RetrieveObject(filename string) ([]byte, error) {
	contents := aws.NewWriteAtBuffer([]byte{})
	downloader := s3manager.NewDownloader(s.sess)
	readLogger.Debugf("GetObject: %s", filename)
	_, err := downloader.Download(contents,
		&s3.GetObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(filename),
		})
	if err != nil {
		return []byte{}, err
	}
	return contents.Bytes(), nil
}
func (s *S3) GetObjectScanner(key string) (*bufio.Scanner, error) {
	req, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return bufio.NewScanner(req.Body), nil
}
func GetBucketRegion(bucketname string) (string, error) {
	var (
		res *http.Response
		err error
	)

	url := fmt.Sprintf("https://%s.s3.amazonaws.com", bucketname)
	if res, err = http.Head(url); err != nil {
		return "", err
	}

	return res.Header.Get("X-Amz-Bucket-Region"), nil
}

func GetAWSConfigForBucket(bucketname string) (*aws.Config, error) {
	region, err := GetBucketRegion(bucketname)
	if err != nil {
		return nil, err
	}
	return &aws.Config{Region: aws.String(region)}, nil
}
