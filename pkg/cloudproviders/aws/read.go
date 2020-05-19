package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	Prefix string
}

func NewS3(config S3Config) (*S3, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(config.Region)})
	if err != nil {
		readLogger.Errorf("Couldn't initialize S3: %s", err)
		return nil, nil
	}
	svc := s3.New(sess)

	// test connection
	input := &s3.GetObjectInput{
		Bucket: aws.String(config.Bucket),
		Key:    aws.String(config.Prefix + "/test-perms"),
	}
	_, err = svc.GetObject(input)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				// we have s3 permissions
			default:
				return nil, aerr
			}
		} else {
			return nil, err
		}
	}

	return &S3{config: config, svc: svc, sess: sess}, nil
}

func (s *S3) ListObjects() ([]string, error) {
	var (
		s3Objects []string
		err       error
	)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
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
func (s *S3) GetObject(filename string) ([]byte, error) {

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
