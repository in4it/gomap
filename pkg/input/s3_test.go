package input

import (
	"os"
	"testing"
)

func TestS3Get(t *testing.T) {
	// skip test if s3 testfile is not set
	if os.Getenv("S3_TESTFILE") == "" {
		t.Skip()
		return
	}
	// unset aws region to make sure we can obtain the region ourselves
	os.Setenv("AWS_REGION", "")
	s3 := NewS3File(FileToProcess{
		filename: os.Getenv("S3_TESTFILE"),
	})
	err := s3.Init()
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}
	result := ""
	for {
		if !s3.Scan() {
			break
		}
		_, v := s3.Bytes()
		result += string(v)
	}
	if result != "this is a sentencethis is another sentence" {
		t.Errorf("Error: unexpected output: %s", result)
		return
	}
}
