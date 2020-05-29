package utils

import (
	"testing"
)

func TestGetS3BucketNameAndKey(t *testing.T) {
	bucket, prefix, err := GetS3BucketNameAndKey("s3://bucketname/path/to/object")
	if bucket != "bucketname" {
		t.Errorf("invalid bucketname")
		return
	}
	if prefix != "/path/to/object" {
		t.Errorf("invalid prefix")
		return

	}
	if err != nil {
		t.Errorf("Error while getting bucket name: %s", err)
		return
	}
}
