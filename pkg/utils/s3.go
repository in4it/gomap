package utils

import (
	"fmt"
	"strings"
)

func GetS3BucketNameAndKey(filename string) (string, string, error) {
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
