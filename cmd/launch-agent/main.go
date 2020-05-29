package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/in4it/gomap/pkg/cloudproviders/aws"
	"github.com/in4it/gomap/pkg/utils"
)

func usage(err string) {
	fmt.Printf("Error: %s\n", err)
	flag.Usage()
	os.Exit(1)
}

func main() {
	var (
		s3get string
	)

	flag.StringVar(&s3get, "s3get", "", "copy s3 object to local machine")

	flag.Parse()

	if s3get == "" {
		usage("s3get url not set")
	}

	if !strings.HasPrefix(s3get, "s3://") {
		usage(fmt.Sprintf("Wrong s3 url (%s)", s3get))
	}

	bucket, prefix, err := utils.GetS3BucketNameAndKey(s3get)
	if err != nil {
		panic(err)
	}

	s3 := aws.NewS3(aws.S3Config{Bucket: bucket})

	outputFile, err := os.Create(filepath.Base(prefix))
	if err != nil {
		panic(err)
	}

	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	scanner, err := s3.GetObjectScanner(prefix)
	if err != nil {
		panic(err)
	}

	scanner.Split(bufio.ScanBytes)

	for scanner.Scan() {
		_, err := writer.Write(scanner.Bytes())
		if err != nil {
			panic(err)
		}
	}
	err = writer.Flush()
	if err != nil {
		panic(err)
	}
}
