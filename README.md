# gomap
[![Travis Status for in4it/gomap](https://travis-ci.org/in4it/gomap.svg?branch=master)](https://travis-ci.org/in4it/gomap)
[![godoc for in4it/gomap](https://godoc.org/github.com/in4it/gomap?status.svg)](https://pkg.go.dev/github.com/in4it/gomap/pkg/context?tab=doc)

Run your MapReduce workloads as a single binary on a single machine with multiple CPUs and high memory. Pricing of a lot of small machines vs heavy machines is the same on most cloud providers.

# Usage

## Import
Context to start using gomap:
```
import "github.com/in4it/gomap/pkg/context"
```
Utils and types (for conversions):
```
import (
  "github.com/in4it/gomap/pkg/utils"
  "github.com/in4it/gomap/pkg/types"
)
```

## WordCount Example

```go
package main

import (
  "github.com/in4it/gomap/pkg/context"
  "github.com/in4it/gomap/pkg/utils"
  "github.com/in4it/gomap/pkg/types"
)

// Print a wordcount of an input file
func main() {
	c := context.New()
	err := c.Read("testdata/sentences.txt").FlatMap(func(str types.RawInput) []types.RawOutput {
		return utils.StringArrayToRawOutput(strings.Split(string(str), " "))
	}).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		return utils.RawInputToRawOutput(input), utils.StringToRawOutput("1")
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		return utils.IntToRawOutput(utils.RawInputToInt(a) + utils.RawInputToInt(b))
	}).Run().Print()
	
	if err != nil {
		fmt.Printf("Error: %s", err)
		os.Exit(1)
	}
}
```

## Parquet example
```go
package main

import (
  "github.com/in4it/gomap/pkg/context"
  "github.com/in4it/gomap/pkg/utils"
  "github.com/in4it/gomap/pkg/types"
)

// define parquet schema
type ParquetLine struct {
  Word  string `parquet:"name=word, type=UTF8"`
  Count int64  `parquet:"name=count, type=INT64"` 
}

// Print a wordcount of an input file
func main() {
	c := context.New()
	err := c.ReadParquet("s3://bucket/directory/", new(ParquetLine)).MapToKV(func(input types.RawInput) (types.RawOutput, types.RawOutput) {
		var line ParquetLine
		err := utils.RawDecode(input, &line)
		if err != nil {
			panic(err)
		}
		return utils.StringToRawOutput(line.Word), utils.RawEncode([]ParquetLine{line})
	}).ReduceByKey(func(a, b types.RawInput) types.RawOutput {
		var line1 []ParquetLine
		var line2 []ParquetLine
		err := utils.RawDecode(a, &line1)
		if err != nil {
			panic(err)
		}
		err = utils.RawDecode(b, &line2)
		if err != nil {
			panic(err)
		}
		return utils.RawEncode(append(line1, line2...))
	}).Run().Foreach(func(key, value types.RawOutput) {
		var lines []ParquetLine
		err := utils.RawDecode(value, &lines)
		if err != nil {
			panic(err)
		}
    //
    // you can now use string(key) and lines ([]ParquetLine)
    //
	})

	if err != nil {
		panic(c.err)
	}
```

## Memory usage and spill to disk
If you don't want to keep the full memory set in memory, you can specify a buffer limit. Between steps (Map, FlatMap, ReduceByKey, ...), a buffer is kept. By configuring a different writer, you can influence the memory usage.

### Default writer (MemoryWriter)
```go
	c := New()
	c.SetConfig(Config{
		bufferWriter: writers.NewMemoryWriter(),
	})
```

### Memory and Disk Writer (MemoryAndDiskWriter)
```go
	c := New()
	c.SetConfig(Config{
		// argument expects bytes. after 5 MB, the buffer will start spilling to disk. 
		bufferWriter: writers.NewMemoryAndDiskWriter(1024 /* kb */ * 1024 /* mb */ * 5), 
	})
```

## Current implemented functions
| Function | Description |
| -------- | ----------- |
| Map | Transform a value |
| FlatMap | Transform and flatten a value into a slice |
| MapToKV | Transform a map to a key value pair |
| ReduceByKey | Group unique keys and apply a reduce function |
| Foreach | Loop over the output of unique keys in a key value result |
| Print | Print output |
| Get | Get output values |
| GetKV | Get output keys and values |

## Current inputs
* Textfiles (local & S3 using s3:// prefix)
* Parquet (local & S3 using s3:// prefix)

## Concurrency
Multiple input files are split into goroutines. If you have multiple cores, the goroutines can run in parallel

# Run gomap on AWS
You can run gomap on AWS on a spot instance using the launcher.

## Configuration

Example launch specification (if the AMI is not supplied, it'll launch the latest ubuntu bionic AMI):
```
{
    "IamInstanceProfile": {
      "Arn": "arn:aws:iam::1234567890:instance-profile/gomap"
    },
    "InstanceType": "r4.large",
    "NetworkInterfaces": [
      {
        "DeviceIndex": 0,
        "Groups": ["sg-0123456789"],
        "SubnetId": "subnet-01234567890"
      }
    ]  
}
```

Note: the instance profile should have s3 & cloudwatch logs access

## Run

Download the wordcount and launch binary from the release page, and run:
```
aws s3 cp wordcount-linux-amd64 s3://yourbucket/binaries/wordcount
./launch -launchSpecification launchspec.json -region eu-west-1 -cmd "./wordcount -input s3://yourbucket/inputfile.txt" -executable s3://yourbucket/binaries/wordcount
```