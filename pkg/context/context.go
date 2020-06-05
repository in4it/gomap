package context

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/in4it/gomap/pkg/cloudproviders/aws"
	"github.com/in4it/gomap/pkg/dataset"
	"github.com/in4it/gomap/pkg/input"
	"github.com/in4it/gomap/pkg/utils"
	"github.com/in4it/gomap/pkg/writers"
)

type inputFile struct {
	name string
}

// New creates a new gomap context
// This is the first command you typically run to initialize gomap
func New() *Context {
	return &Context{}
}

// GetError will return an error if there was an error, otherwise
// it'll return nil
func (c *Context) GetError() error {
	return c.err
}

// SetConfig takes a Config type to set a different configuration
func (c *Context) SetConfig(conf Config) {
	c.config = conf
}

func (c *Context) isFileOrDirectory(name string) (bool, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return false, err
	}
	switch mode := fi.Mode(); {
	case mode.IsDir():
		return true, nil
	case mode.IsRegular():
		// do file stuff
		return false, nil
	}
	return false, fmt.Errorf("File/Dir ormat not recognized")
}

func (c *Context) getS3Files() ([]inputFile, string, string, error) {
	var (
		files []inputFile
	)
	if strings.HasSuffix(c.input, "/") {
		// s3 path is a directory
		bucket, prefix, err := utils.GetS3BucketNameAndKey(c.input)
		if err != nil {
			return []inputFile{}, "", "", err
		}
		region, err := aws.GetBucketRegion(bucket)
		if err != nil {
			return []inputFile{}, "", "", err
		}
		s3 := aws.NewS3(aws.S3Config{Region: region, Bucket: bucket})
		list, err := s3.ListObjects(prefix[1:]) // remove leading "/"
		if err != nil {
			return []inputFile{}, "", "", err
		}
		files = make([]inputFile, len(list))
		for k := range list {
			files[k].name = "s3://" + bucket + "/" + list[k]
		}
	} else {
		files = []inputFile{{name: c.input}}
	}
	switch c.inputType {
	case "file":
		return files, "", "s3file", nil
	case "parquet":
		return files, "", "parquet", nil
	default:
		panic("file type not recognized")
	}
}
func (c *Context) getLocalFiles() ([]inputFile, string, error) {
	var (
		isDirectory bool
		inputDir    string
		err         error
		fileInfo    []os.FileInfo
	)
	if isDirectory, err = c.isFileOrDirectory(c.input); err != nil {
		return []inputFile{}, inputDir, err
	}
	if isDirectory {
		inputDir = c.input
		fileInfo, err = ioutil.ReadDir(c.input)
		if err != nil {
			return []inputFile{}, inputDir, err
		}
		return toInputFile(fileInfo), inputDir, err
	}
	// not a directory
	return []inputFile{{name: c.input}}, inputDir, nil
}

func (c *Context) getFiles() ([]inputFile, string, string, interface{}, error) {
	var (
		err      error
		files    []inputFile
		inputDir string
	)

	// handle s3 files
	if len(c.input) > 5 && c.input[:5] == "s3://" {
		var inputType string
		if files, inputDir, inputType, err = c.getS3Files(); err != nil {
			return []inputFile{}, "", inputType, c.inputSchema, err
		}
		return files, inputDir, inputType, c.inputSchema, nil
	}
	// handle local files
	if files, inputDir, err = c.getLocalFiles(); err != nil {
		return []inputFile{}, "", c.inputType, c.inputSchema, err
	}
	return files, inputDir, c.inputType, c.inputSchema, nil
}

// Run executes preceding Map/MapKV/ReduceByKey/... actions
func (c *Context) Run() *RunOutput {
	var (
		runOutput         *RunOutput
		waitForContext    sync.WaitGroup
		waitForStep       sync.WaitGroup
		filenameToProcess []input.FileToProcess
	)
	// get list of files
	files, inputDir, fileType, schema, err := c.getFiles()
	if err != nil {
		return &RunOutput{err: err}
	}
	// initialize variables
	runOutput = &RunOutput{}
	runOutput.Contexts = make([]*Context, len(files))
	filenameToProcess = make([]input.FileToProcess, len(files))

	// loop over files, prepare to run different contexts in goroutines
	for k, f := range files {
		runOutput.Contexts[k] = &Context{
			steps:  copySteps(c.steps),
			input:  c.input,
			config: c.config,
		}
		if inputDir != "" {
			filenameToProcess[k] = input.NewFileToProcess(inputDir+"/"+f.name, fileType, schema)
		} else {
			filenameToProcess[k] = input.NewFileToProcess(f.name, fileType, schema)
		}
		// add waiting points, so we can sync later in the execution of the step
		for _, step := range c.steps {
			if step.GetStepType() == "reducebykey" {
				waitForStep.Add(1)
			}
		}
	}
	for k := range runOutput.Contexts {
		waitForContext.Add(1)
		go func(partition int, file input.FileToProcess) {
			runFile(partition, file, &waitForContext, &waitForStep, runOutput.Contexts)
		}(k, filenameToProcess[k])
	}
	// wait for completion of the contexts
	waitForContext.Wait()

	for _, contexts := range runOutput.Contexts {
		if contexts.err != nil {
			runOutput.err = contexts.err
			return runOutput
		}
	}

	return runOutput
}

func runFile(partition int, fileToProcess input.FileToProcess, waitForContext *sync.WaitGroup, waitForStep *sync.WaitGroup, contexts []*Context) {
	var (
		keyWriter   writers.WriterReader
		valueWriter writers.WriterReader
		err         error
		inputFile   input.Input
	)

	defer waitForContext.Done()

	inputFile = input.NewInput(fileToProcess)
	if err = inputFile.Init(); err != nil {
		contexts[partition].err = err
		return
	}

	for _, step := range contexts[partition].steps {
		step.SetInput(inputFile)
		if keyWriter, valueWriter, err = newKeyValueBufferWriter(contexts[partition].config.bufferWriter); err != nil {
			contexts[partition].err = err
			cleanupWriters(keyWriter, valueWriter)
			return
		}
		step.SetOutputKV(keyWriter, valueWriter)

		if err := step.Do(partition, len(contexts)); err != nil {
			contexts[partition].err = err
			cleanupWriters(keyWriter, valueWriter)
			return
		}
		// file can be closed now
		inputFile.Close()
		// gather input
		keyWriter, valueWriter = step.GetOutputKV()
		if step.GetStepType() == "reducebykey" {
			// make buffers visible to all contexts
			contexts[partition].outputKey = keyWriter
			contexts[partition].outputValue = valueWriter
			if err := handleReduceSync(partition, waitForStep, contexts, &inputFile, step); err != nil {
				contexts[partition].err = err
				cleanupWriters(keyWriter, valueWriter)
				return
			}
			if partition != 0 {
				return
			}
			keyWriter, valueWriter = step.GetOutputKV()
		}
		// set inputfile to new input for next step
		switch step.GetOutputType() {
		case "value":
			inputFile = input.NewValue(valueWriter)
		case "kv":
			inputFile = input.NewKeyValue(keyWriter, valueWriter)
		}
	}
	contexts[partition].outputKey = keyWriter
	contexts[partition].outputValue = valueWriter
	contexts[partition].outputType = inputFile.GetType()
	return
}

func newKeyValueBufferWriter(bufferWriterReaderFromConfig writers.WriterReader) (writers.WriterReader, writers.WriterReader, error) {
	if bufferWriterReaderFromConfig == nil {
		return writers.NewMemoryWriter(), writers.NewMemoryWriter(), nil
	}
	k, err := bufferWriterReaderFromConfig.New()
	if err != nil {
		return nil, nil, err
	}
	v, err := bufferWriterReaderFromConfig.New()
	if err != nil {
		return nil, nil, err
	}
	return k, v, nil
}

func handleReduceSync(partition int, waitForStep *sync.WaitGroup, contexts []*Context, inputFile *input.Input, step dataset.Step) error {
	waitForStep.Done()
	waitForStep.Wait()
	// now all the reducebykey steps should be finished
	if partition == 0 {
		keyReaders := make([]writers.Reader, len(contexts))
		valueReaders := make([]writers.Reader, len(contexts))
		for k := range contexts {
			keyReaders[k] = contexts[k].outputKey
			valueReaders[k] = contexts[k].outputValue
		}
		keyReader := writers.NewCombinedWriter(keyReaders)
		valueReader := writers.NewCombinedWriter(valueReaders)
		// initialize input
		input := input.NewKeyValue(keyReader, valueReader)
		step.SetInput(input)
		// initialize output
		keyWriter, valueWriter, err := newKeyValueBufferWriter(contexts[partition].config.bufferWriter)
		if err != nil {
			return err
		}
		step.SetOutputKV(keyWriter, valueWriter)
		// run reduce again
		if err := step.Do(partition, len(contexts)); err != nil {
			return err
		}
		// cleanup
		input.Close()
	}
	return nil
}

func toInputFile(fileInfo []os.FileInfo) []inputFile {
	ret := make([]inputFile, len(fileInfo))
	for k := range fileInfo {
		ret[k].name = fileInfo[k].Name()
	}
	return ret
}

func cleanupWriters(keyWriter, valueWriter writers.WriterReader) {
	if keyWriter != nil {
		keyWriter.Cleanup()
	}
	if valueWriter != nil {
		valueWriter.Cleanup()
	}
}
