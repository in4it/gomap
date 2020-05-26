package context

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/in4it/gomap/pkg/input"
)

type InputFile struct {
	name string
}

func New() *Context {
	return &Context{}
}

func (c *Context) GetError() error {
	return c.err
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

func (c *Context) getFiles() ([]InputFile, string, string, interface{}, error) {
	var (
		isDirectory bool
		err         error
		files       []InputFile
		fileInfo    []os.FileInfo
		inputDir    string
	)

	// is file s3 file
	if len(c.input) > 5 && c.input[:5] == "s3://" {
		return []InputFile{{name: c.input}}, "", "s3file", c.inputSchema, nil
	}

	if isDirectory, err = c.isFileOrDirectory(c.input); err != nil {
		return files, inputDir, c.inputType, c.inputSchema, err
	}
	if isDirectory {
		inputDir = c.input
		fileInfo, err = ioutil.ReadDir(c.input)
		if err != nil {
			return toInputFile(fileInfo), inputDir, c.inputType, c.inputSchema, err
		}
	} else {
		inputDir = filepath.Dir(c.input)
		fstat, err := os.Stat(c.input)
		if err != nil {
			return files, inputDir, c.inputType, c.inputSchema, err
		}
		files = toInputFile([]os.FileInfo{fstat})
	}
	return files, inputDir, c.inputType, c.inputSchema, nil
}

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
		c.err = err
		return runOutput
	}
	// initialize variables
	runOutput = &RunOutput{}
	runOutput.Contexts = make([]*Context, len(files))
	filenameToProcess = make([]input.FileToProcess, len(files))

	// loop over files, prepare to run different contexts in goroutines
	for k, f := range files {
		runOutput.Contexts[k] = &Context{
			steps: copySteps(c.steps),
			input: c.input,
		}
		if inputDir != "" {
			filenameToProcess[k] = input.NewFileToProcess(inputDir+"/"+f.name, fileType, schema)
		} else {
			filenameToProcess[k] = input.NewFileToProcess(f.name, fileType, schema)
		}
		// add waiting points, so we can sync later in the execution of the step
		for _, step := range c.steps {
			if step.getStepType() == "reducebykey" {
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
			c.err = err
			return runOutput
		}
	}

	return runOutput
}

func runFile(partition int, fileToProcess input.FileToProcess, waitForContext *sync.WaitGroup, waitForStep *sync.WaitGroup, contexts []*Context) {
	var (
		bufferKey   bytes.Buffer
		bufferValue bytes.Buffer
		err         error
		inputFile   input.Input
	)

	defer waitForContext.Done()

	inputFile = input.NewInput(fileToProcess)
	if err = inputFile.Init(); err != nil {
		contexts[partition].err = err
		// TODO: provide better error control
		panic(err)
		return
	}

	for _, step := range contexts[partition].steps {
		step.setInput(inputFile)

		if err := step.do(partition, len(contexts)); err != nil {
			contexts[partition].err = err
			return
		}
		// file can be closed now
		inputFile.Close()
		// gather input
		bufferKey, bufferValue = step.getOutputKV()

		if step.getStepType() == "reducebykey" {
			// make buffers visible to all contexts
			contexts[partition].outputKey = bufferKey
			contexts[partition].outputValue = bufferValue
			bufferKey = bytes.Buffer{}
			bufferValue = bytes.Buffer{}
			if err := handleReduceSync(partition, waitForStep, contexts, &inputFile, step); err != nil {
				contexts[partition].err = err
				return
			}
			if partition != 0 {
				return
			}
			bufferKey, bufferValue = step.getOutputKV()
		}
		// set inputfile to new input for next step
		switch step.getOutputType() {
		case "value":
			inputFile = input.NewValue(&bufferValue)
		case "kv":
			inputFile = input.NewKeyValue(&bufferKey, &bufferValue)
		}
	}
	contexts[partition].outputKey = bufferKey
	contexts[partition].outputValue = bufferValue
	contexts[partition].outputType = inputFile.GetType()
	return
}

func handleReduceSync(partition int, waitForStep *sync.WaitGroup, contexts []*Context, inputFile *input.Input, step Step) error {
	waitForStep.Done()
	waitForStep.Wait()
	var (
		bufferKey   bytes.Buffer
		bufferValue bytes.Buffer
	)
	// now all the reducebykey steps should be finished
	if partition == 0 {
		for k := range contexts {
			bufferKey.Write(contexts[k].outputKey.Bytes())
			bufferValue.Write(contexts[k].outputValue.Bytes())
			contexts[k].outputKey = bytes.Buffer{}
			contexts[k].outputValue = bytes.Buffer{}
		}
		step.setInput(input.NewKeyValue(&bufferKey, &bufferValue))
		if err := step.do(partition, len(contexts)); err != nil {
			return err
		}
	}
	return nil
}

func toInputFile(fileInfo []os.FileInfo) []InputFile {
	ret := make([]InputFile, len(fileInfo))
	for k := range fileInfo {
		ret[k].name = fileInfo[k].Name()
	}
	return ret
}
