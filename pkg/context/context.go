package context

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

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

func (c *Context) Run() *RunOutput {
	var (
		runOutput         *RunOutput
		isDirectory       bool
		err               error
		files             []os.FileInfo
		inputDir          string
		waitForContext    sync.WaitGroup
		waitForStep       sync.WaitGroup
		filenameToProcess []string
	)
	if isDirectory, err = c.isFileOrDirectory(c.input); err != nil {
		c.err = err
		return runOutput
	}
	if isDirectory {
		inputDir = c.input
		files, err = ioutil.ReadDir(c.input)
		if err != nil {
			c.err = err
			return runOutput
		}
	} else {
		inputDir = filepath.Dir(c.input)
		fstat, err := os.Stat(c.input)
		if err != nil {
			c.err = err
			return runOutput
		}
		files = append(files, fstat)
	}

	runOutput = &RunOutput{}
	runOutput.Contexts = make([]*Context, len(files))
	filenameToProcess = make([]string, len(files))
	for k, f := range files {
		runOutput.Contexts[k] = &Context{
			steps: copySteps(c.steps),
			input: c.input,
		}
		filenameToProcess[k] = inputDir + "/" + f.Name()
		// add waiting points, so we can sync later in the execution of the step
		for _, step := range c.steps {
			if step.getStepType() == "reducebykey" {
				waitForStep.Add(1)
			}
		}
	}
	for k := range runOutput.Contexts {
		waitForContext.Add(1)
		go func(partition int, file string) {
			runFile(partition, file, &waitForContext, &waitForStep, runOutput.Contexts)
		}(k, filenameToProcess[k])
	}
	// wait for completion of the contexts
	waitForContext.Wait()

	return runOutput
}

func runFile(partition int, filename string, waitForContext *sync.WaitGroup, waitForStep *sync.WaitGroup, contexts []*Context) {
	var (
		buffer      bytes.Buffer
		bufferKey   bytes.Buffer
		bufferValue bytes.Buffer
	)

	defer waitForContext.Done()

	fmt.Printf("runFile: %s (partition %d)\n", filename, partition+1)

	file, err := os.Open(filename)
	if err != nil {
		contexts[partition].err = err
		return
	}
	defer file.Close()

	for k, step := range contexts[partition].steps {
		if k == 0 {
			step.setScanner(bufio.NewScanner(file))
		} else {
			step.setScanner(bufio.NewScanner(&buffer))
			step.setScannerKV(bufio.NewScanner(&bufferKey), bufio.NewScanner(&bufferValue))
		}
		if err = step.do(partition, len(contexts)); err != nil {
			contexts[partition].err = err
			return
		}
		buffer = step.getOutput()
		bufferKey, bufferValue = step.getOutputKV()

		if step.getStepType() == "reducebykey" {
			// make buffers visible to all contexts
			contexts[partition].outputKey = bufferKey
			contexts[partition].outputValue = bufferValue
			bufferKey = bytes.Buffer{}
			bufferValue = bytes.Buffer{}
			if err := handleReduceSync(partition, waitForStep, contexts, step); err != nil {
				contexts[partition].err = err
				return
			}
			if partition != 0 {
				return
			}
			bufferKey, bufferValue = step.getOutputKV()
		}
	}
	contexts[partition].output = buffer
	contexts[partition].outputKey = bufferKey
	contexts[partition].outputValue = bufferValue
	return
}
func handleReduceSync(partition int, waitForStep *sync.WaitGroup, contexts []*Context, step Step) error {
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
			contexts[k].output = bytes.Buffer{}
		}
		step.setScannerKV(bufio.NewScanner(&bufferKey), bufio.NewScanner(&bufferValue))
		if err := step.do(partition, len(contexts)); err != nil {
			return err
		}
	}
	return nil
}
