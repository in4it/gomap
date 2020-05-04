package context

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type RunOutput struct {
	Contexts []*Context
}

func (d *Context) isFileOrDirectory(name string) (bool, error) {
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
		runOutput   *RunOutput
		isDirectory bool
		err         error
		files       []os.FileInfo
		inputDir    string
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

	// TODO: run runFile as goroutine, use channels to communicate
	runOutput = &RunOutput{}
	runOutput.Contexts = make([]*Context, len(files))
	for k, f := range files {
		runOutput.Contexts[k] = c
		runOutput.Contexts[k].runFile(inputDir + "/" + f.Name())
	}
	// merge contexts and output one context
	return runOutput
}

func (c *Context) runFile(filename string) *Context {
	var (
		buffer      bytes.Buffer
		bufferKey   bytes.Buffer
		bufferValue bytes.Buffer
	)

	file, err := os.Open(filename)
	if err != nil {
		c.err = err
		return c
	}
	defer file.Close()

	for k, step := range c.steps {
		if k == 0 {
			step.setScanner(bufio.NewScanner(file))
		} else {
			step.setScanner(bufio.NewScanner(&buffer))
			step.setScannerKV(bufio.NewScanner(&bufferKey), bufio.NewScanner(&bufferValue))
		}
		if err = step.do(); err != nil {
			c.err = err
			return c
		}
		buffer = step.getOutput()
		bufferKey, bufferValue = step.getOutputKV()
	}
	c.output = buffer
	c.outputKey = bufferKey
	c.outputValue = bufferValue
	return c
}

func (r *RunOutput) Print() {
	for _, context := range r.Contexts {
		scanner := bufio.NewScanner(&context.output)
		keyScanner := bufio.NewScanner(&context.outputKey)
		valueScanner := bufio.NewScanner(&context.outputValue)

		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
		for keyScanner.Scan() {
			valueScanner.Scan()
			fmt.Println(keyScanner.Text() + "," + valueScanner.Text())
		}
		if err := keyScanner.Err(); err != nil {
			panic(err)
		}
		if err := valueScanner.Err(); err != nil {
			panic(err)
		}
	}
}
