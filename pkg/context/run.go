package context

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

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

func (c *Context) Run() *Context {
	var (
		isDirectory bool
		err         error
		files       []os.FileInfo
		inputDir    string
	)
	if isDirectory, err = c.isFileOrDirectory(c.input); err != nil {
		c.err = err
		return c
	}
	if isDirectory {
		inputDir = c.input
		files, err = ioutil.ReadDir(c.input)
		if err != nil {
			c.err = err
			return c
		}
	} else {
		inputDir = filepath.Dir(c.input)
		fstat, err := os.Stat(c.input)
		if err != nil {
			c.err = err
			return c
		}
		files = append(files, fstat)
	}

	var (
		contexts []*Context
	)
	// TODO: run runFile as goroutine, use channels to communicate
	for _, f := range files {
		contexts = append(contexts, c.runFile(inputDir+"/"+f.Name()))
	}
	// merge contexts and output one context
	return contexts[0]
}

func (c *Context) runFile(filename string) *Context {
	var (
		buffer bytes.Buffer
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
		}
		if err = step.do(); err != nil {
			c.err = err
			return c
		}
		buffer = step.getOutput()
	}
	c.output = buffer
	return c
}

func (c *Context) Print() {
	scanner := bufio.NewScanner(&c.output)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
}
