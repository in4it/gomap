package context

import (
	"bufio"
	"fmt"
)

type RunOutput struct {
	Contexts []*Context
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
func (r *RunOutput) Get() string {
	ret := ""
	for _, context := range r.Contexts {
		scanner := bufio.NewScanner(&context.output)

		for scanner.Scan() {
			ret += scanner.Text() + "\n"
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
	}
	return ret
}

func (r *RunOutput) GetKV() (string, string) {
	key := ""
	value := ""
	for _, context := range r.Contexts {
		keyScanner := bufio.NewScanner(&context.outputKey)
		valueScanner := bufio.NewScanner(&context.outputValue)

		for keyScanner.Scan() {
			valueScanner.Scan()
			key += keyScanner.Text() + "\n"
			value += valueScanner.Text() + "\n"
		}
		if err := keyScanner.Err(); err != nil {
			panic(err)
		}
		if err := valueScanner.Err(); err != nil {
			panic(err)
		}
	}
	return key, value
}
