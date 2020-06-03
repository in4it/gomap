package writers

import (
	"fmt"
	"io"
)

type CombinedWriter struct {
	readers []Reader
}

func NewCombinedWriter(readers []Reader) *CombinedWriter {
	return &CombinedWriter{
		readers: readers,
	}
}
func (c *CombinedWriter) Read(p []byte) (n int, err error) {
	for _, v := range c.readers {
		n, err := v.Read(p)
		if err != io.EOF {
			return n, err
		}
	}
	return 0, io.EOF // nothing to return
}
func (c *CombinedWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("CombinedWriter doesn't support direct writes")
}
