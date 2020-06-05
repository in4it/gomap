package writers

import (
	"fmt"
	"io"
)

// CombinedWriter can combine multiple writers together. This is used internally to reduce from 5 partitions to 1 (combining 5 writers into 1)
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
func (c *CombinedWriter) Close() error {
	for _, v := range c.readers {
		if err := v.Close(); err != nil {
			return err
		}
	}
	return nil
}
func (c *CombinedWriter) Cleanup() error {
	for _, v := range c.readers {
		if err := v.Cleanup(); err != nil {
			return err
		}
	}
	return nil
}
func (c *CombinedWriter) New() (WriterReader, error) {
	return NewCombinedWriter(c.readers), nil
}
