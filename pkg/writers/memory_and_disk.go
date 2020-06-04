package writers

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

type MemoryAndDiskWriter struct {
	length    uint32
	maxLength uint32
	buffer    bytes.Buffer
	tmpFile   *os.File
	readonly  bool
}

func NewMemoryAndDiskWriter(maxLength uint32) (*MemoryAndDiskWriter, error) {
	file, err := ioutil.TempFile("", "gomap.*.dat")
	if err != nil {
		return nil, fmt.Errorf("Can't write temporary file: %s", err)
	}
	return &MemoryAndDiskWriter{
		maxLength: maxLength,
		tmpFile:   file,
	}, nil
}
func (m *MemoryAndDiskWriter) Read(p []byte) (n int, err error) {
	n, err = m.buffer.Read(p)
	if err != io.EOF {
		if len(p) == n {
			return n, err
		}
		// we need to do a partial read from file
		partial := make([]byte, len(p)-n)
		nn, err := m.tmpFile.Read(partial)
		if nn != 0 {
			copy(p[n:], partial)
		}
		return n + nn, err
	}
	return m.tmpFile.Read(p)
}
func (m *MemoryAndDiskWriter) Write(p []byte) (n int, err error) {
	if m.readonly {
		return 0, fmt.Errorf("Can't write to buffer: writer is marked as readonly")
	}
	m.length += uint32(len(p))
	if m.length >= m.maxLength { // length >= maxlength: spill to disk
		return m.tmpFile.Write(p)
	}
	return m.buffer.Write(p)
}

func (m *MemoryAndDiskWriter) Close() error {
	filename := m.tmpFile.Name()
	var err error
	if err = m.tmpFile.Close(); err != nil {
		return fmt.Errorf("Close() error: %s", err)
	}

	if m.tmpFile, err = os.Open(filename); err != nil {
		return fmt.Errorf("Can't open temporary file for reading: %s", err)
	}

	m.readonly = true

	return nil
}
func (m *MemoryAndDiskWriter) Cleanup() error {
	err := m.tmpFile.Close()
	if err != nil {
		return err
	}
	return os.Remove(m.tmpFile.Name())
}
func (m *MemoryAndDiskWriter) New() (WriterReader, error) {
	return NewMemoryAndDiskWriter(m.maxLength)
}
