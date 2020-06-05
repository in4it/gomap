package writers

import "bytes"

// MemoryWriter is the default writer.
// It uses bytes.Buffer to buffer all the data in memory
type MemoryWriter struct {
	buffer bytes.Buffer
}

func NewMemoryWriter() *MemoryWriter {
	return &MemoryWriter{}
}
func (m *MemoryWriter) Read(p []byte) (n int, err error) {
	return m.buffer.Read(p)
}

func (m *MemoryWriter) Write(p []byte) (n int, err error) {
	return m.buffer.Write(p)
}

func (m *MemoryWriter) Close() error {
	return nil // not implemented
}
func (m *MemoryWriter) Cleanup() error {
	return nil // not implemented
}
func (m *MemoryWriter) New() (WriterReader, error) {
	return NewMemoryWriter(), nil
}
