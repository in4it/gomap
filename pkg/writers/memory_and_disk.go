package writers

import "bytes"

type MemoryAndDiskWriter struct {
	buffer bytes.Buffer
}

func NewMemoryAndDiskWriter() *MemoryAndDiskWriter {
	return &MemoryAndDiskWriter{}
}
func (m *MemoryAndDiskWriter) Read(p []byte) (n int, err error) {
	return m.buffer.Read(p)
}
func (m *MemoryAndDiskWriter) Write(p []byte) (n int, err error) {
	return m.buffer.Write(p)
}
