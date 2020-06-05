package writers

type Writer interface {
	Write(p []byte) (n int, err error)
}
type Reader interface {
	Read(p []byte) (n int, err error)
	Close() error
	Cleanup() error
	New() (WriterReader, error)
}

type WriterReader interface {
	Writer
	Reader
}
