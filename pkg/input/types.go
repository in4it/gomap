package input

type FileToProcess struct {
	filename string
	fileType string
	schema   interface{}
}
type Input interface {
	Init() error
	Close()
	Scan() bool
	Bytes() ([]byte, []byte)
	Err() (error, error)
	GetType() string
}
