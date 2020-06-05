package writers

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestMap(t *testing.T) {
	fmt.Printf("Writing to tmp dir: %s\n", os.TempDir())

	tests := map[string][]uint32{
		"hello world test1": {5, 3}, // maxlength + write split
		"hello world test2": {100, 10},
	}
	for str, maxlen := range tests {
		m, err := newMemoryAndDiskWriter(maxlen[0])
		if err != nil {
			t.Errorf("Initialize error: %s", err)
			return
		}
		_, err = m.Write([]byte(str[:maxlen[1]]))
		if err != nil {
			t.Errorf("Read error: %s", err)
			return
		}
		_, err = m.Write([]byte(str[maxlen[1]:]))
		if err != nil {
			t.Errorf("Read error: %s", err)
			return
		}

		if err := m.Close(); err != nil {
			t.Errorf("close error: %s", err)
			return
		}

		res := make([]byte, len(str))
		n, err := m.Read(res)
		if err != nil && err != io.EOF {
			t.Errorf("Read error: %s", err)
			return
		}
		if string(res) != str {
			t.Errorf("Unexpected result (%d bytes read): %s vs %s\n", n, res, str)
		}

		m.Cleanup()
	}
}
