package command

import (
	"io"
	"os"
)

// openDumpWriter 打开导出目标:"-" 或空 → stdout。
func openDumpWriter(path string) (io.WriteCloser, error) {
	if path == "" || path == "-" {
		return nopWriteCloser{os.Stdout}, nil
	}
	return os.Create(path)
}

// openDumpReader 打开导入源:"-" 或空 → stdin。
func openDumpReader(path string) (io.ReadCloser, error) {
	if path == "" || path == "-" {
		return io.NopCloser(os.Stdin), nil
	}
	return os.Open(path)
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
