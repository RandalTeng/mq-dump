package dump

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/goccy/go-json"

	"github.com/RandalTeng/mq-dump/model"
)

// Reader 是导入读源:先 Meta 校验驱动,再逐条 Read 到 ok=false。
type Reader interface {
	Meta() (Meta, error)
	Read() (model.Message, bool, error)
	Close() error
}

// OpenReader 打开 path 并按首行内容判定模式:
// 首行含 "parts" → 清单模式(path 即清单);否则单文件(内嵌 meta)。
// path == "-" 或 "" → stdin 单文件模式。
func OpenReader(path string) (Reader, error) {
	if path == "" || path == "-" {
		return newSingleReader(os.Stdin, nil), nil
	}
	head, err := peekFirstLine(path)
	if err != nil {
		return nil, err
	}
	if isManifest(head) {
		return newManifestReader(path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return newSingleReader(f, f), nil
}

// NewSingleReaderForTest 从 io.Reader 构造单文件 Reader(测试/内存流用)。
func NewSingleReaderForTest(r io.Reader) Reader { return newSingleReader(r, nil) }

func peekFirstLine(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	if !sc.Scan() {
		return nil, sc.Err()
	}
	return append([]byte(nil), sc.Bytes()...), nil
}

func isManifest(head []byte) bool {
	var probe struct {
		Parts []Part `json:"parts"`
	}
	if err := json.Unmarshal(head, &probe); err != nil {
		return false
	}
	return probe.Parts != nil
}

// singleReader 复用 Decoder 读内嵌 meta 的单文件。
type singleReader struct {
	dec *Decoder
	c   io.Closer
}

func newSingleReader(r io.Reader, c io.Closer) *singleReader {
	return &singleReader{dec: NewDecoder(r), c: c}
}

func (s *singleReader) Meta() (Meta, error)                { return s.dec.ReadMeta() }
func (s *singleReader) Read() (model.Message, bool, error) { return s.dec.Read() }
func (s *singleReader) Close() error {
	if s.c != nil {
		return s.c.Close()
	}
	return nil
}

// manifestReader 顺序拼接清单里的纯数据分片。
type manifestReader struct {
	man    Manifest
	dir    string
	idx    int
	cur    *os.File
	curDec *Decoder
}

func newManifestReader(path string) (*manifestReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	man, err := ReadManifest(f)
	if err != nil {
		return nil, err
	}
	return &manifestReader{man: man, dir: filepath.Dir(path)}, nil
}

// Meta 由清单头构造(无 parts 的 Meta)。
func (m *manifestReader) Meta() (Meta, error) {
	return Meta{FormatVersion: m.man.FormatVersion, Driver: m.man.Driver, CreatedAt: m.man.CreatedAt}, nil
}

func (m *manifestReader) Read() (model.Message, bool, error) {
	for {
		if m.curDec == nil {
			if m.idx >= len(m.man.Parts) {
				return model.Message{}, false, nil
			}
			p := filepath.Join(m.dir, m.man.Parts[m.idx].File)
			f, err := os.Open(p)
			if err != nil {
				return model.Message{}, false, fmt.Errorf("open part %q: %w", p, err)
			}
			m.cur, m.curDec, m.idx = f, NewDecoder(f), m.idx+1
		}
		msg, ok, err := m.curDec.Read()
		if err != nil {
			return model.Message{}, false, err
		}
		if !ok { // 当前分片读尽,切下一片
			m.cur.Close()
			m.cur, m.curDec = nil, nil
			continue
		}
		return msg, true, nil
	}
}

func (m *manifestReader) Close() error {
	if m.cur != nil {
		return m.cur.Close()
	}
	return nil
}
