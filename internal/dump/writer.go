package dump

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/RandalTeng/mq-dump/model"
)

// Writer 是导出写目标:先 WriteMeta 一次,再逐条 Write,最后 Close。
type Writer interface {
	WriteMeta() error
	Write(m model.Message) error
	Close() error
}

// singleWriter 写单文件:首行 meta + 逐条消息(v1 格式)。
type singleWriter struct {
	enc *Encoder
	c   io.Closer // 底层文件;stdout/内存流传 nil
}

// NewSingleWriter 创建写 w 的单文件 Writer;driver 用于 meta 头。
// 适用于 stdout / 内存流(不关闭底层)。
func NewSingleWriter(w io.Writer, driver string) *singleWriter {
	return &singleWriter{enc: NewEncoder(w, driver)}
}

// NewSingleWriterFile 创建写文件 f 的单文件 Writer,Close 时关闭 f。
func NewSingleWriterFile(f *os.File, driver string) *singleWriter {
	return &singleWriter{enc: NewEncoder(f, driver), c: f}
}

func (s *singleWriter) WriteMeta() error            { return s.enc.WriteMeta() }
func (s *singleWriter) Write(m model.Message) error { return s.enc.Write(m) }
func (s *singleWriter) Close() error {
	if s.c != nil {
		return s.c.Close()
	}
	return nil
}

// splitWriter 按条数轮转分片(每片为独立 v1 单文件),并维护独立清单。
type splitWriter struct {
	stem      string // 基路径(不含扩展名),如 /out/orders
	driver    string
	limit     int    // 每分片条数上限(>0)
	createdAt string // 导出起始时刻;仅用于清单 created_at(分片各自用写入当时时间)

	cur     *os.File
	curEnc  *Encoder
	curN    int    // 当前分片已写条数
	curFile string // 当前分片文件名(basename)
	parts   []Part
	total   int
}

// NewSplitWriter 创建按 limit 条轮转的拆分 Writer;stem 为基路径(不含扩展名)。
func NewSplitWriter(stem, driver string, limit int) (*splitWriter, error) {
	sw := &splitWriter{
		stem:      stem,
		driver:    driver,
		limit:     limit,
		createdAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := sw.rotate(); err != nil {
		return nil, err
	}
	return sw, nil
}

// WriteMeta 对拆分为 no-op:meta 落在清单里。
func (s *splitWriter) WriteMeta() error { return nil }

func (s *splitWriter) Write(m model.Message) error {
	if s.curN >= s.limit {
		if err := s.closeCurrent(); err != nil {
			return err
		}
		if err := s.rotate(); err != nil {
			return err
		}
	}
	if err := s.curEnc.Write(m); err != nil {
		return err
	}
	s.curN++
	s.total++
	return nil
}

func (s *splitWriter) Close() error {
	if err := s.closeCurrent(); err != nil {
		return err
	}
	return s.writeManifest()
}

// rotate 打开下一个分片文件并写入 meta 头(导出当时时间);分片即独立 v1 单文件。
func (s *splitWriter) rotate() error {
	name := fmt.Sprintf("%s-%03d.jsonl", filepath.Base(s.stem), len(s.parts))
	full := filepath.Join(filepath.Dir(s.stem), name)
	f, err := os.Create(full)
	if err != nil {
		return fmt.Errorf("create part %q: %w", full, err)
	}
	enc := NewEncoder(f, s.driver)
	if err := enc.WriteMeta(); err != nil {
		f.Close()
		return fmt.Errorf("write part meta %q: %w", full, err)
	}
	s.cur, s.curEnc, s.curN, s.curFile = f, enc, 0, name
	return nil
}

// closeCurrent 关闭当前分片、登记到 parts、并重写清单(崩溃安全)。
func (s *splitWriter) closeCurrent() error {
	if s.cur == nil {
		return nil
	}
	if err := s.cur.Close(); err != nil {
		return fmt.Errorf("close part: %w", err)
	}
	if s.curN > 0 {
		s.parts = append(s.parts, Part{File: s.curFile, Count: s.curN})
	} else {
		_ = os.Remove(filepath.Join(filepath.Dir(s.stem), s.curFile)) // 空分片删除
	}
	s.cur, s.curEnc = nil, nil
	return s.writeManifest()
}

// writeManifest 用当前已完成 parts 重写清单(单行 JSON)。
func (s *splitWriter) writeManifest() error {
	man := Manifest{
		FormatVersion: FormatVersion,
		Driver:        s.driver,
		CreatedAt:     s.createdAt,
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339),
		Parts:         s.parts,
		Total:         s.total,
	}
	f, err := os.Create(s.stem + ".mqdump.json")
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	defer f.Close()
	return WriteManifest(f, man)
}
