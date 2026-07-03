package dump

import (
	"bufio"
	"fmt"
	"io"

	"github.com/goccy/go-json"

	"github.com/RandalTeng/mq-dump/model"
)

// Encoder 把 meta 头与消息逐行写为 JSONL。
type Encoder struct {
	enc    *json.Encoder
	driver string
}

// NewEncoder 创建写入 w 的编码器,driver 用于 meta 头。
func NewEncoder(w io.Writer, driver string) *Encoder {
	return &Encoder{enc: json.NewEncoder(w), driver: driver}
}

// WriteMeta 写入首行 meta 头,必须在任何 Write 之前调用一次。
func (e *Encoder) WriteMeta() error { return e.enc.Encode(NewMeta(e.driver)) }

// Write 追加一条消息(一行 JSON)。
func (e *Encoder) Write(m model.Message) error { return e.enc.Encode(m) }

// Decoder 读取 JSONL dump:先 ReadMeta 再循环 Read。
type Decoder struct {
	sc *bufio.Scanner
}

// NewDecoder 创建读取 r 的解码器。
func NewDecoder(r io.Reader) *Decoder {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	return &Decoder{sc: sc}
}

// ReadMeta 读取并校验首行 meta 头。
func (d *Decoder) ReadMeta() (Meta, error) {
	if !d.sc.Scan() {
		if err := d.sc.Err(); err != nil {
			return Meta{}, err
		}
		return Meta{}, fmt.Errorf("empty dump: missing meta header")
	}
	var m Meta
	if err := json.Unmarshal(d.sc.Bytes(), &m); err != nil {
		return Meta{}, fmt.Errorf("parse meta header: %w", err)
	}
	if m.FormatVersion == 0 {
		return Meta{}, fmt.Errorf("first line is not a valid meta header")
	}
	return m, nil
}

// Read 读取下一条消息;ok=false 表示到达流末尾。
func (d *Decoder) Read() (model.Message, bool, error) {
	if !d.sc.Scan() {
		return model.Message{}, false, d.sc.Err()
	}
	var m model.Message
	if err := json.Unmarshal(d.sc.Bytes(), &m); err != nil {
		return model.Message{}, false, fmt.Errorf("parse message: %w", err)
	}
	return m, true, nil
}
