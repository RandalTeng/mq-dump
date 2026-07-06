package dump

import (
	"fmt"
	"io"

	"github.com/goccy/go-json"
)

// Part 是清单中一个分片的记录。
type Part struct {
	File  string `json:"file"`  // 相对清单所在目录
	Count int    `json:"count"` // 该分片消息条数
}

// Manifest 是拆分导出的独立索引(单行 JSON)。
type Manifest struct {
	FormatVersion int    `json:"format_version"`
	Driver        string `json:"driver"`
	CreatedAt     string `json:"created_at"`
	// UpdatedAt 每次清单落地刷新为当时时间(逐分片崩溃安全重写与收尾)。
	UpdatedAt string `json:"updated_at"`
	Parts     []Part `json:"parts"`
	Total     int    `json:"total"`
}

// WriteManifest 把清单写为单行 JSON(带结尾换行)。
func WriteManifest(w io.Writer, m Manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if _, err := w.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

// ReadManifest 解析单行 JSON 清单。
func ReadManifest(r io.Reader) (Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("parse manifest: %w", err)
	}
	return m, nil
}
