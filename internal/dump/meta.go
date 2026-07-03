// Package dump 处理 dump 文件的 JSONL 编解码与 meta 头。
package dump

import (
	"fmt"
	"time"
)

// FormatVersion 是当前 dump 格式版本。
const FormatVersion = 1

// Meta 是 dump 文件首行的头记录。
type Meta struct {
	FormatVersion int    `json:"format_version"`
	Driver        string `json:"driver"`
	CreatedAt     string `json:"created_at"`
}

// NewMeta 用当前时间构造给定驱动的 meta 头。
func NewMeta(driver string) Meta {
	return Meta{FormatVersion: FormatVersion, Driver: driver, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
}

// CheckDriver 校验 meta 声明的驱动与期望一致。
func (m Meta) CheckDriver(want string) error {
	if m.Driver != want {
		return fmt.Errorf("dump driver %q != requested %q", m.Driver, want)
	}
	return nil
}
