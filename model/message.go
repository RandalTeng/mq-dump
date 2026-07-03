// Package model 定义驱动无关的通用消息信封。
package model

import (
	"time"

	"github.com/goccy/go-json"
)

// Message 是 dump 文件中一条消息的通用信封,通用层不解析 Properties。
type Message struct {
	Body       []byte          `json:"body"`
	Timestamp  time.Time       `json:"timestamp,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
}
