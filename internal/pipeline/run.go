// Package pipeline 连接 dump codec 与驱动,编排导入/导出。
package pipeline

import (
	"context"
	"io"
	"sync"

	"github.com/RandalTeng/mq-dump/internal/dump"
	"github.com/RandalTeng/mq-dump/model"
	"github.com/RandalTeng/mq-dump/mq"
)

// Export 让驱动产出消息,逐条经并发安全 emit 写入 JSONL(带 meta 头);count>0 时到量即止。
func Export(ctx context.Context, w io.Writer, driver string, count int, d mq.Driver) error {
	enc := dump.NewEncoder(w, driver)
	if err := enc.WriteMeta(); err != nil {
		return err
	}
	var mu sync.Mutex
	var n int
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	emit := func(m model.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if count > 0 && n >= count {
			cancel()
			return nil
		}
		if err := enc.Write(m); err != nil {
			return err
		}
		n++
		if count > 0 && n >= count {
			cancel()
		}
		return nil
	}
	return d.Export(ctx, emit)
}

// Import 从 JSONL 读取(校验 meta.driver),经并发安全 next 交给驱动 publish。
func Import(ctx context.Context, r io.Reader, driver string, d mq.Driver) error {
	dec := dump.NewDecoder(r)
	meta, err := dec.ReadMeta()
	if err != nil {
		return err
	}
	if err := meta.CheckDriver(driver); err != nil {
		return err
	}
	var mu sync.Mutex
	next := func() (model.Message, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return dec.Read()
	}
	return d.Import(ctx, next)
}
