// Package pipeline 连接 dump codec 与驱动,编排导入/导出。
package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"github.com/RandalTeng/mq-dump/internal/dump"
	"github.com/RandalTeng/mq-dump/model"
	"github.com/RandalTeng/mq-dump/mq"
)

const progressEvery = 10000

// Export 让驱动产出消息,逐条经并发安全 emit 写入 Writer;count>0 时到量即止。
func Export(ctx context.Context, w dump.Writer, count int, d mq.Driver) error {
	if err := w.WriteMeta(); err != nil {
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
		if err := w.Write(m); err != nil {
			return err
		}
		n++
		if n%progressEvery == 0 {
			slog.Info("export progress", "done", n)
		}
		if count > 0 && n >= count {
			cancel()
		}
		return nil
	}
	if err := d.Export(ctx, emit); err != nil {
		return err
	}
	slog.Info("export done", "total", n)
	return nil
}

// Import 从 Reader 读取(校验 meta.driver),经并发安全 next 交给驱动 publish。
func Import(ctx context.Context, r dump.Reader, driver string, d mq.Driver) error {
	meta, err := r.Meta()
	if err != nil {
		return err
	}
	if err := meta.CheckDriver(driver); err != nil {
		return err
	}
	var mu sync.Mutex
	var n int
	next := func() (model.Message, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		m, ok, err := r.Read()
		if ok {
			n++
			if n%progressEvery == 0 {
				slog.Info("import progress", "done", n)
			}
		}
		return m, ok, err
	}
	if err := d.Import(ctx, next); err != nil {
		return err
	}
	slog.Info("import done", "total", n)
	return nil
}
