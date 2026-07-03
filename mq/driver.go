// Package mq 定义驱动接口、工厂与注册表。
package mq

import (
	"context"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/model"
)

// Driver 是一个消息队列驱动的导入/导出能力。
// emit / next 均由框架保证并发安全,驱动可从多 goroutine 调用。
type Driver interface {
	// Export 逐条产出消息;emit 返回 nil 才表示已落盘(ack-after-persist)。
	Export(ctx context.Context, emit func(model.Message) error) error
	// Import 逐条消费消息;next 返回 ok=false 表示流结束。
	Import(ctx context.Context, next func() (model.Message, bool, error)) error
	Close() error
}

// Factory 构造驱动并交出其私有配置模型/模板。
type Factory interface {
	NewConfig() any
	ConfigTemplate() string
	Open(c config.Common, cfg any) (Driver, error)
}

// Namer 由驱动可选实现,给出无 -f 时的默认 dump 基名(不含扩展名)。
// 通用层类型断言使用;外部驱动可不实现。
type Namer interface {
	DumpName() string
}
