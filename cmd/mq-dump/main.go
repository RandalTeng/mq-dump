// Command mq-dump 是消息队列导入/导出工具。
package main

import (
	"github.com/alecthomas/kong"

	"github.com/RandalTeng/mq-dump/internal/command"
	_ "github.com/RandalTeng/mq-dump/mq/amqp" // 注册 amqp 驱动
)

func main() {
	var cli command.CLI
	kctx := kong.Parse(&cli, kong.Name("mq-dump"),
		kong.Description("消息队列导入/导出工具(v1: AMQP)"))
	kctx.FatalIfErrorf(kctx.Run(&cli.Common))
}
