// Command mq-dump 是消息队列导入/导出工具。
package main

import (
	"os"

	"github.com/RandalTeng/mq-dump/internal/command"
	_ "github.com/RandalTeng/mq-dump/mq/amqp" // 注册 amqp 驱动
)

func main() {
	command.Run(os.Args[1:])
}
