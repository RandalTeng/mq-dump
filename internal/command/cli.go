// Package command 定义 CLI 聚合与各子命令。
package command

import "github.com/RandalTeng/mq-dump/config"

// CLI 是 kong 根聚合结构:嵌入通用 flag + 各子命令。
type CLI struct {
	config.Common
	Export ExportCmd `cmd:"" help:"导出消息到 dump 文件"`
	Import ImportCmd `cmd:"" help:"从 dump 文件导入消息"`
	Init   InitCmd   `cmd:"" name:"init" help:"生成驱动配置模板"`
}
