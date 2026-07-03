// Package config 定义通用配置与驱动配置加载。
package config

import (
	"runtime"
	"time"
)

// Common 是所有命令共享的通用配置,来自 kong flag。
type Common struct {
	Driver      string        `short:"d" required:"" help:"消息队列驱动 (amqp)"`
	Config      string        `short:"c" type:"existingfile" help:"驱动私有配置 YAML 路径"`
	DumpFile    string        `short:"f" help:"dump 文件路径;\"-\" = stdin/stdout"`
	Count       int           `short:"n" help:"导出条数上限;0 = 不限"`
	Timeout     time.Duration `short:"t" help:"导出空闲超时"`
	Concurrency int           `short:"j" default:"1" help:"导入 worker 数;0 = CPU 核心数"`
}

// Workers 解析并发度:0 → NumCPU;<1 → 1;否则原值。
func (c Common) Workers() int {
	if c.Concurrency == 0 {
		return runtime.NumCPU()
	}
	if c.Concurrency < 1 {
		return 1
	}
	return c.Concurrency
}
