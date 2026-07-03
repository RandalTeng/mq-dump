package mq

import (
	"fmt"
	"sort"
)

var registry = map[string]Factory{}

// Register 注册一个驱动;重复名 panic(编程错误)。
func Register(name string, f Factory) {
	if _, dup := registry[name]; dup {
		panic(fmt.Sprintf("mq: driver %q already registered", name))
	}
	registry[name] = f
}

// Get 按名取驱动工厂。
func Get(name string) (Factory, bool) {
	f, ok := registry[name]
	return f, ok
}

// Names 返回已注册驱动名(排序)。
func Names() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
