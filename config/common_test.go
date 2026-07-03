package config

import (
	"runtime"
	"testing"
)

func TestWorkers(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{0, runtime.NumCPU()},
		{1, 1},
		{-3, 1},
		{8, 8},
	}
	for _, c := range cases {
		got := Common{Concurrency: c.in}.Workers()
		if got != c.want {
			t.Errorf("Workers(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestCommonSplitAndLogDefaults(t *testing.T) {
	// 零值应表达"关闭拆分";LogLevel/LogFile 默认来自 kong tag,零值结构体这里只验证字段存在与语义。
	var c Common
	if c.SplitCount != 0 {
		t.Errorf("SplitCount zero value = %d, want 0 (off)", c.SplitCount)
	}
	if c.LogLevel != "" || c.LogFile != "" {
		t.Errorf("log fields zero value not empty: level=%q file=%q", c.LogLevel, c.LogFile)
	}
}
