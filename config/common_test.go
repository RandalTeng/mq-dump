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
