package command

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSetupLoggerWritesFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "mq-dump.log")
	closer, err := setupLogger("info", logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closer()
	logDone(5) // 触发一条 info 日志
	b, err := os.ReadFile(logPath)
	if err != nil || len(b) == 0 {
		t.Fatalf("log file empty: err=%v", err)
	}
}

func TestSetupLoggerBadLevel(t *testing.T) {
	if _, err := setupLogger("nope", "-"); err == nil {
		t.Error("bad level should error")
	}
}
