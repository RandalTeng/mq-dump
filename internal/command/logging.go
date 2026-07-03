package command

import (
	"fmt"
	"io"
	"log/slog"
	"os"
)

// setupLogger 按 level/file 构建 slog logger 并设为默认;返回关闭底层文件的函数。
// file == "-" → stderr;否则追加写该文件。level 非法返回错误。
func setupLogger(level, file string) (func(), error) {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	var w io.Writer = os.Stderr
	closer := func() {}
	if file != "-" && file != "" {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open log file %q: %w", file, err)
		}
		w, closer = f, func() { _ = f.Close() }
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: lv})))
	return closer, nil
}

// logDone 是完成事件的薄封装(便于测试触发一条日志)。
func logDone(total int) { slog.Info("done", "total", total) }
