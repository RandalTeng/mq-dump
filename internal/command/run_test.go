package command

import (
	"bytes"
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	_ "github.com/RandalTeng/mq-dump/mq/amqp" // 注册 amqp 驱动供解析
)

// exitPanic 携带退出码,模拟 kong.Exit "不返回" 的契约。
type exitPanic int

// runCapture 以注入的 writers/exit 执行 Run,捕获 stdout、stderr 与退出码。
// 未触发 Exit(正常执行到底)时 code = -1。
func runCapture(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	var out, errb bytes.Buffer
	code = -1
	defer func() {
		if r := recover(); r != nil {
			ep, ok := r.(exitPanic)
			if !ok {
				panic(r) // 非预期 panic,继续抛出
			}
			code = int(ep)
		}
		stdout, stderr = out.String(), errb.String()
	}()
	Run(args,
		kong.Writers(&out, &errb),
		kong.Exit(func(c int) { panic(exitPanic(c)) }),
	)
	return
}

func TestRunNoArgsShowsHelp(t *testing.T) {
	stdout, _, code := runCapture(t)
	if code != 0 {
		t.Fatalf("no-args exit code = %d, want 0", code)
	}
	if !strings.Contains(stdout, "Usage: mq-dump") {
		t.Errorf("no-args stdout missing usage help:\n%s", stdout)
	}
}

func TestRunUnknownFlagShowsHelpAndFails(t *testing.T) {
	stdout, stderr, code := runCapture(t, "--nope")
	if code == 0 {
		t.Fatalf("unknown flag exit code = 0, want non-zero")
	}
	if !strings.Contains(stdout, "Usage: mq-dump") {
		t.Errorf("unknown flag stdout missing usage help:\n%s", stdout)
	}
	if !strings.Contains(stderr, "error") {
		t.Errorf("unknown flag stderr missing error line:\n%s", stderr)
	}
}

func TestRunUnknownCommandShowsHelpAndFails(t *testing.T) {
	stdout, stderr, code := runCapture(t, "bogus")
	if code == 0 {
		t.Fatalf("unknown command exit code = 0, want non-zero")
	}
	if !strings.Contains(stdout, "Usage: mq-dump") {
		t.Errorf("unknown command stdout missing usage help:\n%s", stdout)
	}
	if !strings.Contains(stderr, "error") {
		t.Errorf("unknown command stderr missing error line:\n%s", stderr)
	}
}

func TestRunMissingRequiredDriverShowsHelpAndFails(t *testing.T) {
	// export 需要必填 --driver;缺失应报用法错误。
	stdout, stderr, code := runCapture(t, "export")
	if code == 0 {
		t.Fatalf("missing --driver exit code = 0, want non-zero")
	}
	if !strings.Contains(stdout, "Usage: mq-dump") {
		t.Errorf("missing --driver stdout missing usage help:\n%s", stdout)
	}
	if !strings.Contains(stderr, "error") {
		t.Errorf("missing --driver stderr missing error line:\n%s", stderr)
	}
}
