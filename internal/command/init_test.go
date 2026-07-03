package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/RandalTeng/mq-dump/config"
	_ "github.com/RandalTeng/mq-dump/mq/amqp"
)

func TestInitWritesTemplate(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "amqp.yaml")
	cmd := &InitCmd{Output: out}
	if err := cmd.Run(&config.Common{Driver: "amqp"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil || len(b) == 0 {
		t.Fatalf("template not written: %v", err)
	}
}

func TestInitUnknownDriver(t *testing.T) {
	cmd := &InitCmd{}
	if err := cmd.Run(&config.Common{Driver: "nope"}); err == nil {
		t.Error("unknown driver should error")
	}
}
