package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/RandalTeng/mq-dump/config"
)

type namer struct{ name string }

func (n namer) DumpName() string { return n.name }

func TestResolveExportWriterDefaultName(t *testing.T) {
	dir := t.TempDir()
	old, _ := os.Getwd()
	defer os.Chdir(old)
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	w, err := resolveExportWriter(&config.Common{}, "amqp", namer{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := os.Stat("orders.jsonl"); err != nil {
		t.Errorf("default file orders.jsonl not created: %v", err)
	}
}

func TestResolveExportWriterNoNamerNoFile(t *testing.T) {
	if _, err := resolveExportWriter(&config.Common{}, "amqp", nil); err == nil {
		t.Error("no -f and no Namer should error")
	}
}

func TestResolveExportWriterSplitStdoutErrors(t *testing.T) {
	if _, err := resolveExportWriter(&config.Common{DumpFile: "-", SplitCount: 2}, "amqp", nil); err == nil {
		t.Error("split + stdout should error")
	}
}

func TestResolveExportWriterSplitCreatesManifest(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "orders")
	w, err := resolveExportWriter(&config.Common{DumpFile: base + ".jsonl", SplitCount: 2}, "amqp", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(base + ".mqdump.json"); err != nil {
		t.Errorf("manifest not created: %v", err)
	}
}
