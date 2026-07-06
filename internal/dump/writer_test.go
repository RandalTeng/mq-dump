package dump

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/RandalTeng/mq-dump/model"
)

func TestSingleWriterEmbedsMeta(t *testing.T) {
	var buf bytes.Buffer
	w := NewSingleWriter(&buf, "amqp")
	if err := w.WriteMeta(); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(model.Message{Body: []byte("hi")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// 首行是 meta,次行是消息:共 2 行。
	if n := bytes.Count(buf.Bytes(), []byte("\n")); n != 2 {
		t.Errorf("line count = %d, want 2\n%s", n, buf.String())
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"driver":"amqp"`)) {
		t.Errorf("meta driver missing:\n%s", buf.String())
	}
}

func TestSplitWriterRotatesAndManifests(t *testing.T) {
	dir := t.TempDir()
	stem := filepath.Join(dir, "orders")
	w, err := NewSplitWriter(stem, "amqp", 2) // 每 2 条一分片
	if err != nil {
		t.Fatal(err)
	}
	_ = w.WriteMeta()        // split 下为 no-op
	for i := 0; i < 5; i++ { // 2 + 2 + 1 → 3 分片
		if err := w.Write(model.Message{Body: []byte{byte('0' + i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	man, err := os.Open(stem + ".mqdump.json")
	if err != nil {
		t.Fatal(err)
	}
	defer man.Close()
	m, err := ReadManifest(man)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Parts) != 3 || m.Total != 5 {
		t.Fatalf("parts=%d total=%d, want 3/5: %+v", len(m.Parts), m.Total, m)
	}
	if m.Parts[0].Count != 2 || m.Parts[2].Count != 1 {
		t.Errorf("part counts wrong: %+v", m.Parts)
	}
	if m.Parts[0].File != "orders-000.jsonl" {
		t.Errorf("part file = %q, want orders-000.jsonl", m.Parts[0].File)
	}
	// 分片为独立 v1 单文件:首行即 meta 头。
	b, _ := os.ReadFile(filepath.Join(dir, m.Parts[0].File))
	if !bytes.Contains(b, []byte(`"format_version"`)) {
		t.Errorf("part must carry meta header:\n%s", b)
	}
	if !bytes.Contains(b, []byte(`"driver":"amqp"`)) {
		t.Errorf("part meta must carry driver:\n%s", b)
	}
	if m.UpdatedAt == "" {
		t.Errorf("manifest updated_at must be set: %+v", m)
	}
}
