package dump

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/RandalTeng/mq-dump/model"
)

func TestOpenReaderManifestMode(t *testing.T) {
	dir := t.TempDir()
	stem := filepath.Join(dir, "orders")
	w, _ := NewSplitWriter(stem, "amqp", 2)
	for i := 0; i < 3; i++ {
		_ = w.Write(model.Message{Body: []byte{byte('a' + i)}})
	}
	_ = w.Close()

	r, err := OpenReader(stem + ".mqdump.json")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	meta, err := r.Meta()
	if err != nil || meta.Driver != "amqp" {
		t.Fatalf("meta = %+v err=%v", meta, err)
	}
	var n int
	for {
		_, ok, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		n++
	}
	if n != 3 {
		t.Errorf("aggregated %d msgs, want 3", n)
	}
}

func TestOpenReaderSingleMode(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "one.jsonl")
	f, _ := os.Create(p)
	w := NewSingleWriterFile(f, "amqp")
	_ = w.WriteMeta()
	_ = w.Write(model.Message{Body: []byte("x")})
	_ = w.Close()

	r, err := OpenReader(p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if meta, err := r.Meta(); err != nil || meta.Driver != "amqp" {
		t.Fatalf("meta = %+v err=%v", meta, err)
	}
	if _, ok, _ := r.Read(); !ok {
		t.Error("expected one message")
	}
}
