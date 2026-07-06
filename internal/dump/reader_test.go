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

func TestOpenReaderPartStandalone(t *testing.T) {
	dir := t.TempDir()
	stem := filepath.Join(dir, "orders")
	w, _ := NewSplitWriter(stem, "amqp", 2)
	for i := 0; i < 3; i++ { // 2 + 1 → 分片 000 有 2 条
		_ = w.Write(model.Message{Body: []byte{byte('a' + i)}})
	}
	_ = w.Close()

	// 直接打开单个分片(不经清单):应判为单文件、meta 可读、消息数正确。
	r, err := OpenReader(filepath.Join(dir, "orders-000.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	meta, err := r.Meta()
	if err != nil || meta.Driver != "amqp" {
		t.Fatalf("part meta = %+v err=%v", meta, err)
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
	if n != 2 {
		t.Errorf("standalone part has %d msgs, want 2", n)
	}
}

// TestResolvePart:分片路径解析——普通 basename / 子目录放行,绝对路径与逃出清单目录的 .. 拒绝。
func TestResolvePart(t *testing.T) {
	dir := filepath.FromSlash("/data/dumps")
	ok := []struct{ file, want string }{
		{"orders-000.jsonl", filepath.Join(dir, "orders-000.jsonl")},
		{"parts/orders-000.jsonl", filepath.Join(dir, "parts", "orders-000.jsonl")},
		{"./orders-000.jsonl", filepath.Join(dir, "orders-000.jsonl")},
	}
	for _, c := range ok {
		got, err := resolvePart(dir, c.file)
		if err != nil || got != c.want {
			t.Errorf("resolvePart(%q) = %q,%v; want %q,nil", c.file, got, err, c.want)
		}
	}
	bad := []string{"../evil.jsonl", "sub/../../evil.jsonl", "..", t.TempDir()} // t.TempDir() 为真实绝对路径
	for _, f := range bad {
		if got, err := resolvePart(dir, f); err == nil {
			t.Errorf("resolvePart(%q) = %q,nil; want escape rejection", f, got)
		}
	}
}

// TestManifestReadRejectsEscape:清单分片路径经 .. 指向清单目录外一个真实合法 dump 时,
// Read 必须拒绝(否则会读到目录外文件)。用真实存在的文件确保不是 os.Open 未命中造成的假通过。
func TestManifestReadRejectsEscape(t *testing.T) {
	base := t.TempDir()
	mdir := filepath.Join(base, "dump")
	if err := os.Mkdir(mdir, 0o755); err != nil {
		t.Fatal(err)
	}
	// 在清单目录之外(base 下)放一个真实合法的单文件 dump。
	evil := filepath.Join(base, "evil.jsonl")
	ef, err := os.Create(evil)
	if err != nil {
		t.Fatal(err)
	}
	ew := NewSingleWriterFile(ef, "amqp")
	_ = ew.WriteMeta()
	_ = ew.Write(model.Message{Body: []byte("secret")})
	_ = ew.Close()

	// 清单在 mdir,分片以 ../evil.jsonl 逃到 base。
	mpath := filepath.Join(mdir, "orders.mqdump.json")
	mf, err := os.Create(mpath)
	if err != nil {
		t.Fatal(err)
	}
	man := Manifest{FormatVersion: FormatVersion, Driver: "amqp", Parts: []Part{{File: "../evil.jsonl", Count: 1}}, Total: 1}
	if err := WriteManifest(mf, man); err != nil {
		t.Fatal(err)
	}
	mf.Close()

	r, err := OpenReader(mpath)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	defer r.Close()
	if _, _, err := r.Read(); err == nil {
		t.Error("Read() followed ../ escape to a real file, want rejection")
	}
}

// TestManifestRelocation:整套(清单+分片)搬到新目录后,分片按清单所在目录相对解析仍读通。
func TestManifestRelocation(t *testing.T) {
	src := t.TempDir()
	stem := filepath.Join(src, "orders")
	w, _ := NewSplitWriter(stem, "amqp", 2)
	for i := 0; i < 3; i++ { // 2 + 1 → 分片 000/001
		_ = w.Write(model.Message{Body: []byte{byte('a' + i)}})
	}
	_ = w.Close()

	// 把清单与所有分片搬到全新目录(模拟迁移;导出时的源路径不得残留影响)。
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	r, err := OpenReader(filepath.Join(dst, "orders.mqdump.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
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
		t.Errorf("relocated manifest yielded %d msgs, want 3", n)
	}
}
