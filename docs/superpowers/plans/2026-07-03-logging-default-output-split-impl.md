# 日志 / 默认输出 / 文件拆分 实现计划

> **For agentic workers:** 逐任务实现本计划,每任务遵循 TDD(先写失败测试 → 跑失败 → 最小实现 → 跑通过)。步骤用 `- [ ]` 勾选跟踪。**每次改代码前需开发人员确认(见 AGENTS.md)。本计划任何任务、任何步骤都不执行提交动作(不 `git add`、不 `git commit`)——提交由开发人员在计划外决定。**

**Goal:** 给 mq-dump 增加 slog 文件日志、无 `-f` 时默认 `<队列名>.jsonl` 输出、按消息条数拆分导出并用独立清单聚合导入。

**Architecture:** 新增 `dump.Writer`/`dump.Reader` 抽象承载 single/split 与 single/manifest 两态,管道只调 `Write`/`Read`;队列名经可选接口 `mq.Namer` 从驱动取得;日志用 `log/slog` + `slog.SetDefault`,仅 `export`/`import` 装配文件日志。公共接口 `mq.Driver`/`mq.Factory` 与 v1 单文件格式均不破坏。

**Tech Stack:** Go 1.25,`log/slog`(标准库),`github.com/goccy/go-json`,`github.com/alecthomas/kong`,`github.com/rabbitmq/amqp091-go`。

**基线:** master `7c4a567`。设计见 `docs/superpowers/specs/2026-07-03-logging-default-output-split-design.md`。

**隔离:** 按 AGENTS.md,新功能在 `master` 派生的 worktree 内实现:`git worktree add .worktrees/logging-split -b feature/logging-split master`。

---

## 文件结构

**新建:**
- `internal/dump/manifest.go` — `Part`/`Manifest` 结构 + 单行 JSON 读写。
- `internal/dump/writer.go` — `Writer` 接口 + `singleWriter` + `splitWriter`(轮转 + 清单)。
- `internal/dump/reader.go` — `Reader` 接口 + `singleReader` + `manifestReader` + `OpenReader`(按内容判定)。
- `internal/command/logging.go` — `setupLogger`。
- 各上述文件对应 `_test.go`。

**修改:**
- `config/common.go` — 加 `LogLevel`/`LogFile`/`SplitCount`。
- `mq/driver.go` — 加 `Namer` 可选接口。
- `mq/amqp/amqp.go` — 实现 `DumpName()`;连接/循环加 slog。
- `internal/dump/codec.go` — 保留 `Encoder`/`Decoder`,被 Writer/Reader 复用(不改签名)。
- `internal/command/io.go` — `openDumpWriter`→`resolveExportWriter`;`openDumpReader`→改用 `dump.OpenReader`。
- `internal/command/export.go` / `import.go` — 改用 resolver + `dump.Writer/Reader`;装配日志。
- `internal/pipeline/run.go` — `Export`/`Import` 签名改用 `dump.Writer`/`dump.Reader`;加进度日志。

---

## Task 1: 配置字段 LogLevel / LogFile / SplitCount

**Files:**
- Modify: `config/common.go`
- Test: `config/common_test.go`

- [ ] **Step 1: 写失败测试** — 追加到 `config/common_test.go`

```go
func TestCommonSplitAndLogDefaults(t *testing.T) {
	// 零值应表达"关闭拆分";LogLevel/LogFile 的默认来自 kong tag,零值结构体这里只验证字段存在与语义。
	var c Common
	if c.SplitCount != 0 {
		t.Errorf("SplitCount zero value = %d, want 0 (off)", c.SplitCount)
	}
	if c.LogLevel != "" || c.LogFile != "" {
		t.Errorf("log fields zero value not empty: level=%q file=%q", c.LogLevel, c.LogFile)
	}
}
```

- [ ] **Step 2: 跑失败** — `go test ./config/ -run TestCommonSplitAndLogDefaults`；预期编译失败(字段未定义)。

- [ ] **Step 3: 加字段** — `config/common.go` 的 `Common` 结构体末尾(`Concurrency` 行后)插入:

```go
	LogLevel    string        `default:"info" help:"日志级别 debug|info|warn|error"`
	LogFile     string        `default:"mq-dump.log" help:"日志文件路径;\"-\"=stderr"`
	SplitCount  int           `help:"导出按消息条数拆分;每 N 条一个文件;0=不拆"`
```

- [ ] **Step 4: 跑通过** — `go test ./config/ -run TestCommonSplitAndLogDefaults`;预期 PASS。

## Task 2: Manifest 结构与单行 JSON 读写

**Files:**
- Create: `internal/dump/manifest.go`
- Test: `internal/dump/manifest_test.go`

- [ ] **Step 1: 写失败测试** — `internal/dump/manifest_test.go`

```go
package dump

import (
	"bytes"
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	m := Manifest{
		FormatVersion: FormatVersion,
		Driver:        "amqp",
		CreatedAt:     "2026-07-03T00:00:00Z",
		Parts:         []Part{{File: "orders-000.jsonl", Count: 3}, {File: "orders-001.jsonl", Count: 2}},
		Total:         5,
	}
	var buf bytes.Buffer
	if err := WriteManifest(&buf, m); err != nil {
		t.Fatal(err)
	}
	if bytes.Count(buf.Bytes(), []byte("\n")) != 1 { // 单行 JSON + 结尾换行
		t.Errorf("manifest not single-line: %q", buf.String())
	}
	got, err := ReadManifest(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 5 || len(got.Parts) != 2 || got.Parts[1].File != "orders-001.jsonl" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}
```

- [ ] **Step 2: 跑失败** — `go test ./internal/dump/ -run TestManifestRoundTrip`;预期编译失败(未定义)。

- [ ] **Step 3: 实现** — `internal/dump/manifest.go`

```go
package dump

import (
	"fmt"
	"io"

	"github.com/goccy/go-json"
)

// Part 是清单中一个分片的记录。
type Part struct {
	File  string `json:"file"`  // 相对清单所在目录
	Count int    `json:"count"` // 该分片消息条数
}

// Manifest 是拆分导出的独立索引(单行 JSON)。
type Manifest struct {
	FormatVersion int    `json:"format_version"`
	Driver        string `json:"driver"`
	CreatedAt     string `json:"created_at"`
	Parts         []Part `json:"parts"`
	Total         int    `json:"total"`
}

// WriteManifest 把清单写为单行 JSON(带结尾换行)。
func WriteManifest(w io.Writer, m Manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if _, err := w.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

// ReadManifest 解析单行 JSON 清单。
func ReadManifest(r io.Reader) (Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("parse manifest: %w", err)
	}
	return m, nil
}
```

- [ ] **Step 4: 跑通过** — `go test ./internal/dump/ -run TestManifestRoundTrip`;预期 PASS。

## Task 3: dump.Writer — singleWriter

**Files:**
- Create: `internal/dump/writer.go`
- Test: `internal/dump/writer_test.go`

- [ ] **Step 1: 写失败测试** — `internal/dump/writer_test.go`

```go
package dump

import (
	"bytes"
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
```

- [ ] **Step 2: 跑失败** — `go test ./internal/dump/ -run TestSingleWriter`;预期编译失败。

- [ ] **Step 3: 实现** — `internal/dump/writer.go`(先只 single;split 在 Task 4)

```go
package dump

import (
	"io"

	"github.com/RandalTeng/mq-dump/model"
)

// Writer 是导出写目标:先 WriteMeta 一次,再逐条 Write,最后 Close。
type Writer interface {
	WriteMeta() error
	Write(m model.Message) error
	Close() error
}

// singleWriter 写单文件:首行 meta + 逐条消息(v1 格式)。
type singleWriter struct {
	enc *Encoder
	c   io.Closer // 底层文件;stdout 传 nil
}

// NewSingleWriter 创建写 w 的单文件 Writer;driver 用于 meta 头。
// c 为底层可关闭资源(文件);若 w 不需关闭(stdout)传 nil。
func NewSingleWriter(w io.Writer, driver string) *singleWriter {
	return &singleWriter{enc: NewEncoder(w, driver)}
}

func (s *singleWriter) WriteMeta() error            { return s.enc.WriteMeta() }
func (s *singleWriter) Write(m model.Message) error { return s.enc.Write(m) }
func (s *singleWriter) Close() error {
	if s.c != nil {
		return s.c.Close()
	}
	return nil
}
```

- [ ] **Step 4: 跑通过** — `go test ./internal/dump/ -run TestSingleWriter`;预期 PASS。

## Task 4: dump.Writer — splitWriter(轮转 + 清单)

**Files:**
- Modify: `internal/dump/writer.go`
- Test: `internal/dump/writer_test.go`

- [ ] **Step 1: 写失败测试** — 追加到 `internal/dump/writer_test.go`

```go
import (
	"os"
	"path/filepath"
)

func TestSplitWriterRotatesAndManifests(t *testing.T) {
	dir := t.TempDir()
	stem := filepath.Join(dir, "orders")
	w, err := NewSplitWriter(stem, "amqp", 2) // 每 2 条一分片
	if err != nil {
		t.Fatal(err)
	}
	_ = w.WriteMeta() // split 下为 no-op
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
	// 分片为纯数据:首行即消息,无 meta。
	b, _ := os.ReadFile(filepath.Join(dir, m.Parts[0].File))
	if bytes.Contains(b, []byte("format_version")) {
		t.Errorf("part must not carry meta:\n%s", b)
	}
}
```

- [ ] **Step 2: 跑失败** — `go test ./internal/dump/ -run TestSplitWriter`;预期编译失败。

- [ ] **Step 3: 实现** — 追加到 `internal/dump/writer.go`

```go
import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// splitWriter 按条数轮转纯数据分片,并维护独立清单。
type splitWriter struct {
	stem   string // 基路径(不含扩展名),如 /out/orders
	driver string
	limit  int // 每分片条数上限(>0)

	cur     *os.File
	curEnc  *Encoder
	curN    int    // 当前分片已写条数
	curFile string // 当前分片文件名(basename)
	parts   []Part
	total   int
}

// NewSplitWriter 创建按 limit 条轮转的拆分 Writer;stem 为基路径(不含扩展名)。
func NewSplitWriter(stem, driver string, limit int) (*splitWriter, error) {
	sw := &splitWriter{stem: stem, driver: driver, limit: limit}
	if err := sw.rotate(); err != nil {
		return nil, err
	}
	return sw, nil
}

// WriteMeta 对拆分为 no-op:meta 落在清单里。
func (s *splitWriter) WriteMeta() error { return nil }

func (s *splitWriter) Write(m model.Message) error {
	if s.curN >= s.limit {
		if err := s.closeCurrent(); err != nil {
			return err
		}
		if err := s.rotate(); err != nil {
			return err
		}
	}
	if err := s.curEnc.Write(m); err != nil {
		return err
	}
	s.curN++
	s.total++
	return nil
}

func (s *splitWriter) Close() error {
	if err := s.closeCurrent(); err != nil {
		return err
	}
	return s.writeManifest()
}

// rotate 打开下一个分片文件(纯数据,无 meta 头)。
func (s *splitWriter) rotate() error {
	name := fmt.Sprintf("%s-%03d.jsonl", filepath.Base(s.stem), len(s.parts))
	full := filepath.Join(filepath.Dir(s.stem), name)
	f, err := os.Create(full)
	if err != nil {
		return fmt.Errorf("create part %q: %w", full, err)
	}
	s.cur, s.curEnc, s.curN, s.curFile = f, NewEncoder(f, s.driver), 0, name
	return nil
}

// closeCurrent 关闭当前分片、登记到 parts、并重写清单(崩溃安全)。
func (s *splitWriter) closeCurrent() error {
	if s.cur == nil {
		return nil
	}
	if err := s.cur.Close(); err != nil {
		return fmt.Errorf("close part: %w", err)
	}
	if s.curN > 0 {
		s.parts = append(s.parts, Part{File: s.curFile, Count: s.curN})
	} else {
		_ = os.Remove(filepath.Join(filepath.Dir(s.stem), s.curFile)) // 空分片删除
	}
	s.cur, s.curEnc = nil, nil
	return s.writeManifest()
}

// writeManifest 用当前已完成 parts 重写清单(单行 JSON)。
func (s *splitWriter) writeManifest() error {
	man := Manifest{
		FormatVersion: FormatVersion,
		Driver:        s.driver,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		Parts:         s.parts,
		Total:         s.total,
	}
	f, err := os.Create(s.stem + ".mqdump.json")
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	defer f.Close()
	return WriteManifest(f, man)
}
```

- [ ] **Step 4: 跑通过** — `go test ./internal/dump/ -run TestSplitWriter`;预期 PASS。

## Task 5: dump.Reader — single/manifest + 按内容判定 OpenReader

**Files:**
- Create: `internal/dump/reader.go`
- Test: `internal/dump/reader_test.go`

- [ ] **Step 1: 写失败测试** — `internal/dump/reader_test.go`

```go
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
	w := NewSingleWriter(f, "amqp")
	_ = w.WriteMeta()
	_ = w.Write(model.Message{Body: []byte("x")})
	_ = f.Close()

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
```

- [ ] **Step 2: 跑失败** — `go test ./internal/dump/ -run TestOpenReader`;预期编译失败。

- [ ] **Step 3: 实现** — `internal/dump/reader.go`

```go
package dump

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/goccy/go-json"

	"github.com/RandalTeng/mq-dump/model"
)

// Reader 是导入读源:先 Meta 校验驱动,再逐条 Read 到 ok=false。
type Reader interface {
	Meta() (Meta, error)
	Read() (model.Message, bool, error)
	Close() error
}

// OpenReader 打开 path 并按首行内容判定模式:
// 首行含 "parts" → 清单模式(path 即清单);否则单文件(内嵌 meta)。
// path == "-" 或 "" → stdin 单文件模式。
func OpenReader(path string) (Reader, error) {
	if path == "" || path == "-" {
		return newSingleReader(io.NopCloser(os.Stdin), "", nil), nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	head, err := peekFirstLine(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	if isManifest(head) {
		f.Close()
		return newManifestReader(path)
	}
	return newSingleReader(f, "", f), nil
}

func peekFirstLine(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	if !sc.Scan() {
		return nil, sc.Err()
	}
	return append([]byte(nil), sc.Bytes()...), nil
}

func isManifest(head []byte) bool {
	var probe struct {
		Parts []Part `json:"parts"`
	}
	if err := json.Unmarshal(head, &probe); err != nil {
		return false
	}
	return probe.Parts != nil
}

// singleReader 复用 Decoder 读内嵌 meta 的单文件。
type singleReader struct {
	dec *Decoder
	c   io.Closer
}

func newSingleReader(r io.Reader, _ string, c io.Closer) *singleReader {
	return &singleReader{dec: NewDecoder(r), c: c}
}

func (s *singleReader) Meta() (Meta, error) { return s.dec.ReadMeta() }
func (s *singleReader) Read() (model.Message, bool, error) { return s.dec.Read() }
func (s *singleReader) Close() error {
	if s.c != nil {
		return s.c.Close()
	}
	return nil
}

// manifestReader 顺序拼接清单里的纯数据分片。
type manifestReader struct {
	man     Manifest
	dir     string
	idx     int
	cur     *os.File
	curDec  *Decoder
}

func newManifestReader(path string) (*manifestReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	man, err := ReadManifest(f)
	if err != nil {
		return nil, err
	}
	return &manifestReader{man: man, dir: filepath.Dir(path)}, nil
}

// Meta 由清单头构造(无 parts 的 Meta)。
func (m *manifestReader) Meta() (Meta, error) {
	return Meta{FormatVersion: m.man.FormatVersion, Driver: m.man.Driver, CreatedAt: m.man.CreatedAt}, nil
}

func (m *manifestReader) Read() (model.Message, bool, error) {
	for {
		if m.curDec == nil {
			if m.idx >= len(m.man.Parts) {
				return model.Message{}, false, nil
			}
			p := filepath.Join(m.dir, m.man.Parts[m.idx].File)
			f, err := os.Open(p)
			if err != nil {
				return model.Message{}, false, fmt.Errorf("open part %q: %w", p, err)
			}
			m.cur, m.curDec, m.idx = f, NewDecoder(f), m.idx+1
		}
		msg, ok, err := m.curDec.Read()
		if err != nil {
			return model.Message{}, false, err
		}
		if !ok { // 当前分片读尽,切下一片
			m.cur.Close()
			m.cur, m.curDec = nil, nil
			continue
		}
		return msg, true, nil
	}
}

func (m *manifestReader) Close() error {
	if m.cur != nil {
		return m.cur.Close()
	}
	return nil
}
```

> 注:`singleReader` 的分片是纯数据(无 meta 头),故 `manifestReader` 用裸 `Decoder.Read` 逐行读——但 `NewDecoder` 首次 `Read` 从第一行开始,分片无 meta 头正好逐条即消息。**这要求 `Decoder.Read` 不预设首行为 meta**(现状即如此:`ReadMeta` 与 `Read` 分离,分片模式只调 `Read`)。

- [ ] **Step 4: 跑通过** — `go test ./internal/dump/ -run TestOpenReader`;预期 PASS。

## Task 6: pipeline 改用 dump.Writer/Reader + 进度日志

**Files:**
- Modify: `internal/pipeline/run.go`
- Test: `internal/pipeline/run_test.go`

- [ ] **Step 1: 改测试** — `internal/pipeline/run_test.go` 三处调用改为传 `dump.Writer`/`dump.Reader`。替换 `TestExportThenImportRoundTrip` / `TestExportCountLimit` / `TestImportDriverMismatch` 的 Export/Import 调用:

```go
func TestExportThenImportRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	src := &fakeDriver{out: []model.Message{{Body: []byte("m1")}, {Body: []byte("m2")}}}
	w := dump.NewSingleWriter(&buf, "amqp")
	if err := Export(context.Background(), w, 0, src); err != nil {
		t.Fatal(err)
	}
	dst := &fakeDriver{}
	r := dump.NewSingleReaderForTest(bytes.NewReader(buf.Bytes()))
	if err := Import(context.Background(), r, "amqp", dst); err != nil {
		t.Fatal(err)
	}
	if len(dst.in) != 2 || string(dst.in[0].Body) != "m1" {
		t.Errorf("imported %d: %+v", len(dst.in), dst.in)
	}
}

func TestExportCountLimit(t *testing.T) {
	var buf bytes.Buffer
	src := &fakeDriver{out: []model.Message{{Body: []byte("1")}, {Body: []byte("2")}, {Body: []byte("3")}}}
	w := dump.NewSingleWriter(&buf, "amqp")
	if err := Export(context.Background(), w, 2, src); err != nil {
		t.Fatal(err)
	}
	dec := dump.NewDecoder(bytes.NewReader(buf.Bytes()))
	_, _ = dec.ReadMeta()
	var n int
	for {
		if _, ok, _ := dec.Read(); !ok {
			break
		}
		n++
	}
	if n != 2 {
		t.Errorf("wrote %d msgs, want 2 (count limit)", n)
	}
}

func TestImportDriverMismatch(t *testing.T) {
	var buf bytes.Buffer
	w := dump.NewSingleWriter(&buf, "amqp")
	_ = Export(context.Background(), w, 0, &fakeDriver{})
	r := dump.NewSingleReaderForTest(bytes.NewReader(buf.Bytes()))
	if err := Import(context.Background(), r, "kafka", &fakeDriver{}); err == nil {
		t.Error("driver mismatch should error")
	}
}
```

- [ ] **Step 2: 加测试辅助导出** — 在 `internal/dump/reader.go` 末尾加(供 pipeline 测试从 `io.Reader` 造 single Reader):

```go
// NewSingleReaderForTest 从 io.Reader 构造单文件 Reader(测试/内存流用)。
func NewSingleReaderForTest(r io.Reader) Reader { return newSingleReader(r, "", nil) }
```

- [ ] **Step 3: 跑失败** — `go test ./internal/pipeline/`;预期编译失败(Export/Import 签名不符)。

- [ ] **Step 4: 改实现** — `internal/pipeline/run.go` 全量替换为:

```go
// Package pipeline 连接 dump codec 与驱动,编排导入/导出。
package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"github.com/RandalTeng/mq-dump/internal/dump"
	"github.com/RandalTeng/mq-dump/model"
	"github.com/RandalTeng/mq-dump/mq"
)

const progressEvery = 10000

// Export 让驱动产出消息,逐条经并发安全 emit 写入 Writer;count>0 时到量即止。
func Export(ctx context.Context, w dump.Writer, count int, d mq.Driver) error {
	if err := w.WriteMeta(); err != nil {
		return err
	}
	var mu sync.Mutex
	var n int
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	emit := func(m model.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if count > 0 && n >= count {
			cancel()
			return nil
		}
		if err := w.Write(m); err != nil {
			return err
		}
		n++
		if n%progressEvery == 0 {
			slog.Info("export progress", "done", n)
		}
		if count > 0 && n >= count {
			cancel()
		}
		return nil
	}
	if err := d.Export(ctx, emit); err != nil {
		return err
	}
	slog.Info("export done", "total", n)
	return nil
}

// Import 从 Reader 读取(校验 meta.driver),经并发安全 next 交给驱动 publish。
func Import(ctx context.Context, r dump.Reader, driver string, d mq.Driver) error {
	meta, err := r.Meta()
	if err != nil {
		return err
	}
	if err := meta.CheckDriver(driver); err != nil {
		return err
	}
	var mu sync.Mutex
	var n int
	next := func() (model.Message, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		m, ok, err := r.Read()
		if ok {
			n++
			if n%progressEvery == 0 {
				slog.Info("import progress", "done", n)
			}
		}
		return m, ok, err
	}
	if err := d.Import(ctx, next); err != nil {
		return err
	}
	slog.Info("import done", "total", n)
	return nil
}
```

- [ ] **Step 5: 跑通过** — `go test ./internal/dump/ ./internal/pipeline/`;预期 PASS。

## Task 7: mq.Namer 可选接口

**Files:**
- Modify: `mq/driver.go`
- Test: `mq/driver_test.go`(新建)

- [ ] **Step 1: 写失败测试** — `mq/driver_test.go`

```go
package mq

import "testing"

type namerStub struct{ Driver }

func (namerStub) DumpName() string { return "orders" }

func TestNamerAssertion(t *testing.T) {
	var d Driver = namerStub{}
	n, ok := d.(Namer)
	if !ok || n.DumpName() != "orders" {
		t.Fatalf("Namer assertion failed: ok=%v", ok)
	}
}
```

- [ ] **Step 2: 跑失败** — `go test ./mq/ -run TestNamer`;预期编译失败(Namer 未定义)。

- [ ] **Step 3: 实现** — `mq/driver.go` 末尾追加:

```go
// Namer 由驱动可选实现,给出无 -f 时的默认 dump 基名(不含扩展名)。
// 通用层类型断言使用;外部驱动可不实现。
type Namer interface {
	DumpName() string
}
```

- [ ] **Step 4: 跑通过** — `go test ./mq/ -run TestNamer`;预期 PASS。

## Task 8: setupLogger

**Files:**
- Create: `internal/command/logging.go`
- Test: `internal/command/logging_test.go`

- [ ] **Step 1: 写失败测试** — `internal/command/logging_test.go`

```go
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
```

- [ ] **Step 2: 跑失败** — `go test ./internal/command/ -run TestSetupLogger`;预期编译失败。

- [ ] **Step 3: 实现** — `internal/command/logging.go`

```go
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
```

> `slog.Level.UnmarshalText` 接受 `debug|info|warn|error`(大小写不敏感),非法值返回 error —— 正好满足校验需求,无需自己写 map。

- [ ] **Step 4: 跑通过** — `go test ./internal/command/ -run TestSetupLogger`;预期 PASS。

## Task 9: command 输出/输入 resolver + export/import 装配

**Files:**
- Modify: `internal/command/io.go`, `internal/command/export.go`, `internal/command/import.go`
- Test: `internal/command/io_test.go`(新建)

- [ ] **Step 1: 写失败测试** — `internal/command/io_test.go`

```go
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
	os.Chdir(dir)
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
```

- [ ] **Step 2: 跑失败** — `go test ./internal/command/ -run TestResolveExportWriter`;预期编译失败。

- [ ] **Step 3: 实现 io.go** — 全量替换 `internal/command/io.go`:

```go
package command

import (
	"fmt"
	"os"
	"strings"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/internal/dump"
	"github.com/RandalTeng/mq-dump/mq"
)

// resolveExportWriter 依据 -f / 拆分 / 驱动 Namer 决定导出 Writer。
//   -f -            → stdout(single;拆分下报错)
//   -f <path>       → 该路径(去 .jsonl 得基名)
//   -f 空 + Namer   → <DumpName()>
//   -f 空 + 无 Namer→ 报错
func resolveExportWriter(c *config.Common, driver string, namer mq.Namer) (dump.Writer, error) {
	if c.DumpFile == "-" {
		if c.SplitCount > 0 {
			return nil, fmt.Errorf("拆分导出不支持写 stdout")
		}
		return dump.NewSingleWriter(os.Stdout, driver), nil
	}
	stem := strings.TrimSuffix(c.DumpFile, ".jsonl")
	if stem == "" {
		if namer == nil || namer.DumpName() == "" {
			return nil, fmt.Errorf("未指定 -f 且驱动 %q 无默认名,请用 -f 指定输出", driver)
		}
		stem = namer.DumpName()
	}
	if c.SplitCount > 0 {
		return dump.NewSplitWriter(stem, driver, c.SplitCount)
	}
	f, err := os.Create(stem + ".jsonl")
	if err != nil {
		return nil, err
	}
	return dump.NewSingleWriterFile(f, driver), nil
}
```

- [ ] **Step 4: 加 file-backed single 构造** — 在 `internal/dump/writer.go` 加(设置 closer,使文件在 Close 时关闭):

```go
// NewSingleWriterFile 创建写文件 f 的单文件 Writer,Close 时关闭 f。
func NewSingleWriterFile(f *os.File, driver string) *singleWriter {
	return &singleWriter{enc: NewEncoder(f, driver), c: f}
}
```

- [ ] **Step 5: 改 export.go** — 全量替换 `internal/command/export.go`:

```go
package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/internal/pipeline"
	"github.com/RandalTeng/mq-dump/mq"
)

// ExportCmd 导出:通用 flag + 驱动 YAML。
type ExportCmd struct{}

// Run 执行导出。
func (c *ExportCmd) Run(common *config.Common) error {
	closer, err := setupLogger(common.LogLevel, common.LogFile)
	if err != nil {
		return err
	}
	defer closer()

	f, ok := mq.Get(common.Driver)
	if !ok {
		return fmt.Errorf("unknown driver %q", common.Driver)
	}
	cfg := f.NewConfig()
	if err := config.LoadDriverYAML(common.Config, cfg); err != nil {
		return err
	}
	d, err := f.Open(*common, cfg)
	if err != nil {
		return err
	}
	defer d.Close()

	namer, _ := d.(mq.Namer)
	w, err := resolveExportWriter(common, common.Driver, namer)
	if err != nil {
		return err
	}
	defer w.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return pipeline.Export(ctx, w, common.Count, d)
}
```

- [ ] **Step 6: 改 import.go** — 全量替换 `internal/command/import.go`:

```go
package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/internal/dump"
	"github.com/RandalTeng/mq-dump/internal/pipeline"
	"github.com/RandalTeng/mq-dump/mq"
)

// ImportCmd 导入:通用 flag + 驱动 YAML。
type ImportCmd struct{}

// Run 执行导入。
func (c *ImportCmd) Run(common *config.Common) error {
	closer, err := setupLogger(common.LogLevel, common.LogFile)
	if err != nil {
		return err
	}
	defer closer()

	f, ok := mq.Get(common.Driver)
	if !ok {
		return fmt.Errorf("unknown driver %q", common.Driver)
	}
	cfg := f.NewConfig()
	if err := config.LoadDriverYAML(common.Config, cfg); err != nil {
		return err
	}
	d, err := f.Open(*common, cfg)
	if err != nil {
		return err
	}
	defer d.Close()

	r, err := dump.OpenReader(common.DumpFile)
	if err != nil {
		return err
	}
	defer r.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return pipeline.Import(ctx, r, common.Driver, d)
}
```

- [ ] **Step 7: 跑通过** — `go test ./internal/command/ ./internal/pipeline/ ./internal/dump/`;预期 PASS。

## Task 10: AMQP DumpName + slog 埋点

**Files:**
- Modify: `mq/amqp/amqp.go`
- Test: `mq/amqp/amqp_test.go`

- [ ] **Step 1: 写失败测试** — 追加到 `mq/amqp/amqp_test.go`

```go
func TestDriverDumpName(t *testing.T) {
	d := &Driver{cfg: Config{Export: ExportConfig{Queue: "orders"}}}
	if d.DumpName() != "orders" {
		t.Errorf("DumpName = %q, want orders", d.DumpName())
	}
}
```

- [ ] **Step 2: 跑失败** — `go test ./mq/amqp/ -run TestDriverDumpName`;预期编译失败。

- [ ] **Step 3: 实现 DumpName** — `mq/amqp/amqp.go` 的 `Close` 方法后追加:

```go
// DumpName 实现 mq.Namer:无 -f 时默认 dump 基名 = 导出队列名。
func (d *Driver) DumpName() string { return d.cfg.Export.Queue }
```

- [ ] **Step 4: 加连接日志** — `factory.Open` 内 `amqp.Dial` 成功后(`return &Driver{...}` 前)插入:

```go
	slog.Info("amqp connected", "addr", sanitizeURI(ac.Connection.URI))
```

并加脱敏辅助(文件末尾)与 import:

```go
import "net/url"

// sanitizeURI 仅保留 host:port,剥除账号口令。
func sanitizeURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return "?"
	}
	return u.Host
}
```

> 在文件顶部 import 块加 `"log/slog"` 与 `"net/url"`。

- [ ] **Step 5: 加导出起始日志** — `Export` 内 `ch.Consume` 成功后插入:

```go
	slog.Info("amqp export start", "queue", d.cfg.Export.Queue, "prefetch", prefetch, "ack", d.cfg.Export.Ack)
```

- [ ] **Step 6: 跑通过** — `go test ./mq/amqp/`;预期 PASS(含既有测试)。

## Task 11: 集成测试补拆分回环

**Files:**
- Modify: `mq/amqp/integration_test.go`

- [ ] **Step 1: 加拆分回环用例** — 追加到 `mq/amqp/integration_test.go`(build tag `integration`)。用真实 RabbitMQ:导出到临时目录、`--split-count` 拆分、再从清单导入,断言条数一致。

```go
func TestIntegrationSplitRoundTrip(t *testing.T) {
	// 前置:与既有集成测试相同的连接/建队列/灌 N 条消息。
	// 1) 用 NewSplitWriter(stem, "amqp", k) 经 pipeline.Export 导出;
	// 2) 用 dump.OpenReader(stem+".mqdump.json") 经 pipeline.Import 导入到另一队列;
	// 3) 断言导入条数 == N,且分片数 == ceil(N/k)。
	// 具体连接样板复用本文件既有 helper(见文件内 setup)。
	t.Skip("样板复用既有集成 helper;联网 RabbitMQ 环境下取消 Skip 并补全")
}
```

> 说明:此步依赖既有集成测试的连接/建队列 helper。实现时读 `mq/amqp/integration_test.go` 现有 setup,套用同样的 dial/queue-declare/publish,再跑上面三步;去掉 `t.Skip`。默认 `go test ./...` 不含 `integration` tag,不受影响。

- [ ] **Step 2: 编译校验** — `go build -tags integration ./mq/amqp/`;预期成功(不实跑,除非有 RabbitMQ)。

## 最终校验(全部任务后)

- [ ] `go build ./...` — 成功
- [ ] `go test ./...` — 全 PASS
- [ ] `go vet ./...` — 干净
- [ ] `gofmt -l .` — 空(用 `goimports -local github.com/RandalTeng/mq-dump` 整理 import 分组)
- [ ] 手动冒烟:`go run ./cmd/mq-dump export --help`(不建 mq-dump.log);构造小 dump 验证 single↔split 判定

## 自检(计划对照 spec)

- 日志:Task 1(字段)/8(setupLogger)/6(进度)/10(连接+起始)+ 事件级别 ✓
- 默认输出:Task 7(Namer)/9(resolver 默认名)/10(AMQP DumpName)✓
- 拆分+清单:Task 2(Manifest)/3-4(Writer)/5(Reader 自动判定)/9(split+stdout 报错、清单创建)✓
- 多队列扩展性:接口未变(Task 7 仅新增可选接口),清单 parts 为对象数组 ✓
- 类型一致性:`Writer`/`Reader`/`NewSingleWriter`/`NewSingleWriterFile`/`NewSplitWriter`/`OpenReader`/`NewSingleReaderForTest`/`Manifest`/`Part`/`setupLogger`/`DumpName` 跨任务一致 ✓
