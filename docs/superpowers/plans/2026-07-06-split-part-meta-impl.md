# 拆分分片内嵌 meta 头 实现计划

> **For agentic workers:** 逐任务实现本计划,每任务遵循 TDD(先写失败测试 → 跑失败 → 最小实现 → 跑通过)。步骤用 `- [ ]` 勾选跟踪。**每次改代码前需开发人员确认(见 AGENTS.md)。本计划任何任务、任何步骤都不执行提交动作(不 `git add`、不 `git commit`)——提交由开发人员在计划外决定。**

**Goal:** 在 split 导出模式下给每个分片文件首行写入 meta 头,使单个分片本身即为合法 v1 单文件 dump,可脱离清单独立导入;清单增加 `updated_at`(每次落地刷新),`created_at` 保持导出起始时刻;导入侧把"分片路径相对清单目录解析"从约定升级为强约束(拒绝绝对路径与 `..` 逃逸)。

**Architecture:** split 分片从"纯数据"升级为"内嵌 meta 的单文件"——与 v1 单文件同构。写侧 `splitWriter` 每 rotate 一个分片即调 `Encoder.WriteMeta()` 写入**当时时间**的 meta 头(各分片时间可不同,无需统一);清单 `created_at` 在 `NewSplitWriter` 锁定,`updated_at` 在每次 `writeManifest`(含崩溃安全的逐分片重写)刷新为当时时间。读侧 `manifestReader` 聚合时先消费每个分片的 meta 头,并经 `resolvePart` 把分片相对路径按清单所在目录解析、强制落在该目录内;独立导入路径复用现有 single reader,无需改动(分片首行无 `parts` 字段,`OpenReader` 自动判为单文件)。公共接口签名不变。

**Tech Stack:** Go 1.25,`github.com/goccy/go-json`,标准库 `time`。

**基线:** master。设计见 `docs/superpowers/specs/2026-07-03-logging-default-output-split-design.md`(本计划同步更新其"拆分分片/清单格式"描述)。

**隔离:** 按 AGENTS.md,新功能在 `master` 派生的 worktree 内实现:`git worktree add .worktrees/split-part-meta -b feature/split-part-meta master`。

**格式版本(不升,决策留痕):** `dump.FormatVersion` 保持 `1`。分片升级后其磁盘结构恰好等于 v1 单文件(meta 头 + 逐条消息),"版本 1"仍准确;解码器只对 `FormatVersion==0` 报错、不按具体值分支,升版号无运行时收益且会误标未变的单文件。清单新增 `updated_at` 为加法字段(向前向后兼容),其存在即"新式清单"的可辨识信号。旧式无 meta 分片被新读侧聚合时,`ReadMeta` 读到消息行即显式报错(干净失败,非静默损坏),无需版号守护。

**兼容性(清晰切换):** 本变更后,旧版 split 导出(分片无 meta 头)将不再可经清单聚合导入。仓库未发布,采用干净切换,不保留对旧格式的容忍读取。

---

## 文件结构

**修改:**
- `internal/dump/manifest.go` — `Manifest` 结构增 `UpdatedAt` 字段(置于 `CreatedAt` 后)。
- `internal/dump/writer.go` — `splitWriter` 加 `createdAt` 字段(仅供清单);`NewSplitWriter` 锁定 `createdAt`;`rotate` 给每分片写 meta 头;`writeManifest` 写 `CreatedAt`(锁定)+ `UpdatedAt`(当时)。
- `internal/dump/reader.go` — `manifestReader.Read` 打开分片后先 `ReadMeta()` 消费+校验驱动;新增 `resolvePart` 把分片路径按清单目录相对解析并硬化(拒绝绝对/`..` 逃逸)。
- `docs/superpowers/specs/2026-07-03-logging-default-output-split-design.md` — 更新"分片为纯数据"及清单格式表述。

**测试:**
- `internal/dump/manifest_test.go` — 断言 `UpdatedAt` 往返。
- `internal/dump/writer_test.go` — 翻转 `TestSplitWriterRotatesAndManifests`:分片**含** meta 头;清单 `UpdatedAt` 非空。
- `internal/dump/reader_test.go` — 加 `TestOpenReaderPartStandalone`;`TestOpenReaderManifestMode` 隐式回归 meta 跳过;加 `TestResolvePart`/`TestManifestReadRejectsEscape`/`TestManifestRelocation` 覆盖路径硬化与迁移。

---

## Task 1: Manifest 增加 updated_at 字段

**Files:**
- Modify: `internal/dump/manifest.go:16-23`
- Test: `internal/dump/manifest_test.go`

- [ ] **Step 1: 写失败测试** — `internal/dump/manifest_test.go` 的 `TestManifestRoundTrip` 内 `m := Manifest{...}` 加 `UpdatedAt`,并在末尾断言往返:

```go
	m := Manifest{
		FormatVersion: FormatVersion,
		Driver:        "amqp",
		CreatedAt:     "2026-07-03T00:00:00Z",
		UpdatedAt:     "2026-07-03T00:05:00Z",
		Parts:         []Part{{File: "orders-000.jsonl", Count: 3}, {File: "orders-001.jsonl", Count: 2}},
		Total:         5,
	}
```

在 `if got.Total != 5 ...` 断言块后追加:

```go
	if got.CreatedAt != "2026-07-03T00:00:00Z" || got.UpdatedAt != "2026-07-03T00:05:00Z" {
		t.Errorf("timestamps round-trip mismatch: created=%q updated=%q", got.CreatedAt, got.UpdatedAt)
	}
```

- [ ] **Step 2: 跑失败** — `go test ./internal/dump/ -run TestManifestRoundTrip`;预期编译失败(`Manifest` 无 `UpdatedAt` 字段)。

- [ ] **Step 3: 加字段** — `internal/dump/manifest.go` 的 `Manifest` 结构在 `CreatedAt` 行后插入:

```go
	UpdatedAt     string `json:"updated_at"`
```

- [ ] **Step 4: 跑通过** — `go test ./internal/dump/ -run TestManifestRoundTrip`;预期 PASS。

## Task 2: splitWriter 分片内嵌 meta 头 + 清单双时间戳

**Files:**
- Modify: `internal/dump/writer.go:46-67,96-140`
- Test: `internal/dump/writer_test.go:33-72`

- [ ] **Step 1: 翻转失败测试** — 把 `internal/dump/writer_test.go` 尾部断言(第 67-71 行)改为:

```go
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
```

- [ ] **Step 2: 跑失败** — `go test ./internal/dump/ -run TestSplitWriterRotatesAndManifests`;预期 FAIL(分片当前无 meta 头)。

- [ ] **Step 3: 加 createdAt 字段** — `internal/dump/writer.go` 的 `splitWriter` 结构体在 `limit int` 行后加:

```go
	createdAt string // 导出起始时刻;仅用于清单 created_at(分片各自用写入当时时间)
```

- [ ] **Step 4: 锁定 createdAt** — `NewSplitWriter` 替换为:

```go
// NewSplitWriter 创建按 limit 条轮转的拆分 Writer;stem 为基路径(不含扩展名)。
func NewSplitWriter(stem, driver string, limit int) (*splitWriter, error) {
	sw := &splitWriter{
		stem:      stem,
		driver:    driver,
		limit:     limit,
		createdAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := sw.rotate(); err != nil {
		return nil, err
	}
	return sw, nil
}
```

- [ ] **Step 5: rotate 写分片 meta 头** — `rotate` 替换为:

```go
// rotate 打开下一个分片文件并写入 meta 头(导出当时时间);分片即独立 v1 单文件。
func (s *splitWriter) rotate() error {
	name := fmt.Sprintf("%s-%03d.jsonl", filepath.Base(s.stem), len(s.parts))
	full := filepath.Join(filepath.Dir(s.stem), name)
	f, err := os.Create(full)
	if err != nil {
		return fmt.Errorf("create part %q: %w", full, err)
	}
	enc := NewEncoder(f, s.driver)
	if err := enc.WriteMeta(); err != nil {
		f.Close()
		return fmt.Errorf("write part meta %q: %w", full, err)
	}
	s.cur, s.curEnc, s.curN, s.curFile = f, enc, 0, name
	return nil
}
```

- [ ] **Step 6: writeManifest 双时间戳** — `writeManifest` 的 `man := Manifest{...}` 块替换为:

```go
	man := Manifest{
		FormatVersion: FormatVersion,
		Driver:        s.driver,
		CreatedAt:     s.createdAt,
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339),
		Parts:         s.parts,
		Total:         s.total,
	}
```

- [ ] **Step 7: 跑通过** — `go test ./internal/dump/ -run 'TestSplitWriterRotatesAndManifests|TestManifestRoundTrip'`;预期 PASS。

## Task 3: manifestReader 聚合时消费分片 meta 头

**Files:**
- Modify: `internal/dump/reader.go:116-140`
- Test: `internal/dump/reader_test.go`

- [ ] **Step 1: 加独立分片读取测试** — 追加到 `internal/dump/reader_test.go`

```go
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
```

- [ ] **Step 2: 跑失败** — `go test ./internal/dump/ -run 'TestOpenReaderPartStandalone|TestOpenReaderManifestMode'`;预期 `TestOpenReaderManifestMode` FAIL(聚合把分片 meta 头误读成空消息,计数偏多),`TestOpenReaderPartStandalone` PASS(single reader 本就读 meta)。

- [ ] **Step 3: 聚合跳过分片 meta** — `internal/dump/reader.go` 的 `manifestReader.Read` 替换为:

```go
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
			dec := NewDecoder(f)
			meta, err := dec.ReadMeta() // 分片内嵌 meta:消费首行并校验驱动
			if err != nil {
				f.Close()
				return model.Message{}, false, fmt.Errorf("part %q meta: %w", p, err)
			}
			if meta.Driver != m.man.Driver {
				f.Close()
				return model.Message{}, false, fmt.Errorf("part %q driver %q != manifest %q", p, meta.Driver, m.man.Driver)
			}
			m.cur, m.curDec, m.idx = f, dec, m.idx+1
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
```

- [ ] **Step 4: 跑通过** — `go test ./internal/dump/ -run 'TestOpenReaderPartStandalone|TestOpenReaderManifestMode'`;预期均 PASS。

## Task 4: 更新设计文档

**Files:**
- Modify: `docs/superpowers/specs/2026-07-03-logging-default-output-split-design.md:125-144`

- [ ] **Step 1: 修正分片格式表述** — 把"分片为**纯数据**(无 meta 头)"一句(第 125 行)改为:

```markdown
  - 分片为**独立 v1 单文件**:首行内嵌 meta 头(时间为写入分片当时;`format_version`/`driver` 同清单),其后逐条消息;每写满 N 条 rotate 到下一分片。单个分片可脱离清单直接 `-f <分片>` 导入。
```

- [ ] **Step 2: 清单格式加 updated_at** — 在清单示例 JSON(第 131-142 行)`"created_at"` 行后加 `"updated_at"`,并在示例后补一句:

```markdown
`created_at` 为导出起始时刻;`updated_at` 每次清单落地(逐分片崩溃安全重写与收尾)刷新为当时时间。`format_version` 不因分片内嵌 meta 而升(分片结构恰为 v1 单文件)。
```

- [ ] **Step 3: 校对** — 通读 125-162 行,确认"按内容判定"一节仍自洽(分片首行 `format_version` 无 `parts` → single reader),无需额外改动。

## Task 5: 分片路径按清单目录相对解析并硬化

**Files:**
- Modify: `internal/dump/reader.go`(新增 `resolvePart`;`manifestReader.Read` 改调用)
- Test: `internal/dump/reader_test.go`

**动机:** 分片路径已"相对清单目录"解析(`filepath.Join(m.dir, part.File)`),但清单为不可信输入——手写/篡改的 `../` 或绝对路径可读到清单目录之外。把相对解析从约定升级为强约束。

- [ ] **Step 1: 写失败测试** — `internal/dump/reader_test.go` 追加两测:

`TestResolvePart`(表驱动,直测函数):basename / 子目录 / `./` 放行;`../evil.jsonl`、`sub/../../evil.jsonl`、`..`、真实绝对路径(用 `t.TempDir()` 保证跨平台绝对)拒绝。
`TestManifestReadRejectsEscape`(端到端):清单以 `../evil.jsonl` 指向清单目录外一个**真实合法**单文件 dump(`NewSingleWriterFile` 写 meta+1 条),断言 `Read` 返回错误——用真实文件确保不是 `os.Open` 未命中造成的假通过。
另加 `TestManifestRelocation`:整套(清单+分片)复制到新目录后 `OpenReader` 仍读通全部消息,证明相对解析生效、导出源路径不残留。

- [ ] **Step 2: 跑失败** — `go test ./internal/dump/ -run 'TestResolvePart|TestManifestReadRejectsEscape'`;预期编译失败(`resolvePart` 未定义)。

- [ ] **Step 3: 实现 resolvePart 并接入** — `internal/dump/reader.go` 加 `strings` import;新增:

```go
// resolvePart 把清单里的分片相对路径按清单所在目录 dir 解析为可打开的路径,
// 并强制其落在 dir 之内:拒绝绝对路径与经 .. 逃出 dir 的路径(清单为不可信输入)。
func resolvePart(dir, file string) (string, error) {
	if filepath.IsAbs(file) {
		return "", fmt.Errorf("part path %q must be relative to manifest dir", file)
	}
	joined := filepath.Join(dir, file)
	rel, err := filepath.Rel(dir, joined)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("part path %q escapes manifest dir %q", file, dir)
	}
	return joined, nil
}
```

`manifestReader.Read` 打开分片前把 `p := filepath.Join(m.dir, m.man.Parts[m.idx].File)` 换为:

```go
			p, err := resolvePart(m.dir, m.man.Parts[m.idx].File)
			if err != nil {
				return model.Message{}, false, err
			}
```

- [ ] **Step 4: 跑通过** — `go test ./internal/dump/ -run 'TestResolvePart|TestManifestReadRejectsEscape|TestManifestRelocation'`;预期均 PASS。

## Task 6: 全量验证门禁

**Files:** 无(仅运行)

- [ ] **Step 1: 构建** — `go build ./...`;预期无输出。
- [ ] **Step 2: 静态检查** — `go vet ./...`;预期无输出。
- [ ] **Step 3: 格式** — `gofmt -l .`;预期空输出。
- [ ] **Step 4: 单元测试** — `go test ./...`;预期全部 PASS(integration 用例在 `-tags integration` 后,默认不跑)。

---

## Self-Review

- **Spec 覆盖:** 分片可独立导出/导入 → Task 2(写侧内嵌 meta)+ Task 3(读侧聚合跳过)+ `TestOpenReaderPartStandalone`。清单双时间戳 → Task 1 + Task 2 Step 6。分片路径相对解析硬化 → Task 5(`resolvePart` + `TestResolvePart`/`TestManifestReadRejectsEscape`/`TestManifestRelocation`)。版本号决策 → 头部留痕,不升。✓
- **占位符扫描:** 各步骤均含完整代码/命令与预期输出,无 TBD/TODO。✓
- **类型一致性:** `Manifest.UpdatedAt`、`splitWriter.createdAt`、`Encoder.WriteMeta`、`resolvePart`、`NewSplitWriter`/`OpenReader`/`ReadMeta` 跨任务命名一致;codec.go 无改动。✓
- **兼容性显式声明:** 旧 split 分片不再聚合(干净切换)+ 不升版号(理由留痕),已在头部记明。✓
