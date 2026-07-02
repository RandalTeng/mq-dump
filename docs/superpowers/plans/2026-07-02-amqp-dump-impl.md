# amqp-dump 实现计划

> **For agentic workers:** 逐任务实现本计划,每个任务遵循 TDD(先写失败测试 → 跑失败 → 最小实现 → 跑通过 → 提交)。步骤用 `- [ ]` 勾选跟踪。**每次改代码前、每次提交前均需开发人员确认(见 AGENTS.md)。**

**Goal:** 实现 amqp-dump —— 一个可插拔驱动的消息队列导入/导出 CLI,v1 支持 AMQP。

**Architecture:** 通用层(命令 + 编排 + dump 编解码 + 注册表)对驱动私有内容一无所知;AMQP 驱动实现 `Driver` 接口,自带 typed Config/Properties;命令按文件拆分在 `internal/command`;详见 `docs/superpowers/specs/2026-07-02-amqp-dump-design.md`。

**Tech Stack:** Go 1.22+,kong(CLI),amqp091-go(AMQP),errgroup(并发),yaml.v3(驱动配置),stdlib testing。

**Module path:** `github.com/randal/mq-dump`(计划正文示例中出现的 `amqp-dump` import 前缀,实现时一律替换为 `mq-dump`;二进制名仍为 `amqp-dump`)。

**约定:**
- 依赖用固定版本(`go get pkg@vX.Y.Z`),不用开放区间。
- 子代理只实现,不跑项目级 build/lint;由主执行者在阶段末统一验证。
- 每个任务末尾的提交按 AGENTS.md 中文 commit 规范,且需开发人员确认。

---

## Task 1: 初始化模块与目录骨架

**Files:**
- Create: `go.mod`
- Create: `.golangci.yml`

- [ ] **Step 1: 初始化 go module**

Run: `go mod init github.com/randal/amqp-dump && go mod edit -go=1.22`
Expected: 生成 `go.mod`,module 行为 `github.com/randal/amqp-dump`。

- [ ] **Step 2: 拉取固定版本依赖**

Run:
```bash
go get github.com/alecthomas/kong@v1.6.0
go get github.com/rabbitmq/amqp091-go@v1.10.0
go get golang.org/x/sync@v0.10.0
go get gopkg.in/yaml.v3@v3.0.1
```
Expected: `go.mod`/`go.sum` 记录以上依赖。(版本以拉取时最新稳定为准,记录到 go.mod 即可。)

- [ ] **Step 3: 添加 golangci 配置**

```yaml
run:
  timeout: 3m
linters:
  enable:
    - gofmt
    - govet
    - errcheck
    - staticcheck
    - ineffassign
    - unused
```

- [ ] **Step 4: 验证构建基线**

Run: `go build ./... && go vet ./...`
Expected: 无包时通过(或 "no Go files" — 属正常,后续任务补齐)。

- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 2: 通用消息信封 model

**Files:**
- Create: `internal/model/message.go`
- Test: `internal/model/message_test.go`

- [ ] **Step 1: 写失败测试**

```go
package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMessageJSONRoundTrip(t *testing.T) {
	in := Message{
		Body:       []byte("hello"),
		Timestamp:  time.Unix(1700000000, 0).UTC(),
		Properties: json.RawMessage(`{"exchange":"a"}`),
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Message
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(out.Body) != "hello" {
		t.Errorf("body = %q, want hello", out.Body)
	}
	if !out.Timestamp.Equal(in.Timestamp) {
		t.Errorf("ts = %v, want %v", out.Timestamp, in.Timestamp)
	}
	if string(out.Properties) != `{"exchange":"a"}` {
		t.Errorf("props = %s", out.Properties)
	}
}
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/model/` Expected: 编译失败(Message 未定义)。

- [ ] **Step 3: 最小实现**

```go
// Package model 定义驱动无关的通用消息信封。
package model

import (
	"encoding/json"
	"time"
)

// Message 是 dump 文件中一条消息的通用信封,通用层不解析 Properties。
type Message struct {
	Body       []byte          `json:"body"`
	Timestamp  time.Time       `json:"timestamp,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/model/` Expected: PASS。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 3: dump meta 头

**Files:**
- Create: `internal/dump/meta.go`
- Test: `internal/dump/meta_test.go`

- [ ] **Step 1: 写失败测试**

```go
package dump

import (
	"encoding/json"
	"testing"
)

func TestMetaRoundTrip(t *testing.T) {
	m := Meta{FormatVersion: 1, Driver: "amqp", CreatedAt: "2026-07-02T12:00:00Z"}
	b, _ := json.Marshal(m)
	var got Meta
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got != m {
		t.Errorf("got %+v want %+v", got, m)
	}
}

func TestMetaCheckDriver(t *testing.T) {
	m := Meta{FormatVersion: 1, Driver: "amqp"}
	if err := m.CheckDriver("amqp"); err != nil {
		t.Errorf("same driver should pass: %v", err)
	}
	if err := m.CheckDriver("kafka"); err == nil {
		t.Error("mismatched driver should error")
	}
}
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/dump/` Expected: 编译失败。

- [ ] **Step 3: 最小实现**

```go
// Package dump 处理 dump 文件的 JSONL 编解码与 meta 头。
package dump

import (
	"fmt"
	"time"
)

// FormatVersion 是当前 dump 格式版本。
const FormatVersion = 1

// Meta 是 dump 文件首行的头记录。
type Meta struct {
	FormatVersion int    `json:"format_version"`
	Driver        string `json:"driver"`
	CreatedAt     string `json:"created_at"`
}

// NewMeta 用当前时间构造给定驱动的 meta 头。
func NewMeta(driver string) Meta {
	return Meta{FormatVersion: FormatVersion, Driver: driver, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
}

// CheckDriver 校验 meta 声明的驱动与期望一致。
func (m Meta) CheckDriver(want string) error {
	if m.Driver != want {
		return fmt.Errorf("dump driver %q != requested %q", m.Driver, want)
	}
	return nil
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/dump/` Expected: PASS。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 4: JSONL 编解码器(codec)

**Files:**
- Create: `internal/dump/codec.go`
- Test: `internal/dump/codec_test.go`

- [ ] **Step 1: 写失败测试**

```go
package dump

import (
	"bytes"
	"testing"

	"github.com/randal/amqp-dump/internal/model"
)

func TestEncoderDecoderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, "amqp")
	if err := enc.WriteMeta(); err != nil {
		t.Fatal(err)
	}
	msgs := []model.Message{{Body: []byte("m1")}, {Body: []byte("m2")}}
	for _, m := range msgs {
		if err := enc.Write(m); err != nil {
			t.Fatal(err)
		}
	}

	dec := NewDecoder(bytes.NewReader(buf.Bytes()))
	meta, err := dec.ReadMeta()
	if err != nil {
		t.Fatal(err)
	}
	if meta.Driver != "amqp" {
		t.Errorf("meta driver = %q", meta.Driver)
	}
	var got []model.Message
	for {
		m, ok, err := dec.Read()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, m)
	}
	if len(got) != 2 || string(got[0].Body) != "m1" || string(got[1].Body) != "m2" {
		t.Errorf("got %d msgs: %+v", len(got), got)
	}
}

func TestDecoderMissingMeta(t *testing.T) {
	dec := NewDecoder(bytes.NewReader([]byte(`{"body":"eA=="}` + "\n")))
	if _, err := dec.ReadMeta(); err == nil {
		t.Error("first line without format_version should error")
	}
}
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/dump/` Expected: 编译失败(Encoder/Decoder 未定义)。

- [ ] **Step 3: 最小实现**

```go
package dump

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"github.com/randal/amqp-dump/internal/model"
)

// Encoder 把 meta 头与消息逐行写为 JSONL。
type Encoder struct {
	enc    *json.Encoder
	driver string
}

// NewEncoder 创建写入 w 的编码器,driver 用于 meta 头。
func NewEncoder(w io.Writer, driver string) *Encoder {
	return &Encoder{enc: json.NewEncoder(w), driver: driver}
}

// WriteMeta 写入首行 meta 头,必须在任何 Write 之前调用一次。
func (e *Encoder) WriteMeta() error { return e.enc.Encode(NewMeta(e.driver)) }

// Write 追加一条消息(一行 JSON)。
func (e *Encoder) Write(m model.Message) error { return e.enc.Encode(m) }

// Decoder 读取 JSONL dump:先 ReadMeta 再循环 Read。
type Decoder struct {
	sc *bufio.Scanner
}

// NewDecoder 创建读取 r 的解码器。
func NewDecoder(r io.Reader) *Decoder {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	return &Decoder{sc: sc}
}

// ReadMeta 读取并校验首行 meta 头。
func (d *Decoder) ReadMeta() (Meta, error) {
	if !d.sc.Scan() {
		if err := d.sc.Err(); err != nil {
			return Meta{}, err
		}
		return Meta{}, fmt.Errorf("empty dump: missing meta header")
	}
	var m Meta
	if err := json.Unmarshal(d.sc.Bytes(), &m); err != nil {
		return Meta{}, fmt.Errorf("parse meta header: %w", err)
	}
	if m.FormatVersion == 0 {
		return Meta{}, fmt.Errorf("first line is not a valid meta header")
	}
	return m, nil
}

// Read 读取下一条消息;ok=false 表示到达流末尾。
func (d *Decoder) Read() (model.Message, bool, error) {
	if !d.sc.Scan() {
		return model.Message{}, false, d.sc.Err()
	}
	var m model.Message
	if err := json.Unmarshal(d.sc.Bytes(), &m); err != nil {
		return model.Message{}, false, fmt.Errorf("parse message: %w", err)
	}
	return m, true, nil
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/dump/` Expected: PASS。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 5: 通用配置 Common + Workers()

**Files:**
- Create: `internal/config/common.go`
- Test: `internal/config/common_test.go`

- [ ] **Step 1: 写失败测试**

```go
package config

import (
	"runtime"
	"testing"
)

func TestWorkers(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{0, runtime.NumCPU()},
		{1, 1},
		{-3, 1},
		{8, 8},
	}
	for _, c := range cases {
		got := Common{Concurrency: c.in}.Workers()
		if got != c.want {
			t.Errorf("Workers(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/config/` Expected: 编译失败。

- [ ] **Step 3: 最小实现**

```go
// Package config 定义通用配置与驱动配置加载。
package config

import (
	"runtime"
	"time"
)

// Common 是所有命令共享的通用配置,来自 kong flag。
type Common struct {
	Driver      string        `short:"d" required:"" help:"消息队列驱动 (amqp)"`
	Config      string        `short:"c" type:"existingfile" help:"驱动私有配置 YAML 路径"`
	DumpFile    string        `short:"f" help:"dump 文件路径;\"-\" = stdin/stdout"`
	Count       int           `short:"n" help:"导出条数上限;0 = 不限"`
	Timeout     time.Duration `short:"t" help:"导出空闲超时"`
	Concurrency int           `short:"j" default:"1" help:"导入 worker 数;0 = CPU 核心数"`
}

// Workers 解析并发度:0 → NumCPU;<1 → 1;否则原值。
func (c Common) Workers() int {
	if c.Concurrency == 0 {
		return runtime.NumCPU()
	}
	if c.Concurrency < 1 {
		return 1
	}
	return c.Concurrency
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/config/` Expected: PASS。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 6: 驱动 YAML 加载 helper

**Files:**
- Create: `internal/config/driver.go`
- Test: `internal/config/driver_test.go`
- Test fixture: `internal/config/testdata/amqp.yaml`

- [ ] **Step 1: 写失败测试**

```go
package config

import "testing"

type fakeCfg struct {
	Connection struct {
		URI string `yaml:"uri"`
	} `yaml:"connection"`
}

func TestLoadDriverYAML(t *testing.T) {
	var c fakeCfg
	if err := LoadDriverYAML("testdata/amqp.yaml", &c); err != nil {
		t.Fatal(err)
	}
	if c.Connection.URI != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("uri = %q", c.Connection.URI)
	}
}

func TestLoadDriverYAMLMissingPath(t *testing.T) {
	var c fakeCfg
	if err := LoadDriverYAML("", &c); err == nil {
		t.Error("empty path should error")
	}
}
```

fixture `internal/config/testdata/amqp.yaml`:
```yaml
connection:
  uri: amqp://guest:guest@localhost:5672/
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/config/` Expected: 编译失败(LoadDriverYAML 未定义)。

- [ ] **Step 3: 最小实现**

```go
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadDriverYAML 把 path 指向的 YAML 解析进驱动私有配置 dst。
func LoadDriverYAML(path string, dst any) error {
	if path == "" {
		return fmt.Errorf("--config is required for this command")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("parse config %q: %w", path, err)
	}
	return nil
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/config/` Expected: PASS。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 7: 驱动注册表与接口

**Files:**
- Create: `internal/mq/driver.go`
- Create: `internal/mq/registry.go`
- Test: `internal/mq/registry_test.go`

- [ ] **Step 1: 写失败测试**

```go
package mq

import (
	"context"
	"testing"

	"github.com/randal/amqp-dump/internal/config"
	"github.com/randal/amqp-dump/internal/model"
)

type stubFactory struct{}

func (stubFactory) NewConfig() any          { return &struct{}{} }
func (stubFactory) ConfigTemplate() string  { return "stub: {}" }
func (stubFactory) Open(config.Common, any) (Driver, error) { return stubDriver{}, nil }

type stubDriver struct{}

func (stubDriver) Export(context.Context, func(model.Message) error) error            { return nil }
func (stubDriver) Import(context.Context, func() (model.Message, bool, error)) error  { return nil }
func (stubDriver) Close() error                                                       { return nil }

func TestRegisterAndGet(t *testing.T) {
	Register("stub", stubFactory{})
	if _, ok := Get("stub"); !ok {
		t.Error("registered driver not found")
	}
	if _, ok := Get("nope"); ok {
		t.Error("unknown driver should not be found")
	}
	found := false
	for _, n := range Names() {
		if n == "stub" {
			found = true
		}
	}
	if !found {
		t.Error("Names() missing stub")
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("duplicate Register should panic")
		}
	}()
	Register("dup", stubFactory{})
	Register("dup", stubFactory{})
}
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/mq/` Expected: 编译失败。

- [ ] **Step 3: 最小实现**

`internal/mq/driver.go`:
```go
// Package mq 定义驱动接口、工厂与注册表。
package mq

import (
	"context"

	"github.com/randal/amqp-dump/internal/config"
	"github.com/randal/amqp-dump/internal/model"
)

// Driver 是一个消息队列驱动的导入/导出能力。
// emit / next 均由框架保证并发安全,驱动可从多 goroutine 调用。
type Driver interface {
	// Export 逐条产出消息;emit 返回 nil 才表示已落盘(ack-after-persist)。
	Export(ctx context.Context, emit func(model.Message) error) error
	// Import 逐条消费消息;next 返回 ok=false 表示流结束。
	Import(ctx context.Context, next func() (model.Message, bool, error)) error
	Close() error
}

// Factory 构造驱动并交出其私有配置模型/模板。
type Factory interface {
	NewConfig() any
	ConfigTemplate() string
	Open(c config.Common, cfg any) (Driver, error)
}
```

`internal/mq/registry.go`:
```go
package mq

import (
	"fmt"
	"sort"
)

var registry = map[string]Factory{}

// Register 注册一个驱动;重复名 panic(编程错误)。
func Register(name string, f Factory) {
	if _, dup := registry[name]; dup {
		panic(fmt.Sprintf("mq: driver %q already registered", name))
	}
	registry[name] = f
}

// Get 按名取驱动工厂。
func Get(name string) (Factory, bool) {
	f, ok := registry[name]
	return f, ok
}

// Names 返回已注册驱动名(排序)。
func Names() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/mq/` Expected: PASS。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 8: AMQP Properties + 转换函数

**Files:**
- Create: `internal/mq/amqp/properties.go`
- Test: `internal/mq/amqp/properties_test.go`

纯函数 `deliveryToMessage` / `messageToPublishing` 是最高价值测试点(不需 broker)。

- [ ] **Step 1: 写失败测试**

```go
package amqp

import (
	"encoding/json"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDeliveryToMessageRoundTrip(t *testing.T) {
	d := amqp.Delivery{
		Exchange:      "a",
		RoutingKey:    "k",
		ContentType:   "text/plain",
		DeliveryMode:  2,
		CorrelationId: "c1",
		Body:          []byte("hi"),
		Headers:       amqp.Table{"x": int32(7)},
	}
	m := deliveryToMessage(d)
	if string(m.Body) != "hi" {
		t.Fatalf("body = %q", m.Body)
	}
	var p Properties
	if err := json.Unmarshal(m.Properties, &p); err != nil {
		t.Fatal(err)
	}
	if p.Exchange != "a" || p.RoutingKey != "k" || p.ContentType != "text/plain" || p.DeliveryMode != 2 || p.CorrelationID != "c1" {
		t.Errorf("props = %+v", p)
	}
	if p.AMQPHeaders["x"] != int32(7) {
		t.Errorf("headers = %+v", p.AMQPHeaders)
	}
}

func TestMessageToPublishing(t *testing.T) {
	p := Properties{ContentType: "application/json", DeliveryMode: 2, CorrelationID: "c1", AMQPHeaders: amqp.Table{"x": int32(7)}}
	pb, _ := json.Marshal(p)
	pub := messageToPublishing(model.Message{Body: []byte("hi"), Properties: pb})
	if pub.ContentType != "application/json" || pub.DeliveryMode != 2 || pub.CorrelationId != "c1" {
		t.Errorf("pub = %+v", pub)
	}
	if string(pub.Body) != "hi" || pub.Headers["x"] != int32(7) {
		t.Errorf("pub body/headers = %+v", pub)
	}
}
```

(测试需 `import "github.com/randal/amqp-dump/internal/model"`。)

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/mq/amqp/` Expected: 编译失败。

- [ ] **Step 3: 最小实现**

```go
package amqp

import (
	"encoding/json"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/randal/amqp-dump/internal/model"
)

// Properties 是 AMQP 驱动私有的消息属性(含原始路由),序列化进 Message.Properties。
type Properties struct {
	Exchange      string     `json:"exchange,omitempty"`
	RoutingKey    string     `json:"routing_key,omitempty"`
	ContentType   string     `json:"content_type,omitempty"`
	DeliveryMode  uint8      `json:"delivery_mode,omitempty"`
	CorrelationID string     `json:"correlation_id,omitempty"`
	Priority      uint8      `json:"priority,omitempty"`
	Expiration    string     `json:"expiration,omitempty"`
	MessageID     string     `json:"message_id,omitempty"`
	Type          string     `json:"type,omitempty"`
	AMQPHeaders   amqp.Table `json:"amqp_headers,omitempty"`
}

// deliveryToMessage 把 broker 投递转换为通用信封 + AMQP Properties。
func deliveryToMessage(d amqp.Delivery) model.Message {
	p := Properties{
		Exchange: d.Exchange, RoutingKey: d.RoutingKey,
		ContentType: d.ContentType, DeliveryMode: d.DeliveryMode,
		CorrelationID: d.CorrelationId, Priority: d.Priority,
		Expiration: d.Expiration, MessageID: d.MessageId, Type: d.Type,
		AMQPHeaders: d.Headers,
	}
	raw, _ := json.Marshal(p)
	ts := d.Timestamp
	if ts.IsZero() {
		ts = time.Time{}
	}
	return model.Message{Body: d.Body, Timestamp: ts, Properties: raw}
}

// messageToPublishing 从通用信封重建 amqp.Publishing(不含路由目标,由 target() 决定)。
func messageToPublishing(m model.Message) amqp.Publishing {
	var p Properties
	_ = json.Unmarshal(m.Properties, &p)
	return amqp.Publishing{
		ContentType: p.ContentType, DeliveryMode: p.DeliveryMode,
		CorrelationId: p.CorrelationID, Priority: p.Priority,
		Expiration: p.Expiration, MessageId: p.MessageID, Type: p.Type,
		Headers: p.AMQPHeaders, Body: m.Body, Timestamp: m.Timestamp,
	}
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/mq/amqp/` Expected: PASS。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 9: AMQP Config + 路由覆盖 target()

**Files:**
- Create: `internal/mq/amqp/config.go`
- Test: `internal/mq/amqp/config_test.go`

- [ ] **Step 1: 写失败测试**

```go
package amqp

import (
	"encoding/json"
	"testing"

	"github.com/randal/amqp-dump/internal/model"
)

func msgWithRoute(ex, key string) model.Message {
	p, _ := json.Marshal(Properties{Exchange: ex, RoutingKey: key})
	return model.Message{Properties: p}
}

func TestTargetFallbackToOriginal(t *testing.T) {
	d := &Driver{cfg: Config{}}
	ex, key := d.target(msgWithRoute("a", "orig"))
	if ex != "a" || key != "orig" {
		t.Errorf("fallback = %q/%q, want a/orig", ex, key)
	}
}

func TestTargetOverride(t *testing.T) {
	d := &Driver{cfg: Config{Import: ImportConfig{Exchange: "b", RoutingKey: "B1"}}}
	ex, key := d.target(msgWithRoute("a", "orig"))
	if ex != "b" || key != "B1" {
		t.Errorf("override = %q/%q, want b/B1", ex, key)
	}
}

func TestTargetPartialOverride(t *testing.T) {
	d := &Driver{cfg: Config{Import: ImportConfig{Exchange: "b"}}}
	ex, key := d.target(msgWithRoute("a", "orig"))
	if ex != "b" || key != "orig" {
		t.Errorf("partial = %q/%q, want b/orig", ex, key)
	}
}
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/mq/amqp/` Expected: 编译失败(Config/Driver/target 未定义)。

- [ ] **Step 3: 最小实现**

`internal/mq/amqp/config.go`:
```go
package amqp

import (
	"encoding/json"

	"github.com/randal/amqp-dump/internal/model"
)

// Config 是 AMQP 驱动私有配置(仅从 --config YAML 解析)。
type Config struct {
	Connection ConnConfig   `yaml:"connection"`
	Export     ExportConfig `yaml:"export"`
	Import     ImportConfig `yaml:"import"`
}
type ConnConfig struct {
	URI string `yaml:"uri"`
}
type ExportConfig struct {
	Queue    string `yaml:"queue"`
	Ack      bool   `yaml:"ack"`
	Prefetch int    `yaml:"prefetch"`
}
type ImportConfig struct {
	Exchange   string `yaml:"exchange"`
	RoutingKey string `yaml:"routing_key"`
	Persistent bool   `yaml:"persistent"`
	Confirm    bool   `yaml:"confirm"`
	Mandatory  bool   `yaml:"mandatory"`
}

// target 决定导入目标:import 配置非空则覆盖,否则用消息原始路由。
func (d *Driver) target(m model.Message) (exchange, key string) {
	var p Properties
	_ = json.Unmarshal(m.Properties, &p)
	exchange, key = p.Exchange, p.RoutingKey
	if d.cfg.Import.Exchange != "" {
		exchange = d.cfg.Import.Exchange
	}
	if d.cfg.Import.RoutingKey != "" {
		key = d.cfg.Import.RoutingKey
	}
	return
}
```

注:`Driver` 结构体在 Task 10 定义;本任务先声明其字段 `cfg Config`(实现时把 Driver 结构体与 target 一并落地,或先放最小 `type Driver struct{ cfg Config }`,Task 10 再补全其余字段)。

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/mq/amqp/` Expected: PASS。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 10: AMQP 驱动实现(Export/Import/Open/Close + 注册)

**Files:**
- Create: `internal/mq/amqp/amqp.go`
- Create: `internal/mq/amqp/template.yaml`
- 补全 `internal/mq/amqp/config.go` 中的 `Driver` 结构体(如 Task 9 用了最小占位)

此任务含 broker I/O,单元测试不覆盖 Export/Import 网络部分(留给 Task 13 集成测试);此处仅保证编译 + 注册 + 模板解析回环。

- [ ] **Step 1: 写失败测试(模板解析回环 + 注册)**

`internal/mq/amqp/amqp_test.go`:
```go
package amqp

import (
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/randal/amqp-dump/internal/mq"
)

func TestRegistered(t *testing.T) {
	if _, ok := mq.Get("amqp"); !ok {
		t.Error("amqp driver not registered")
	}
}

func TestConfigTemplateParsesBack(t *testing.T) {
	f, _ := mq.Get("amqp")
	var c Config
	if err := yaml.Unmarshal([]byte(f.ConfigTemplate()), &c); err != nil {
		t.Fatalf("template not parseable: %v", err)
	}
	if c.Connection.URI == "" {
		t.Error("template should include a connection.uri example")
	}
}
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/mq/amqp/` Expected: 编译失败 / 未注册。

- [ ] **Step 3: 最小实现**

`internal/mq/amqp/template.yaml`:
```yaml
# AMQP 驱动配置。amqp-dump export/import --config <this-file>
connection:
  # RabbitMQ 连接串
  uri: amqp://guest:guest@localhost:5672/
export:
  queue: orders        # 源队列
  ack: false           # true = 破坏性 drain(读完即移除);false = 非破坏(requeue)
  prefetch: 100        # consume 预取量
import:
  exchange: ""         # 覆盖目标 exchange;空 = 用消息原始 exchange
  routing_key: ""      # 覆盖目标 routing key;空 = 用原始 routing key
  persistent: true     # delivery-mode=2
  confirm: true        # publisher confirms
  mandatory: false
```

`internal/mq/amqp/amqp.go`:
```go
package amqp

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"

	"github.com/randal/amqp-dump/internal/config"
	"github.com/randal/amqp-dump/internal/model"
	"github.com/randal/amqp-dump/internal/mq"
)

//go:embed template.yaml
var configTemplate string

func init() { mq.Register("amqp", factory{}) }

type factory struct{}

func (factory) NewConfig() any         { return &Config{} }
func (factory) ConfigTemplate() string { return configTemplate }
func (factory) Open(c config.Common, cfg any) (mq.Driver, error) {
	ac, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("amqp: bad config type %T", cfg)
	}
	if ac.Connection.URI == "" {
		return nil, fmt.Errorf("amqp: connection.uri is required")
	}
	conn, err := amqp.Dial(ac.Connection.URI)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	return &Driver{conn: conn, cfg: *ac, common: c}, nil
}

// Driver 是 AMQP 驱动实例。
type Driver struct {
	conn   *amqp.Connection
	cfg    Config
	common config.Common
}

func (d *Driver) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

// Export 从队列 consume,逐条 emit;emit 成功后按配置 ack/nack。
func (d *Driver) Export(ctx context.Context, emit func(model.Message) error) error {
	ch, err := d.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()
	prefetch := d.cfg.Export.Prefetch
	if prefetch <= 0 {
		prefetch = 100
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	deliveries, err := ch.Consume(d.cfg.Export.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %q: %w", d.cfg.Export.Queue, err)
	}
	idle := d.common.Timeout
	var count int
	for {
		var timeout <-chan time.Time
		if idle > 0 {
			t := time.NewTimer(idle)
			timeout = t.C
			defer t.Stop()
		}
		select {
		case <-ctx.Done():
			return nil // requeue 未 ack 的消息(channel 关闭时自动)
		case <-timeout:
			return nil // 空闲超时,正常结束
		case dv, ok := <-deliveries:
			if !ok {
				return nil
			}
			if err := emit(deliveryToMessage(dv)); err != nil {
				_ = dv.Nack(false, true) // 落盘失败 → requeue,不丢
				return err
			}
			if d.cfg.Export.Ack {
				_ = dv.Ack(false) // 破坏性 drain
			} else {
				_ = dv.Nack(false, true) // 非破坏:requeue
			}
			count++
			if d.common.Count > 0 && count >= d.common.Count {
				return nil
			}
		}
	}
}

// Import 用 Workers() 个 worker 并发 publish,每 worker 独立 channel。
func (d *Driver) Import(ctx context.Context, next func() (model.Message, bool, error)) error {
	n := d.common.Workers()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < n; i++ {
		g.Go(func() error {
			ch, err := d.conn.Channel()
			if err != nil {
				return fmt.Errorf("open channel: %w", err)
			}
			defer ch.Close()
			if d.cfg.Import.Confirm {
				if err := ch.Confirm(false); err != nil {
					return fmt.Errorf("confirm mode: %w", err)
				}
			}
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				m, ok, err := next()
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
				if err := d.publish(ch, m); err != nil {
					return err
				}
			}
		})
	}
	return g.Wait()
}

func (d *Driver) publish(ch *amqp.Channel, m model.Message) error {
	ex, key := d.target(m)
	pub := messageToPublishing(m)
	if d.cfg.Import.Persistent {
		pub.DeliveryMode = amqp.Persistent
	}
	confirm, err := ch.PublishWithDeferredConfirm(ex, key, d.cfg.Import.Mandatory, false, pub)
	if err != nil {
		return fmt.Errorf("publish to %q/%q: %w", ex, key, err)
	}
	if d.cfg.Import.Confirm && confirm != nil {
		if ok := confirm.Wait(); !ok {
			return fmt.Errorf("publish to %q/%q nacked by broker", ex, key)
		}
	}
	return nil
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/mq/amqp/` Expected: PASS(注册 + 模板回环;Export/Import 网络行为留集成测试)。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 11: pipeline 编排(Export/Import)

**Files:**
- Create: `internal/pipeline/run.go`
- Test: `internal/pipeline/run_test.go`

pipeline 连接 codec 与 driver:Export 提供并发安全 emit(内部加锁写盘 + meta 头 + count 限制);Import 提供并发安全 next(读盘)。用 fake driver 测试(不需 broker)。

- [ ] **Step 1: 写失败测试**

```go
package pipeline

import (
	"bytes"
	"context"
	"testing"

	"github.com/randal/amqp-dump/internal/dump"
	"github.com/randal/amqp-dump/internal/model"
)

// fakeDriver:Export 产出 N 条,Import 收集。
type fakeDriver struct {
	out []model.Message
	in  []model.Message
}

func (f *fakeDriver) Export(ctx context.Context, emit func(model.Message) error) error {
	for _, m := range f.out {
		if err := emit(m); err != nil {
			return err
		}
	}
	return nil
}
func (f *fakeDriver) Import(ctx context.Context, next func() (model.Message, bool, error)) error {
	for {
		m, ok, err := next()
		if err != nil || !ok {
			return err
		}
		f.in = append(f.in, m)
	}
}
func (f *fakeDriver) Close() error { return nil }

func TestExportThenImportRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	src := &fakeDriver{out: []model.Message{{Body: []byte("m1")}, {Body: []byte("m2")}}}
	if err := Export(context.Background(), &buf, "amqp", 0, src); err != nil {
		t.Fatal(err)
	}

	dst := &fakeDriver{}
	if err := Import(context.Background(), bytes.NewReader(buf.Bytes()), "amqp", dst); err != nil {
		t.Fatal(err)
	}
	if len(dst.in) != 2 || string(dst.in[0].Body) != "m1" {
		t.Errorf("imported %d: %+v", len(dst.in), dst.in)
	}
}

func TestExportCountLimit(t *testing.T) {
	var buf bytes.Buffer
	src := &fakeDriver{out: []model.Message{{Body: []byte("1")}, {Body: []byte("2")}, {Body: []byte("3")}}}
	if err := Export(context.Background(), &buf, "amqp", 2, src); err != nil {
		t.Fatal(err)
	}
	dec := dump.NewDecoder(bytes.NewReader(buf.Bytes()))
	_, _ = dec.ReadMeta()
	var n int
	for {
		_, ok, _ := dec.Read()
		if !ok {
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
	_ = Export(context.Background(), &buf, "amqp", 0, &fakeDriver{})
	if err := Import(context.Background(), bytes.NewReader(buf.Bytes()), "kafka", &fakeDriver{}); err == nil {
		t.Error("driver mismatch should error")
	}
}
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/pipeline/` Expected: 编译失败。

- [ ] **Step 3: 最小实现**

```go
// Package pipeline 连接 dump codec 与驱动,编排导入/导出。
package pipeline

import (
	"context"
	"io"
	"sync"

	"github.com/randal/amqp-dump/internal/dump"
	"github.com/randal/amqp-dump/internal/model"
	"github.com/randal/amqp-dump/internal/mq"
)

// Export 让驱动产出消息,逐条经并发安全 emit 写入 JSONL(带 meta 头);count>0 时到量即止。
func Export(ctx context.Context, w io.Writer, driver string, count int, d mq.Driver) error {
	enc := dump.NewEncoder(w, driver)
	if err := enc.WriteMeta(); err != nil {
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
		if err := enc.Write(m); err != nil {
			return err
		}
		n++
		if count > 0 && n >= count {
			cancel()
		}
		return nil
	}
	return d.Export(ctx, emit)
}

// Import 从 JSONL 读取(校验 meta.driver),经并发安全 next 交给驱动 publish。
func Import(ctx context.Context, r io.Reader, driver string, d mq.Driver) error {
	dec := dump.NewDecoder(r)
	meta, err := dec.ReadMeta()
	if err != nil {
		return err
	}
	if err := meta.CheckDriver(driver); err != nil {
		return err
	}
	var mu sync.Mutex
	next := func() (model.Message, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return dec.Read()
	}
	return d.Import(ctx, next)
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/pipeline/` Expected: PASS。
- [ ] **Step 5: 提交**(需开发人员确认 message)

---

## Task 12: CLI 命令 + main

**Files:**
- Create: `internal/command/cli.go`
- Create: `internal/command/export.go`
- Create: `internal/command/import.go`
- Create: `internal/command/init.go`
- Create: `internal/command/io.go`(dump 文件/stdin/stdout 打开 helper)
- Create: `cmd/amqp-dump/main.go`
- Test: `internal/command/init_test.go`

- [ ] **Step 1: 写失败测试(init 输出模板)**

```go
package command

import (
	"path/filepath"
	"os"
	"testing"

	_ "github.com/randal/amqp-dump/internal/mq/amqp"
	"github.com/randal/amqp-dump/internal/config"
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
```

- [ ] **Step 2: 跑失败** — Run: `go test ./internal/command/` Expected: 编译失败。

- [ ] **Step 3: 最小实现**

`internal/command/cli.go`:
```go
// Package command 定义 CLI 聚合与各子命令。
package command

import "github.com/randal/amqp-dump/internal/config"

// CLI 是 kong 根聚合结构:嵌入通用 flag + 各子命令。
type CLI struct {
	config.Common
	Export ExportCmd `cmd:"" help:"导出消息到 dump 文件"`
	Import ImportCmd `cmd:"" help:"从 dump 文件导入消息"`
	Init   InitCmd   `cmd:"" name:"init" help:"生成驱动配置模板"`
}
```

`internal/command/io.go`:
```go
package command

import (
	"io"
	"os"
)

// openDumpWriter 打开导出目标:"-" 或空 → stdout。
func openDumpWriter(path string) (io.WriteCloser, error) {
	if path == "" || path == "-" {
		return nopWriteCloser{os.Stdout}, nil
	}
	return os.Create(path)
}

// openDumpReader 打开导入源:"-" 或空 → stdin。
func openDumpReader(path string) (io.ReadCloser, error) {
	if path == "" || path == "-" {
		return io.NopCloser(os.Stdin), nil
	}
	return os.Open(path)
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
```

`internal/command/export.go`:
```go
package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/randal/amqp-dump/internal/config"
	"github.com/randal/amqp-dump/internal/mq"
	"github.com/randal/amqp-dump/internal/pipeline"
)

// ExportCmd 导出:通用 flag + 驱动 YAML。
type ExportCmd struct{}

func (c *ExportCmd) Run(common *config.Common) error {
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

	w, err := openDumpWriter(common.DumpFile)
	if err != nil {
		return err
	}
	defer w.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return pipeline.Export(ctx, w, common.Driver, common.Count, d)
}
```

`internal/command/import.go`:
```go
package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/randal/amqp-dump/internal/config"
	"github.com/randal/amqp-dump/internal/mq"
	"github.com/randal/amqp-dump/internal/pipeline"
)

// ImportCmd 导入:通用 flag + 驱动 YAML。
type ImportCmd struct{}

func (c *ImportCmd) Run(common *config.Common) error {
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

	r, err := openDumpReader(common.DumpFile)
	if err != nil {
		return err
	}
	defer r.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return pipeline.Import(ctx, r, common.Driver, d)
}
```

`internal/command/init.go`:
```go
package command

import (
	"fmt"
	"os"

	"github.com/randal/amqp-dump/internal/config"
	"github.com/randal/amqp-dump/internal/mq"
)

// InitCmd 生成驱动配置模板。
type InitCmd struct {
	Output string `short:"o" help:"模板输出路径;缺省写 stdout"`
}

func (c *InitCmd) Run(common *config.Common) error {
	f, ok := mq.Get(common.Driver)
	if !ok {
		return fmt.Errorf("unknown driver %q", common.Driver)
	}
	tpl := f.ConfigTemplate()
	if c.Output == "" || c.Output == "-" {
		_, err := fmt.Fprint(os.Stdout, tpl)
		return err
	}
	return os.WriteFile(c.Output, []byte(tpl), 0o644)
}
```

`cmd/amqp-dump/main.go`:
```go
// Command amqp-dump 是消息队列导入/导出工具。
package main

import (
	"github.com/alecthomas/kong"

	"github.com/randal/amqp-dump/internal/command"
	_ "github.com/randal/amqp-dump/internal/mq/amqp" // 注册 amqp 驱动
)

func main() {
	var cli command.CLI
	kctx := kong.Parse(&cli, kong.Name("amqp-dump"),
		kong.Description("消息队列导入/导出工具(v1: AMQP)"))
	kctx.FatalIfErrorf(kctx.Run(&cli.Common))
}
```

- [ ] **Step 4: 跑通过** — Run: `go test ./internal/command/ && go build ./cmd/amqp-dump` Expected: PASS + 生成二进制。
- [ ] **Step 5: 手动 smoke test**

Run: `go run ./cmd/amqp-dump init -d amqp` Expected: stdout 打印 AMQP YAML 模板。
Run: `go run ./cmd/amqp-dump --help` Expected: 显示 export/import/init 三命令与通用 flag(含短 flag)。

- [ ] **Step 6: 提交**(需开发人员确认 message)

---

## Task 13: 集成测试(build tag,真实 RabbitMQ)

**Files:**
- Create: `internal/mq/amqp/integration_test.go`(`//go:build integration`)
- Create: `docker-compose.yaml`(RabbitMQ 服务)

- [ ] **Step 1: docker-compose**

```yaml
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports: ["5672:5672", "15672:15672"]
```

- [ ] **Step 2: 集成测试**

```go
//go:build integration

package amqp

// 需环境变量 AMQP_URI(默认 amqp://guest:guest@localhost:5672/)。
// 用例:
// 1. 声明队列 → 播种 N 条 → Export 到 buffer → 断言条数与内容。
// 2. 非破坏导出后队列仍有 N 条;--ack drain 后队列为 0。
// 3. Import 到临时 exchange/queue → 断言收到 N 条;路由覆盖生效。
// 4. Concurrency=0/4 时结果完整。
// (实现时用 amqp091 直接声明/断言,ctx 带超时防挂起。)
```

- [ ] **Step 3: 跑集成测试**

Run: `docker compose up -d && go test -tags integration ./internal/mq/amqp/ && docker compose down`
Expected: PASS(需本机 docker)。若环境无 docker,记录为未验证并说明。

- [ ] **Step 4: 提交**(需开发人员确认 message)

---

## Task 14: 收尾验证

- [ ] **Step 1: 全量单测** — Run: `go test ./...` Expected: PASS(集成测试默认排除)。
- [ ] **Step 2: 静态检查** — Run: `go vet ./... && gofmt -l .` Expected: 无输出。
- [ ] **Step 3: golangci** — Run: `golangci-lint run`(若已安装)Expected: 无 issue。
- [ ] **Step 4: README 快速上手**(可选,若需要)— 记录 init/export/import 用法与 docker-compose。
- [ ] **Step 5: 完成分支收尾** — 按 finishing-a-development-branch 决定 merge/PR(需开发人员确认)。

## Self-Review 覆盖对照

| spec 要点 | 覆盖任务 |
|---|---|
| 通用信封 Message | T2 |
| dump meta 头 + 同驱动校验 | T3, T11 |
| JSONL codec | T4 |
| Common + Workers(0→NumCPU) | T5 |
| 驱动 YAML 加载 | T6 |
| registry + 接口 | T7 |
| AMQP Properties + 转换 | T8 |
| 路由覆盖 target() | T9 |
| AMQP Export/Import(ack-after-persist、并发、confirm) | T10, T13 |
| pipeline 编排 + count 限制 | T11 |
| CLI 子命令拆文件 + 短 flag + main | T12 |
| init 模板 go:embed + 回环 | T10, T12 |
| 集成往返 / 非破坏 vs drain / 并发 | T13 |
