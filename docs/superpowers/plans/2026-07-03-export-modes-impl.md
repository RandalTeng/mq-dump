# 导出模式(drain / requeue / peek)实现计划

> **For agentic workers:** 逐任务实现,每任务遵循 TDD(先写失败测试 → 跑失败 → 最小实现 → 跑通过)。步骤用 `- [ ]` 勾选跟踪。**本计划任何任务、任何步骤都不执行提交动作(不 `git add`、不 `git commit`)——提交由开发人员在计划外决定(见 AGENTS.md)。**

**Goal:** 用显式的三态导出模式替换 AMQP 私有的 `Export.Ack bool`,把"读队列后消息去向"从一个隐式布尔升级为有明确终止/保真语义的枚举,并修掉当前 `ack=false` 默认路径的 nack-requeue 死循环。

**Architecture:** 在 `mq/amqp` 内实现,`config.Common` 与 `mq.Driver`/`mq.Factory` 接口均不改(`factory.Open` 已把 `Common` 交给 driver)。`ExportConfig.Ack bool` → `ExportConfig.Mode string`(`drain|requeue|peek`,默认 `drain`)。`Driver.Export` 拆成一个按 `Mode` 分派的 dispatcher + 三个私有实现方法。三模式铁律:**读循环里绝不 requeue,只在特定时机释放**——drain 用 `ack`(删),requeue 用"先 confirm 重发回队尾、再 ack 原件",peek 读满即停后一次性 `nack(requeue)`。

**Tech Stack:** Go 1.25,`github.com/rabbitmq/amqp091-go` v1.12.0,`github.com/goccy/go-json`,`gopkg.in/yaml.v3`。

**基线:** master `7c4a567`。worktree:`.worktrees/export-modes`,分支 `feature/export-modes`。

---

## 三模式语义(实现契约)

| 模式 | ack/nack 时机 | 终止条件 | 破坏性 | hold 峰值 | 保真度 |
|---|---|---|---|---|---|
| `drain`(默认) | emit 落盘后 `ack` | 队列取空(idle timeout)/ `Count` | 是(消息被消费) | prefetch | 全 |
| `requeue` | 先重发副本回队尾+confirm,再 `ack` 原件 | 起始队列深度快照 N / `Count` | 否 | prefetch | rk 变队列名、顺序变 |
| `peek` | 全程不 ack 不 nack,停后一次性 `nack(requeue)` | 读满 N(`Count`/prefetch)/ idle timeout | 否 | ≤ N | 全(仅拿队头 N 条) |

**requeue 关键约束(写进 template 警告):**
- 重发**固定走默认交换机 `""` + routingKey=被导队列名**,只回原队、不扇出;因此投递 routing key 会变成队列名(原始 rk 仍存于 dump 供 import 还原)。
- **强制开启 publisher confirm**,保证"发成功了才 ack",不丢消息。
- **须由用户先停掉该队列的其他消费者**,否则重复消费 + 破坏 FIFO 计数保证。

---

## 文件结构

**修改:**
- `mq/amqp/config.go` — `ExportConfig.Ack bool` → `Mode string`;加模式常量 + 校验/归一化 `resolveMode`。
- `mq/amqp/amqp.go` — `Export` 改为 dispatcher;新增 `exportDrain` / `exportRequeue` / `exportPeek` / `consumeChannel`(共用开 channel+Qos+Consume)/ `republish`。
- `mq/amqp/template.yaml` — `ack` 行 → `mode` 说明 + requeue 三条警告。
- `mq/amqp/config_test.go` — 加 `TestResolveMode`。
- `mq/amqp/integration_test.go` — `TestExportAckDrain`→`TestExportDrain`;`TestExportNonDestructive`→`TestExportRequeue`;新增 `TestExportPeek`;`TestImportRoundTripWithRouting` 改用 `Mode:"drain"`。

---

## Task 1: 配置 Mode 字段 + 校验

**Files:**
- Modify: `mq/amqp/config.go`
- Test: `mq/amqp/config_test.go`

- [ ] **Step 1: 写失败测试** — 追加到 `mq/amqp/config_test.go`

```go
func TestResolveMode(t *testing.T) {
	cases := []struct {
		in      string
		want    ExportMode
		wantErr bool
	}{
		{"", ModeDrain, false},
		{"drain", ModeDrain, false},
		{"requeue", ModeRequeue, false},
		{"peek", ModePeek, false},
		{"bogus", "", true},
	}
	for _, c := range cases {
		got, err := resolveMode(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("resolveMode(%q) err=nil, want error", c.in)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("resolveMode(%q) = %q,%v; want %q,nil", c.in, got, err, c.want)
		}
	}
}
```

- [ ] **Step 2: 跑失败** — `go test ./mq/amqp/ -run TestResolveMode`;预期编译失败(未定义)。

- [ ] **Step 3: 实现** — `mq/amqp/config.go`:把 `ExportConfig` 的 `Ack bool` 行换成 `Mode string`,并加常量 + `resolveMode`。

```go
// ExportMode 是导出模式(消息读出后的去向语义)。
type ExportMode string

const (
	// ModeDrain 破坏性抽干:emit 落盘后 ack,消息被移除;天然读空即止。
	ModeDrain ExportMode = "drain"
	// ModeRequeue 非破坏全导:重发副本回原队(默认交换机+队列名)并 confirm 后再 ack 原件;
	// 须先停该队列其他消费者。投递 routing key 会变为队列名。
	ModeRequeue ExportMode = "requeue"
	// ModePeek 非破坏抽样:仅取队头 N 条,全程不 ack/nack,结束一次性 nack requeue。
	ModePeek ExportMode = "peek"
)

// resolveMode 归一化并校验导出模式;空串视为默认 drain。
func resolveMode(s string) (ExportMode, error) {
	switch ExportMode(s) {
	case "", ModeDrain:
		return ModeDrain, nil
	case ModeRequeue:
		return ModeRequeue, nil
	case ModePeek:
		return ModePeek, nil
	default:
		return "", fmt.Errorf("amqp: unknown export mode %q (want drain|requeue|peek)", s)
	}
}
```

`ExportConfig` 结构改为:

```go
// ExportConfig 是导出参数。
type ExportConfig struct {
	Queue    string `yaml:"queue"`
	Mode     string `yaml:"mode"` // drain(默认) | requeue | peek
	Prefetch int    `yaml:"prefetch"`
}
```

并在文件顶部 import 补 `"fmt"`。

- [ ] **Step 4: 跑通过** — `go test ./mq/amqp/ -run TestResolveMode`;预期 PASS。

---

## Task 2: Export dispatcher + 共用 consume 辅助

**Files:**
- Modify: `mq/amqp/amqp.go`

- [ ] **Step 1: 重写 `Export` 为 dispatcher,并抽出 `consumeChannel`。** 把 `amqp.go` 现有 `Export`(第 56-107 行整段)替换为:

```go
// Export 按 cfg.Export.Mode 分派:drain / requeue / peek。
func (d *Driver) Export(ctx context.Context, emit func(model.Message) error) error {
	mode, err := resolveMode(d.cfg.Export.Mode)
	if err != nil {
		return err
	}
	switch mode {
	case ModeRequeue:
		return d.exportRequeue(ctx, emit)
	case ModePeek:
		return d.exportPeek(ctx, emit)
	default:
		return d.exportDrain(ctx, emit)
	}
}

// prefetch 返回配置的预取量,<=0 时取默认 100。
func (d *Driver) prefetch() int {
	if d.cfg.Export.Prefetch > 0 {
		return d.cfg.Export.Prefetch
	}
	return 100
}

// consumeChannel 开 channel、设 Qos(prefetch)、以手动 ack 方式 consume 源队列。
// 返回 channel(调用方负责 Close)、投递流。
func (d *Driver) consumeChannel(prefetch int) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := d.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("qos: %w", err)
	}
	deliveries, err := ch.Consume(d.cfg.Export.Queue, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("consume %q: %w", d.cfg.Export.Queue, err)
	}
	return ch, deliveries, nil
}
```

- [ ] **Step 2: 编译校验** — `go build ./mq/amqp/`;此时 `exportDrain/exportRequeue/exportPeek` 未定义,预期失败(下一任务补齐)。可跳过单独跑,直接进 Task 3。

---

## Task 3: exportDrain

**Files:**
- Modify: `mq/amqp/amqp.go`

- [ ] **Step 1: 实现 `exportDrain`。** 在 `Export` 分派方法之后追加:

```go
// exportDrain 破坏性抽干:emit 落盘后 ack;队列取空(idle timeout)或到 Count 即止。
func (d *Driver) exportDrain(ctx context.Context, emit func(model.Message) error) error {
	ch, deliveries, err := d.consumeChannel(d.prefetch())
	if err != nil {
		return err
	}
	defer ch.Close()
	idle := d.common.Timeout
	var count int
	timer := newIdleTimer(idle)
	defer timer.stop()
	for {
		timer.reset(idle)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
			return nil
		case dv, ok := <-deliveries:
			if !ok {
				return nil
			}
			if err := emit(deliveryToMessage(dv)); err != nil {
				_ = dv.Nack(false, true)
				return err
			}
			_ = dv.Ack(false)
			count++
			if d.common.Count > 0 && count >= d.common.Count {
				return nil
			}
		}
	}
}
```

- [ ] **Step 2: 加 `idleTimer` 辅助**(避免每轮 `time.NewTimer`+`defer` 堆积;单一可复用计时器)。追加到 `amqp.go` 末尾:

```go
// idleTimer 是可复用的空闲计时器;idle<=0 时 C() 返回 nil(永不触发)。
type idleTimer struct{ t *time.Timer }

func newIdleTimer(idle time.Duration) idleTimer {
	if idle <= 0 {
		return idleTimer{}
	}
	return idleTimer{t: time.NewTimer(idle)}
}

func (i idleTimer) C() <-chan time.Time {
	if i.t == nil {
		return nil
	}
	return i.t.C
}

func (i idleTimer) reset(idle time.Duration) {
	if i.t == nil {
		return
	}
	if !i.t.Stop() {
		select {
		case <-i.t.C:
		default:
		}
	}
	i.t.Reset(idle)
}

func (i idleTimer) stop() {
	if i.t != nil {
		i.t.Stop()
	}
}
```

- [ ] **Step 3: 编译校验** — `go build ./mq/amqp/`;requeue/peek 仍未定义,继续 Task 4。

---

## Task 4: exportRequeue + republish

**Files:**
- Modify: `mq/amqp/amqp.go`

- [ ] **Step 1: 实现 `exportRequeue` 与 `republish`。** 追加到 `amqp.go`:

```go
// exportRequeue 非破坏全导:起始读队列深度快照 N;逐条 emit 落盘后,
// 先把副本重发回原队(默认交换机+队列名)并等 confirm,再 ack 原件;导满 N 或 Count 即止。
// 须先停该队列其他消费者(见 template.yaml 警告)。
func (d *Driver) exportRequeue(ctx context.Context, emit func(model.Message) error) error {
	ch, deliveries, err := d.consumeChannel(d.prefetch())
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("confirm mode: %w", err)
	}
	// 起始快照:队列现有条数,作为终止上界,防止导入自己刚重发的副本。
	q, err := ch.QueueDeclarePassive(d.cfg.Export.Queue, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("inspect queue %q: %w", d.cfg.Export.Queue, err)
	}
	snapshot := q.Messages
	limit := snapshot
	if d.common.Count > 0 && d.common.Count < limit {
		limit = d.common.Count
	}
	if limit == 0 {
		return nil
	}
	idle := d.common.Timeout
	var count int
	timer := newIdleTimer(idle)
	defer timer.stop()
	for {
		timer.reset(idle)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
			return nil
		case dv, ok := <-deliveries:
			if !ok {
				return nil
			}
			m := deliveryToMessage(dv)
			if err := emit(m); err != nil {
				_ = dv.Nack(false, true)
				return err
			}
			// 先重发副本回原队并 confirm,成功后再 ack 原件(顺序不可反,防丢)。
			if err := d.republish(ctx, ch, m); err != nil {
				_ = dv.Nack(false, true)
				return err
			}
			_ = dv.Ack(false)
			count++
			if count >= limit {
				return nil
			}
		}
	}
}

// republish 把消息重发回被导出队列本身:默认交换机 "" + routingKey=队列名,
// 只投该队列、不经原 exchange、不扇出;等 publisher confirm 成功。
func (d *Driver) republish(ctx context.Context, ch *amqp.Channel, m model.Message) error {
	pub := messageToPublishing(m)
	queue := d.cfg.Export.Queue
	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, pub)
	if err != nil {
		return fmt.Errorf("requeue to %q: %w", queue, err)
	}
	if confirm != nil {
		if ok := confirm.Wait(); !ok {
			return fmt.Errorf("requeue to %q nacked by broker", queue)
		}
	}
	return nil
}
```

- [ ] **Step 2: 编译校验** — `go build ./mq/amqp/`;peek 仍未定义,继续 Task 5。

---

## Task 5: exportPeek

**Files:**
- Modify: `mq/amqp/amqp.go`

- [ ] **Step 1: 实现 `exportPeek`。** 追加到 `amqp.go`:

```go
// exportPeek 非破坏抽样:仅取队头 n 条(Count>0 用 Count,否则用 prefetch),
// 全程不 ack/nack;读满 n 或 idle 后一次性 nack(requeue) 释放。仅碰队头 n 条,影响极小。
func (d *Driver) exportPeek(ctx context.Context, emit func(model.Message) error) error {
	n := d.common.Count
	if n <= 0 {
		n = d.prefetch()
	}
	// prefetch 卡在 n:broker 最多投 n 条未确认,天然不会给第 n+1 条。
	ch, deliveries, err := d.consumeChannel(n)
	if err != nil {
		return err
	}
	defer ch.Close()
	idle := d.common.Timeout
	var count int
	var lastTag uint64
	var held bool
	// 收尾:一次性把持有的未确认消息 requeue 回队。
	defer func() {
		if held {
			_ = ch.Nack(lastTag, true, true)
		}
	}()
	timer := newIdleTimer(idle)
	defer timer.stop()
	for {
		timer.reset(idle)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
			return nil
		case dv, ok := <-deliveries:
			if !ok {
				return nil
			}
			if err := emit(deliveryToMessage(dv)); err != nil {
				return err // defer 的 Nack multiple 会连同本条一起 requeue
			}
			lastTag = dv.DeliveryTag
			held = true
			count++
			if count >= n {
				return nil
			}
		}
	}
}
```

- [ ] **Step 2: 编译 + vet 全绿** — `go build ./... && go vet ./...`;预期通过。

- [ ] **Step 3: 跑非集成单测** — `go test ./mq/amqp/ -run 'TestResolveMode|TestRegistered|TestConfigTemplateParsesBack|TestTarget'`;预期 PASS。

---

## Task 6: 更新 template.yaml

**Files:**
- Modify: `mq/amqp/template.yaml`

- [ ] **Step 1: 把 `export` 段的 `ack` 行替换为 `mode` + 警告。** 新 `export` 段:

```yaml
export:
  queue: orders        # 源队列
  # mode: 导出后消息去向
  #   drain   (默认) 破坏性:读完即从队列移除;天然读空即止。恢复靠随后 import 回灌。
  #   requeue 非破坏:把每条重发回本队列(默认交换机按【队列名】直投,不经原 exchange、不扇出),
  #           confirm 成功后再移除原件,整队导出且顺序变为队尾追加。
  #           注意1:重发利用"默认交换机+队列名"特性做重新入队,投递 routing key 会变成队列名
  #                 (dump 内仍保留原始 routing key,import 可还原)。
  #           注意2:执行前必须停掉本队列的其他消费者,否则会重复消费并破坏计数。
  #   peek    非破坏:只取队头 N 条(见 --count),全程挂起、结束整体 requeue,用于抽样/测试。
  mode: drain
  prefetch: 100        # consume 预取量;peek 模式下 --count 为 0 时即取此条数
```

- [ ] **Step 2: 模板可解析回单测** — `go test ./mq/amqp/ -run TestConfigTemplateParsesBack`;预期 PASS(`mode: drain` 能解进 `Config`)。

---

## Task 7: 更新集成测试

**Files:**
- Modify: `mq/amqp/integration_test.go`

> 集成测试有 `//go:build integration`,默认 `go test ./...` 不跑;需真实 broker:`docker compose -f deploy/docker-compose.yaml up -d`,再 `go test -tags integration ./mq/amqp/`。

- [ ] **Step 1: 把 `TestExportNonDestructive`(第 123-138 行)整段替换为 requeue + drain 两个用例。**

```go
// TestExportDrain:drain(默认)导出后队列被清空。
func TestExportDrain(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	seed(t, ch, queue, 5)

	b := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "drain"}}, config.Common{})
	if got := countMessages(t, b); got != 5 {
		t.Errorf("exported %d, want 5", got)
	}
	time.Sleep(300 * time.Millisecond)
	if got := queueLen(t, ch, queue); got != 0 {
		t.Errorf("queue has %d after drain, want 0", got)
	}
}

// TestExportRequeue:requeue 非破坏全导,导出后队列条数不变,dump 完整,不死循环。
func TestExportRequeue(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	seed(t, ch, queue, 5)

	b := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "requeue"}}, config.Common{})
	if got := countMessages(t, b); got != 5 {
		t.Errorf("exported %d msgs, want 5", got)
	}
	time.Sleep(300 * time.Millisecond)
	if got := queueLen(t, ch, queue); got != 5 {
		t.Errorf("queue has %d after requeue export, want 5", got)
	}
}

// TestExportPeek:peek 只取队头 N 条,非破坏,队列条数不变。
func TestExportPeek(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	seed(t, ch, queue, 5)

	b := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "peek"}}, config.Common{Count: 3})
	if got := countMessages(t, b); got != 3 {
		t.Errorf("peeked %d msgs, want 3", got)
	}
	time.Sleep(300 * time.Millisecond)
	if got := queueLen(t, ch, queue); got != 5 {
		t.Errorf("queue has %d after peek, want 5", got)
	}
}
```

- [ ] **Step 2: 删除旧 `TestExportAckDrain`(原第 140-154 行)。** 已被上面的 `TestExportDrain` 取代,避免重复。

- [ ] **Step 3: 修 `TestImportRoundTripWithRouting` 里的 drain 导出行(原第 163 行)** —— `Ack: true` 改为 `Mode: "drain"`:

```go
	dumpBytes := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "drain"}}, config.Common{})
```

- [ ] **Step 4: 集成测试通过**(需 broker)— `docker compose -f deploy/docker-compose.yaml up -d` 后 `go test -tags integration ./mq/amqp/ -run 'TestExportDrain|TestExportRequeue|TestExportPeek|TestImportRoundTrip'`;预期全 PASS。

---

## 最终验证(全部完成后)

- [ ] `gofmt -l mq/amqp/` 输出为空。
- [ ] `go build ./...` 通过。
- [ ] `go vet ./...` 通过。
- [ ] `go test ./...`(非集成)通过。
- [ ] (有 broker 时)`go test -tags integration ./mq/amqp/` 通过。

## 自审清单

- **spec 覆盖:** drain(Task 3)、requeue(Task 4)、peek(Task 5)、config 枚举+校验(Task 1)、template 警告含"利用 routing key 特性重新入队 + 先停消费者"(Task 6)、三模式集成验证(Task 7)。
- **无占位符:** 每个改动步骤均含完整代码。
- **类型一致:** `ExportMode`/`ModeDrain`/`ModeRequeue`/`ModePeek`/`resolveMode`/`ExportConfig.Mode`/`consumeChannel`/`exportDrain`/`exportRequeue`/`exportPeek`/`republish`/`idleTimer` 跨任务一致。
