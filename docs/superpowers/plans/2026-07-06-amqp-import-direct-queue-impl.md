# AMQP 导入直投队列 实现计划

> **For agentic workers:** 逐任务实现本计划,每任务遵循 TDD(先写失败测试 → 跑失败 → 最小实现 → 跑通过)。步骤用 `- [ ]` 勾选跟踪。**每次改代码前需开发人员确认(见 AGENTS.md)。本计划任何任务、任何步骤都不执行提交动作(不 `git add`、不 `git commit`)——提交由开发人员在计划外决定。**

**Goal:** AMQP 导入时,当 `import.exchange` 为空且 `import.routing_key` 非空,走 AMQP 默认交换机(`""`)+ routing_key=队列名直投指定队列——与 export requeue 的"默认交换机 + 队列名直投"同一机制,让导入可直接灌入某个已知队列而不经原 exchange。

**Architecture:** 逻辑集中在 `Driver.target(m)`(`mq/amqp/config.go`)。现状:`Import.Exchange==""` 回退到消息**原始** exchange。改为:`Import.Exchange==""` 且 `Import.RoutingKey!=""` 时,强制 exchange=`""`(默认交换机),key 用配置的 routing_key(即目标队列名)——RabbitMQ 默认交换机把每个队列以其名字绑定,故 routingKey=队列名即直投该队列。其余组合语义不变。`publish()` 不改(已用 `target()` 的返回值)。

**Tech Stack:** Go 1.25,`github.com/rabbitmq/amqp091-go`,`github.com/goccy/go-json`。

**基线:** master。参考实现:`mq/amqp/amqp.go` 的 `republish`(第 208-223 行,默认交换机 `""` + queue 名直投)。

**隔离:** 按 AGENTS.md,新功能在 `master` 派生的 worktree 内实现:`git worktree add .worktrees/amqp-import-direct-queue -b feature/amqp-import-direct-queue master`。

---

## 语义表(target 决策矩阵)

以 `X=Import.Exchange`、`K=Import.RoutingKey`、消息原始 `(oe, ok)` 为输入:

| X | K | 结果 exchange | 结果 key | 说明 |
|---|---|---|---|---|
| 空 | 空 | `oe` | `ok` | 回退原始路由(不变) |
| 非空 | 非空 | `X` | `K` | 全覆盖(不变) |
| 非空 | 空 | `X` | `ok` | 覆盖 exchange,保留原 key(不变) |
| **空** | **非空** | **`""`** | **`K`** | **新增:默认交换机直投队列 `K`** |

第 4 行是本计划唯一行为变更;前三行由现有 3 个 `target` 测试锁定,须保持绿。

---

## 文件结构

**修改:**
- `mq/amqp/config.go` — `Driver.target`(第 66-78 行)加"空 exchange + 非空 key → 默认交换机直投"分支。
- `mq/amqp/template.yaml` — `import.exchange`/`import.routing_key` 注释补该组合语义。

**测试:**
- `mq/amqp/config_test.go` — 加 `TestTargetDirectQueue`(空 exchange + 非空 key → `""`/K);现有 3 个 `target` 测试须保持通过。
- `mq/amqp/integration_test.go` — 加 `TestImportDirectQueue`(gated `//go:build integration`):导出后清空,空 exchange + routing_key=队列名导入,断言队列恢复条数。

---

## Task 1: target 支持空 exchange 直投队列

**Files:**
- Modify: `mq/amqp/config.go:66-78`
- Test: `mq/amqp/config_test.go`

- [ ] **Step 1: 写失败测试** — 追加到 `mq/amqp/config_test.go`

```go
func TestTargetDirectQueue(t *testing.T) {
	// 空 exchange + 非空 routing_key:忽略原始 exchange,走默认交换机直投队列。
	d := &Driver{cfg: Config{Import: ImportConfig{RoutingKey: "orders"}}}
	ex, key := d.target(msgWithRoute("amq.topic", "orig.key"))
	if ex != "" || key != "orders" {
		t.Errorf("direct-queue = %q/%q, want \"\"/orders", ex, key)
	}
}
```

- [ ] **Step 2: 跑失败** — `go test ./mq/amqp/ -run TestTargetDirectQueue`;预期 FAIL(现逻辑回退原始 exchange `amq.topic`,得 `amq.topic`/`orders`)。

- [ ] **Step 3: 加分支** — `mq/amqp/config.go` 的 `target` 替换为:

```go
// target 决定导入目标:
//   - Import.Exchange 非空 → 覆盖 exchange;
//   - Import.RoutingKey 非空 → 覆盖 routing key;
//   - Import.Exchange 空且 RoutingKey 非空 → 走默认交换机 "" 直投队列(routing key=队列名),
//     忽略消息原始 exchange(与 export requeue 同机制);
//   - 两者皆空 → 用消息原始路由。
func (d *Driver) target(m model.Message) (exchange, key string) {
	var p Properties
	_ = json.Unmarshal(m.Properties, &p)
	exchange, key = p.Exchange, p.RoutingKey
	switch {
	case d.cfg.Import.Exchange != "":
		exchange = d.cfg.Import.Exchange
		if d.cfg.Import.RoutingKey != "" {
			key = d.cfg.Import.RoutingKey
		}
	case d.cfg.Import.RoutingKey != "":
		// 默认交换机直投:队列名即 routing key。
		exchange = ""
		key = d.cfg.Import.RoutingKey
	}
	return
}
```

- [ ] **Step 4: 跑通过** — `go test ./mq/amqp/ -run TestTarget`;预期 `TestTargetFallbackToOriginal`/`TestTargetOverride`/`TestTargetPartialOverride`/`TestTargetDirectQueue` 全 PASS。

## Task 2: 更新配置模板注释

**Files:**
- Modify: `mq/amqp/template.yaml:17-19`

- [ ] **Step 1: 补注释** — 把 `import` 段 `exchange`/`routing_key` 两行(第 18-19 行)替换为:

```yaml
  exchange: ""         # 覆盖目标 exchange;空 = 用消息原始 exchange
                       #   特例:exchange 空且 routing_key 非空 → 走默认交换机直投,
                       #   routing_key 视为目标队列名(不经原 exchange、不扇出)
  routing_key: ""      # 覆盖目标 routing key;空 = 用原始 routing key
```

- [ ] **Step 2: 校对** — 通读 `import` 段,确认与 Task 1 语义表一致。

## Task 3: 集成测试(直投队列往返)

**Files:**
- Modify: `mq/amqp/integration_test.go`(追加,`//go:build integration`)

- [ ] **Step 1: 写集成测试** — 追加到 `mq/amqp/integration_test.go`

```go
// TestImportDirectQueue:导出后清空,空 exchange + routing_key=队列名导入,应经默认交换机直投恢复条数。
func TestImportDirectQueue(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	const n = 5
	seed(t, ch, queue, n)

	// drain 导出到内存 buf,清空队列。
	b := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "drain"}}, config.Common{Timeout: 2 * time.Second})
	if got := countMessages(t, b); got != n {
		t.Fatalf("dump has %d msgs, want %d", got, n)
	}

	// 空 exchange + routing_key=队列名:走默认交换机直投该队列。
	importCfg := Config{Import: ImportConfig{Exchange: "", RoutingKey: queue, Confirm: true}}
	id := openDriver(t, importCfg, config.Common{Concurrency: 4})
	r := dump.NewSingleReaderForTest(bytes.NewReader(b))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := pipeline.Import(ctx, r, "amqp", id); err != nil {
		t.Fatalf("direct-queue import: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if queueLen(t, ch, queue) >= n {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := queueLen(t, ch, queue); got != n {
		t.Errorf("queue has %d after direct-queue import, want %d", got, n)
	}
}
```

- [ ] **Step 2: 确认依赖已导入** — 若 `bytes` 未在 `integration_test.go` 的 import 块,则加入(`exportToBuf` 返回 `[]byte`,`NewSingleReaderForTest` 需 `bytes.NewReader`);其余 `context`/`time`/`dump`/`pipeline`/`config` 现有测试已用。

- [ ] **Step 3: 说明(不在默认门禁跑)** — 需真实 broker:`docker compose -f deploy/docker-compose.yaml up -d` 后 `go test -tags integration ./mq/amqp/ -run TestImportDirectQueue`;预期 PASS。默认 `go test ./...` 不含本用例(build tag 隔离)。

## Task 4: 验证门禁

**Files:** 无(仅运行)

- [ ] **Step 1: 构建** — `go build ./...`;预期无输出。
- [ ] **Step 2: 静态检查** — `go vet ./...`;预期无输出。
- [ ] **Step 3: 格式** — `gofmt -l .`;预期空输出。
- [ ] **Step 4: 单元测试** — `go test ./...`;预期全部 PASS(含 `TestTargetDirectQueue`;集成用例因 build tag 不跑)。
- [ ] **Step 5(可选,需 broker):集成测试** — `go test -tags integration ./mq/amqp/`;预期含 `TestImportDirectQueue` 全 PASS。

---

## Self-Review

- **Spec 覆盖:** "空 exchange + 非空 routing_key → 默认交换机直投队列" → Task 1(`target` 分支)+ `TestTargetDirectQueue`(单元)+ `TestImportDirectQueue`(集成)。模板注释 → Task 2。✓
- **不回归:** 语义表前三行由现有 3 个 `target` 测试锁定;Task 1 Step 4 显式跑 `-run TestTarget` 全绿。✓
- **占位符扫描:** 各步骤均含完整代码/命令与预期输出,无 TBD/TODO。✓
- **类型一致性:** `Driver.target`、`ImportConfig{Exchange,RoutingKey}`、`msgWithRoute`、`exportToBuf`/`countMessages`/`openDriver`/`queueLen`/`setup`/`seed`(集成测试既有辅助)命名一致。✓
