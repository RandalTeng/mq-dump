# mq-dump

`mq-dump` 是一个用 Go 编写的消息队列导入/导出命令行工具:把消息从消息中间件**导出**为可移植的 dump 文件,再**导入**回去。当前内置 **AMQP**(RabbitMQ)驱动,核心按可插拔驱动设计——Kafka、RocketMQ 等可在不改动核心管道的前提下接入。

## 特性

- **导出 / 导入**:一条命令导出队列消息到 dump 文件,另一条导回。
- **单文件或分片导出**:大数据量可按消息条数拆分,生成独立清单索引各分片;每个分片本身即合法 dump,可脱离清单单独导入。
- **可移植 dump 格式**:JSONL,首行 meta 头;分片路径按清单目录相对解析,整套可迁移。
- **可插拔驱动**:驱动实现一个接口并注册即可接入,核心与管道零改动(见[驱动特性](#驱动特性))。
- **结构化日志**:基于 `log/slog`,可选级别与输出目标。

## 安装 / 构建

需要 Go 1.25+。

```bash
go build -o bin/mq-dump ./cmd/mq-dump
```

或直接 `go run ./cmd/mq-dump ...`。

## 快速开始

```bash
# 1. 生成 AMQP 驱动配置模板
mq-dump init -d amqp -o amqp.yaml

# 2. 按需编辑 amqp.yaml(连接串、队列、模式等)

# 3. 导出队列消息到 dump 文件
mq-dump export -d amqp -c amqp.yaml -f orders.jsonl

# 4. 从 dump 文件导入
mq-dump import -d amqp -c amqp.yaml -f orders.jsonl
```

## 命令

顶层有三个子命令,通用 flag 在子命令前给出。

| 子命令 | 说明 |
|--------|------|
| `export` | 导出消息到 dump 文件 |
| `import` | 从 dump 文件导入消息 |
| `init`   | 生成驱动配置模板 |

无参数运行等价于 `--help`。

### 通用 flag

| flag | 简写 | 默认 | 说明 |
|------|------|------|------|
| `--driver` | `-d` | (必填) | 驱动名(如 `amqp`) |
| `--config` | `-c` | | 驱动私有配置 YAML 路径(`export`/`import` 必需) |
| `--dump-file` | `-f` | | dump 文件路径;`-` 表示 stdin/stdout |
| `--count` | `-n` | `0` | 导出条数上限;`0` = 不限 |
| `--timeout` | `-t` | `0` | 导出空闲超时(如 `2s`);`0` = 不超时 |
| `--concurrency` | `-j` | `1` | 导入 worker 数;`0` = CPU 核心数 |
| `--split-count` | | `0` | 导出按消息条数拆分;每 N 条一个文件;`0` = 不拆 |
| `--log-level` | | `info` | 日志级别 `debug`\|`info`\|`warn`\|`error` |
| `--log-file` | | `mq-dump.log` | 日志文件路径;`-` = stderr |

`init` 额外支持 `--output` / `-o`(模板输出路径,缺省写 stdout)。

### 输出文件名规则(export)

- `-f -`:写 stdout(拆分模式下报错)。
- `-f orders.jsonl`:写该路径(去掉 `.jsonl` 得基名)。
- 不指定 `-f`:用驱动默认名(AMQP 为导出队列名),如 `<队列名>.jsonl`。

### 导入源判定(import)

按 `-f` 目标**首行内容自动判定**,不依赖扩展名:

- 首行含 `parts` 字段 → 清单模式:该文件即清单,按 `file` 相对清单目录逐分片读取。
- 首行含 `format_version` 但无 `parts` → 单文件模式(含内嵌 meta 的分片、`-` stdin)。

## 驱动特性

核心与驱动解耦:导出/导入管道、dump 格式、拆分/清单、并发导入等能力对所有驱动通用;下面按驱动说明各自**特有**的能力、配置与状态。

### AMQP(RabbitMQ) — 可用

能力:

- **连接**:`amqp://` URI(含 vhost);连接串在日志中脱敏。
- **导出模式**:`drain` / `requeue` / `peek`(见下表)。
- **预取控制**:`export.prefetch` 限制 consume 在途量。
- **导入路由重写**:按优先级互斥决定投递目标,支持覆盖 exchange / routing key、经默认交换机直投指定队列(见下)。
- **投递可靠性**:`persistent`(delivery-mode=2)、`confirm`(publisher confirms)、`mandatory`。
- **属性保真**:保留 `exchange`/`routing_key`/`content_type`/`delivery_mode`/`correlation_id`/`priority`/`expiration`/`message_id`/`type` 及 `amqp_headers`(headers 值保留精确类型,JSON 往返不丢失)。

配置模板(`mq-dump init -d amqp`):

```yaml
connection:
  uri: amqp://guest:guest@localhost:5672/
export:
  queue: orders        # 源队列
  mode: drain          # drain(默认) | requeue | peek
  prefetch: 100        # consume 预取量
import:
  exchange: ""         # 覆盖目标 exchange;空 = 用消息原始 exchange
                       #   特例:exchange 空且 routing_key 非空 → 走默认交换机直投,
                       #   routing_key 视为目标队列名(不经原 exchange、不扇出)
  routing_key: ""      # 覆盖目标 routing key;空 = 用原始 routing key
  persistent: true     # delivery-mode=2
  confirm: true        # publisher confirms
  mandatory: false
```

导出模式:

| 模式 | 破坏性 | 说明 |
|------|--------|------|
| `drain`（默认） | 是 | 读完即从队列移除(ack-after-persist,落盘成功才确认);队列取空(空闲超时)或到 `--count` 即止。恢复靠随后 import 回灌。 |
| `requeue` | 否 | 每条重发回本队列(默认交换机按队列名直投),confirm 成功后再移除原件;整队导出。**执行前必须停掉本队列的其他消费者。** |
| `peek` | 否 | 只取队头 N 条(见 `--count`),全程挂起,结束整体 requeue;用于抽样/测试。 |

导入路由决策(`target`,按优先级互斥):

| `import.exchange` | `import.routing_key` | 实际投递 exchange | 实际 routing key |
|---|---|---|---|
| 非空 | 非空 | 配置值 | 配置值 |
| 非空 | 空 | 配置值 | 消息原始 key |
| 空 | 非空 | `""`(默认交换机) | 配置值(视为队列名,直投) |
| 空 | 空 | 消息原始 exchange | 消息原始 key |

### Kafka — 计划中(TODO)

- [ ] `mq/kafka/` 驱动骨架,实现 `Driver` + `Factory` 并注册。
- [ ] 连接与认证配置(brokers、SASL/TLS)。
- [ ] 导出:按 topic / partition / offset 区间消费;记录 key、partition、offset、timestamp、headers。
- [ ] 导入:按原 key 或指定策略选 partition;支持幂等/事务性生产。
- [ ] 配置模板与单元测试(fake driver / 编解码往返)。

### RocketMQ — 计划中(TODO)

- [ ] `mq/rocketmq/` 驱动骨架,实现 `Driver` + `Factory` 并注册。
- [ ] 连接配置(NameServer 地址、AccessKey/SecretKey)。
- [ ] 导出:按 topic / consumer group 拉取;记录 tag、key、queueId、消息属性。
- [ ] 导入:按 topic + tag 发送,保留 key 与延迟级别等属性。
- [ ] 配置模板与单元测试。

## Dump 格式

JSONL(每行一个 JSON 对象),`format_version` 当前为 `1`。

### 单文件

首行是 meta 头,其后逐条消息:

```
{"format_version":1,"driver":"amqp","created_at":"2026-07-06T00:00:00Z"}
{"body":"...","timestamp":"...","properties":{...}}
{"body":"...","properties":{...}}
```

### 分片(`--split-count N`)

生成一个独立清单 `<基名>.mqdump.json`(单行 JSON)索引各分片:

```json
{
  "format_version": 1,
  "driver": "amqp",
  "created_at": "2026-07-06T00:00:00Z",
  "updated_at": "2026-07-06T00:05:00Z",
  "parts": [
    {"file": "orders-000.jsonl", "count": 100000},
    {"file": "orders-001.jsonl", "count": 45000}
  ],
  "total": 145000
}
```

- 每个分片是**独立的 v1 单文件**:首行内嵌 meta 头,可脱离清单直接 `-f <分片>` 导入。
- `created_at` 为导出起始时刻;`updated_at` 每次清单落地(逐分片崩溃安全重写与收尾)刷新。
- 分片 `file` 相对清单所在目录解析,并强制落在该目录内(拒绝绝对路径与 `..` 逃逸);整套(清单+分片)可整体迁移到任意目录。
- 拆分模式不支持写 stdout(`-f -` 报错)。

### 消息信封

通用层的 `Message` 与驱动无关:

- `body`:消息体(JSON 中为 base64)。
- `timestamp`:可选时间戳。
- `properties`:驱动私有属性(通用层不解析)。

AMQP 驱动在 `properties` 中保存原始路由与常用属性:`exchange`、`routing_key`、`content_type`、`delivery_mode`、`correlation_id`、`priority`、`expiration`、`message_id`、`type`、`amqp_headers`(headers 保留精确类型)。

## 用 Docker 跑一个本地 RabbitMQ

```bash
docker compose -f deploy/docker-compose.yaml up -d
# 管理界面 http://localhost:15672 (guest/guest)
```

也提供了工具自身的容器镜像(`deploy/Dockerfile`,多阶段构建,alpine 运行时);entrypoint 会把 `export`/`import`/`init` 及 flag 透传给 `mq-dump`,其余可执行命令直接运行(便于调试)。

## 开发

```bash
go mod tidy         # 同步依赖
go build ./...      # 构建
go test ./...       # 单元测试(不需真实 broker)
go vet ./...        # 静态检查
gofmt -l .          # 列出未格式化文件(应为空)
```

集成测试位于 `//go:build integration` 之后,需要真实 RabbitMQ:

```bash
docker compose -f deploy/docker-compose.yaml up -d
go test -tags integration ./mq/amqp/
```

## 项目结构

| 路径 | 职责 |
|------|------|
| `cmd/mq-dump/` | CLI 入口(注册驱动,分派子命令) |
| `internal/command/` | 子命令定义与装配(export/import/init、日志、IO) |
| `internal/pipeline/` | 连接 dump 编解码与驱动,编排导入/导出 |
| `internal/dump/` | dump 格式:JSONL 编解码、meta 头、单文件/分片 Writer 与 Reader、清单 |
| `mq/` | `Driver` 接口 + 工厂 + 注册表(核心扩展点) |
| `mq/amqp/` | AMQP/RabbitMQ 驱动 |
| `config/` | 通用配置(kong flag)与驱动 YAML 加载 |
| `model/` | 驱动无关的消息信封 |
| `deploy/` | Dockerfile、docker-compose、entrypoint |

## 扩展新驱动

在 `mq/` 定义的 `Driver` 接口一处即扩展点:实现 `Driver`(`Export`/`Import`/`Close`)与 `Factory`(`NewConfig`/`ConfigTemplate`/`Open`),在 `init()` 中 `mq.Register(name, factory)`,并在入口 `cmd/mq-dump/main.go` 以空白导入注册。核心管道与 `cmd/` 无需改动。

## 依赖

- `github.com/alecthomas/kong` — CLI 解析
- `github.com/rabbitmq/amqp091-go` — AMQP 客户端
- `github.com/goccy/go-json` — JSON 编解码
- `gopkg.in/yaml.v3` — 配置文件解析
- `golang.org/x/sync` — errgroup(并发导入)

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 发布。

```
Copyright 2026 Randal Teng (https://github.com/RandalTeng)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```
