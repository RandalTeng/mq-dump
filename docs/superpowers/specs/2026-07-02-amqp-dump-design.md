# amqp-dump 设计文档

> 状态:设计定稿(v2),待实现。本文档是 brainstorming 阶段的产物,下一步进入 writing-plans 出实现计划。

## 目标

一个 Go 单二进制 CLI 工具,用于**导出**消息队列中的消息到可移植的 dump 文件,并能**导入**回队列。首发支持 **AMQP(RabbitMQ)**,架构上可插拔扩展 Kafka、RocketMQ 等。**通用配置由命令行 flag 提供;驱动私有参数由 `--config` 指向的 YAML 文件提供。**

## 核心设计原则

1. **两条正交分层**:纵轴 = 上层通用 vs 驱动私有;横轴 = config model vs message/properties model。通用层对驱动私有内容**一无所知**。
2. **配置来源清晰二分**:通用配置(选驱动 + 全局编排)= kong flag;驱动私有配置(connection/export/import)= `--config` YAML 文件。二者不混。
3. **路由覆盖归各驱动**:通用层不含任何路由/rewrite 概念。每个驱动在自己的 import 配置(YAML)里用 typed 字段实现"路由覆盖"。
4. **可插拔驱动**:新增驱动 = 实现接口 + `init()` 注册,**不改通用层与命令**。
5. **命令按文件拆分**:`export`/`import`/`init` 同属 `internal/command` 一个包,按命令拆成 `export.go`/`import.go`/`init.go`,靠 kong 声明式 `cmd:""` + `Run()` 注册,flag 注入统一静态。
6. **防返工契约先行**:并发安全、ack-after-persist、dump 格式版本号在 v1 就钉进接口/格式,避免后续破坏性变更。

## 技术栈

- Go 1.22+,Go modules
- `github.com/alecthomas/kong` — CLI 解析(声明式子命令 + 通用 flag)
- `github.com/rabbitmq/amqp091-go` — AMQP 驱动
- `golang.org/x/sync/errgroup` — 导入侧 worker 池
- `gopkg.in/yaml.v3` — 驱动私有配置文件解析
- 单元测试:stdlib `testing`;集成测试:build tag `integration` + 真实 RabbitMQ

> 注:不再需要 `kong-yaml`。通用配置来自 kong flag,驱动配置由 yaml.v3 直接解析 `--config` 文件,无需把 YAML 作为 flag 默认值来源。

## 决策记录(为什么这么定)

| 决策点 | 选择 | 理由 |
|---|---|---|
| CLI 调用模型 | subcommand-first(`export`/`import`/`init`) | 动词领头,git/docker 风格,直观 |
| 配置来源 | 通用=flag;驱动私有=`--config` YAML | flag 注入统一静态,去掉动态 plugin 与 bootstrap |
| 配置格式 | YAML | 支持注释(生成模板友好)、嵌套分段 |
| CLI 框架 | kong | 声明式 `cmd:""`+`Run()`,注入按类型 |
| 命令组织 | 单包 `internal/command`,按命令拆文件 `export.go`/`import.go`/`init.go` | 同包多文件,结构体 `ExportCmd`/`ImportCmd`/`InitCmd`;避免关键字包名绕路 |
| dump 格式 | JSONL + meta 头 | 流式、可追加、可 diff;meta 头带版本与驱动标识 |
| 路由重写 | 归各驱动 import 配置(YAML,typed) | 各驱动路由语义不同;通用层不掺和,消除过早抽象 |
| 驱动 I/O | 回调/迭代器接口 | 支持 ack-after-persist、背压;并发安全契约防返工 |
| 并发 | 导入侧 worker 池;默认 1,`0`=CPU 核心数 | 导入受 confirm RTT 限制,是真瓶颈;导出靠 prefetch |
| init 模板 | go:embed 手写模板 | 注释质量最高、实现最简;解析回环测试防漂移 |
| 泛型 | 不用于分发边界 | registry 运行时按字符串选驱动,与编译期类型参数硬冲突 |

## 架构总览

单二进制。核心只负责三件事:**配置装配、dump 编解码、导入/导出管道**。每种消息队列是一个可插拔驱动,编译进二进制,运行时按 `--driver` 选择。

```
   通用 flag (kong)  ──►  config.Common (driver/config/dump-file/count/timeout/concurrency)
   --config file.yaml ──►  yaml.v3 解析 ──► 驱动私有 Config
                              |
   export:  Driver.Export(emit) --> emit 写 JSONL(落盘) --> 驱动 ack
   import:  JSONL 解码 --> next() --> Driver.Import(next) --> publish + confirm
                              |
                   internal/mq(Driver + Factory + registry)
                     |             |               |
                  amqp/        kafka/(未来)     rocketmq/(未来)
```

新增驱动 = 实现接口 + `init()` 注册,不改 `cmd/`、`internal/command`、管道。

## 模块布局

```
cmd/amqp-dump/main.go            main:kong.Parse(&cli) → kctx.Run(&cli.Common);空导入驱动包触发注册
internal/command/cli.go          根 CLI 聚合 struct(嵌入 config.Common + 各子命令字段)
internal/command/export.go       package command:ExportCmd + Run(导出编排)
internal/command/import.go       package command:ImportCmd + Run(导入编排)
internal/command/init.go         package command:InitCmd + Run(输出驱动配置模板)
internal/mq/driver.go            Driver 接口、Factory 接口、Register/Get/Names 注册表
internal/mq/amqp/amqp.go         AMQP 驱动实现(Export/Import/Close)
internal/mq/amqp/config.go       AMQP 私有 Config(connection/export/import,含路由覆盖;仅 yaml tag)
internal/mq/amqp/properties.go   AMQP 私有 Properties(content-type/delivery-mode/... + 原始路由)
internal/mq/amqp/template.yaml   init 模板(go:embed)
internal/model/message.go        通用消息信封(Body/Timestamp/Properties)
internal/dump/codec.go           JSONL Encoder/Decoder
internal/dump/meta.go            dump meta 头结构(format_version/driver/created_at)
internal/config/common.go        通用配置(kong flag tag)+ 加载驱动 YAML 的 helper
internal/pipeline/run.go         export/import 编排:连接 codec 与 driver
testdata/                        golden 模板、样例 dump、集成测试 fixtures
```

同一个包 `command`,按命令拆文件(`export.go`/`import.go`/`init.go`)。`import`/`init` 只是文件名与 struct 字段名(`ExportCmd`/`ImportCmd`/`InitCmd`),不作包名,故不触碰 Go 关键字/保留名限制;命令用户可见名由 kong `name:"..."` tag 决定。

## CLI 命令注册(kong 声明式 + 同包多文件)

```go
// internal/command/cli.go
type CLI struct {
    config.Common                                 // 嵌入 → 通用 flag 成全局
    Export ExportCmd `cmd:"" help:"导出消息到 dump 文件"`
    Import ImportCmd `cmd:"" help:"从 dump 文件导入消息"`
    Init   InitCmd   `cmd:"" name:"init" help:"生成驱动配置模板"`
}

// cmd/amqp-dump/main.go
func main() {
    var cli command.CLI
    kctx := kong.Parse(&cli, kong.Name("amqp-dump"))
    kctx.FatalIfErrorf(kctx.Run(&cli.Common))     // kong 按类型把 *Common 注入选中命令的 Run
}

// internal/command/export.go   (package command)
type ExportCmd struct{}                           // 无 export 专属 flag(全在通用层/驱动 YAML)
func (c *ExportCmd) Run(common *config.Common) error {
    f, ok := mq.Get(common.Driver); if !ok { return fmt.Errorf("unknown driver %q", common.Driver) }
    cfg := f.NewConfig()
    if err := config.LoadDriverYAML(common.Config, cfg); err != nil { return err }  // yaml.v3
    d, err := f.Open(*common, cfg); if err != nil { return err }
    defer d.Close()
    return pipeline.Export(ctx, common, d)
}
```

`InitCmd.Run` 不建连接:取 `Factory.ConfigTemplate()` 直接输出,故 `init` 无需 `--config` 文件。三个命令 struct 同属 `package command`,分别落在 `export.go`/`import.go`/`init.go`。

## 核心接口与模型

### 通用消息信封(驱动无关,无路由无 Headers)

```go
// internal/model/message.go
type Message struct {
    Body       []byte          `json:"body"`                 // JSON 自动 base64
    Timestamp  time.Time       `json:"timestamp,omitempty"`
    Properties json.RawMessage `json:"properties,omitempty"` // 驱动私有属性 + 原始路由,通用层不解析
}
```

通用层只认这三个字段。原始路由(如 AMQP exchange/routing_key)存在**驱动私有 Properties** 里,不进通用层。

### 通用配置(kong flag;选驱动 + 全局编排,无路由/无 rewrite)

```go
// internal/config/common.go
type Common struct {
    Driver      string        `short:"d" required:"" help:"消息队列驱动 (amqp)"`
    Config      string        `short:"c" type:"existingfile" help:"驱动私有配置 YAML 路径"`
    DumpFile    string        `short:"f" help:"dump 文件路径;\"-\" = stdin/stdout"`
    Count       int           `short:"n" help:"导出条数上限;0 = 不限"`
    Timeout     time.Duration `short:"t" help:"导出空闲超时"`
    Concurrency int           `short:"j" default:"1" help:"导入 worker 数;0 = CPU 核心数"`
}

// 并发度解析:0 → runtime.NumCPU();否则取 max(1, n)
func (c Common) Workers() int {
    if c.Concurrency == 0 { return runtime.NumCPU() }
    if c.Concurrency < 1  { return 1 }
    return c.Concurrency
}
```

`--config` 仅 `export`/`import` 需要(`init` 不读)。driver 私有参数一律来自该 YAML,通用 flag 不含任何驱动私有项。

**短 flag 支持**:长名由字段名自动 kebab 化(`DumpFile` → `--dump-file`),短名用 kong `short:"X"` tag。映射如下,每个通用 flag 都提供长短两式:

| flag | 长 | 短 |
|---|---|---|
| 驱动 | `--driver` | `-d` |
| 驱动配置文件 | `--config` | `-c` |
| dump 文件 | `--dump-file` | `-f` |
| 导出条数上限 | `--count` | `-n` |
| 导出空闲超时 | `--timeout` | `-t` |
| 导入并发度 | `--concurrency` | `-j` |
| init 输出路径 | `--output` | `-o` |

短 flag 可合并(kong 支持 `-dc amqp` 之类的紧凑写法)。示例:`amqp-dump export -d amqp -c amqp.yaml -f dump.jsonl -n 1000`。

### 驱动接口(回调/迭代器 + 并发安全契约)

```go
// internal/mq/driver.go
type Driver interface {
    // emit 保证并发安全(内部对单文件 writer 加锁);驱动可从多 goroutine 调用。
    // emit 返回 nil 才代表已落盘,驱动据此 ack(ack-after-persist)。
    Export(ctx context.Context, emit func(model.Message) error) error
    // next 保证并发安全;驱动可开 N 个 worker 各自调用 next 拉取。
    // 返回 (msg, ok, err):ok=false 表示流结束。
    Import(ctx context.Context, next func() (model.Message, bool, error)) error
    Close() error
}

type Factory interface {
    NewConfig() any                                  // 交出驱动私有 config model 指针
    ConfigTemplate() string                          // init 用的 go:embed 模板
    Open(c config.Common, cfg any) (Driver, error)   // 用装配好的配置建驱动;校验必填项
}

func Register(name string, f Factory) // 驱动在 init() 里注册
func Get(name string) (Factory, bool)
func Names() []string
```

`cmd/amqp-dump` 只 `_ "…/internal/mq/amqp"` 空导入触发注册,与具体驱动解耦。`Open(cfg any)` 内一次类型断言(registry 运行时选驱动的必然代价,影响隔离在单点)。

### 驱动私有模型(AMQP,全 typed,不用泛型;仅 yaml tag)

```go
// internal/mq/amqp/config.go —— 只从 --config YAML 解析,不带 kong tag
type Config struct {
    Connection ConnConfig   `yaml:"connection"`
    Export     ExportConfig `yaml:"export"`
    Import     ImportConfig `yaml:"import"`   // 路由覆盖在这里
}
type ConnConfig struct {
    URI string `yaml:"uri"`   // amqp://user:pass@host:port/vhost
}
type ExportConfig struct {
    Queue    string `yaml:"queue"`      // 源队列
    Ack      bool   `yaml:"ack"`        // 破坏性 drain:读完即 ack 移除
    Prefetch int    `yaml:"prefetch"`   // 默认 100(Open 内兜底)
}
type ImportConfig struct {
    Exchange   string `yaml:"exchange"`     // 覆盖目标 exchange;空=用原值
    RoutingKey string `yaml:"routing_key"`  // 覆盖目标 routing key;空=用原值
    Persistent bool   `yaml:"persistent"`   // delivery-mode=2
    Confirm    bool   `yaml:"confirm"`       // publisher confirms
    Mandatory  bool   `yaml:"mandatory"`
}

// internal/mq/amqp/properties.go —— 驱动私有 message properties model,含原始路由(保真)
type Properties struct {
    Exchange      string     `json:"exchange,omitempty"`      // 导出时原始路由
    RoutingKey    string     `json:"routing_key,omitempty"`
    ContentType   string     `json:"content_type,omitempty"`
    DeliveryMode  uint8      `json:"delivery_mode,omitempty"`
    CorrelationID string     `json:"correlation_id,omitempty"`
    Priority      uint8      `json:"priority,omitempty"`
    Expiration    string     `json:"expiration,omitempty"`
    MessageID     string     `json:"message_id,omitempty"`
    Type          string     `json:"type,omitempty"`
    AMQPHeaders   amqp.Table `json:"amqp_headers,omitempty"`  // int/bool/嵌套全保真
}
```

## 路由覆盖(归各驱动,通用层不参与)

AMQP 驱动导入时决定目标:配置非空则覆盖,否则回退用导出时保存在 Properties 里的原始路由。

```go
func (d *AMQPDriver) target(m model.Message) (exchange, key string) {
    var p Properties
    _ = json.Unmarshal(m.Properties, &p)
    exchange, key = p.Exchange, p.RoutingKey        // 默认:原值
    if d.cfg.Import.Exchange != ""   { exchange = d.cfg.Import.Exchange }   // 覆盖
    if d.cfg.Import.RoutingKey != "" { key = d.cfg.Import.RoutingKey }
    return
}
```

例(源队列 a → 导入到 exchange b + routing key B1),覆盖写在 `--config` YAML:
```yaml
# amqp.yaml
connection: { uri: amqp://guest:guest@localhost:5672/ }
export:      { queue: a }
import:      { exchange: b, routing_key: B1, confirm: true }
```
```bash
amqp-dump import --driver amqp --config amqp.yaml --dump-file dump.jsonl
```

Kafka 将来接入,其 import 段自定义 `topic`/`partition`/`key`,与 AMQP 无关 —— 通用层零改动。

## dump 格式(JSONL + meta 头)

首行是 meta 头,其后每行一条消息:
```json
{"format_version":1,"driver":"amqp","created_at":"2026-07-02T12:00:00Z"}
{"body":"aGVsbG8=","timestamp":"...","properties":{"exchange":"a","routing_key":"k","content_type":"text/plain"}}
```

- **同驱动往返**为唯一保证:导入时校验 `meta.driver` 与 `--driver` 一致,不匹配报错。
- `format_version` 用于未来格式演进的迁移判定。
- 跨驱动导入(如 AMQP dump → Kafka)不在 v1 保证范围(Properties 驱动私有,无法重建原生属性)。

## 并发模型

- **导出**:靠 AMQP `prefetch` 流水线;写盘单 writer 串行;不铺 worker(收益低且乱序)。
- **导入**:worker 池,worker 数 = `Common.Workers()`(`concurrency` 默认 1 保序,`0` = CPU 核心数)。
- amqp091 `Channel` 非并发安全 → 每 worker 各自建独立 Channel。这决定了并发必须归驱动所有(框架无法代管 channel-per-worker)。
- `emit`/`next` 契约声明**并发安全**;现在钉死,将来加并发/分区感知不破坏接口。

```go
func (d *AMQPDriver) Import(ctx context.Context, next func() (model.Message, bool, error)) error {
    n := d.common.Workers()                  // 0→NumCPU, 否则 max(1,n)
    g, ctx := errgroup.WithContext(ctx)
    for i := 0; i < n; i++ {
        g.Go(func() error {
            ch, err := d.conn.Channel()          // 每 worker 独立 channel
            if err != nil { return err }
            defer ch.Close()
            if d.cfg.Import.Confirm { ch.Confirm(false) }
            for {
                msg, ok, err := next()           // 线程安全拉取
                if err != nil || !ok { return err }
                if err := d.publish(ch, msg); err != nil { return err }
            }
        })
    }
    return g.Wait()
}
```

## 错误处理

- 库层一律 `fmt.Errorf("...: %w", err)` 包裹;仅 `main`/命令 `Run` 记录并以非零码退出。
- `signal.NotifyContext` 捕获 SIGINT → ctx 取消 → 导出侧未 ack 消息 requeue,干净退出。
- 导出 ack-after-persist:`emit` 返回 nil(已落盘)驱动才 ack;失败不 ack → 消息保留在队列,不丢。
- 导入默认 fail-fast(首错停,errgroup 取消其余 worker);`--continue-on-error` 归后续。

## init 命令(go:embed 手写模板)

```go
// internal/command/init.go   (package command)
type InitCmd struct {
    Output string `short:"o" help:"模板输出路径;缺省写 stdout"`
}

//go:embed template.yaml   (amqp 驱动包内)
var configTemplate string
func (amqpFactory) ConfigTemplate() string { return configTemplate }
```
`amqp-dump init -d amqp -o amqp.yaml`(或省略 `-o` 输出到 stdout)直接吐该模板。`InitCmd.Run` 只调 `Factory.ConfigTemplate()`,不建连接、不读 `--config`。防漂移:单元测试断言"模板能被 yaml.v3 解析回 `Config` 且必填字段齐全"。

## 测试策略

**单元(默认 `go test`,不依赖 broker):**
- JSONL 编解码往返:meta 头 + body base64 + Properties 保真
- registry Register/Get + 未知驱动报错
- 驱动 YAML 加载:`config.LoadDriverYAML` 解析 `Config` 各段
- `Common.Workers()`:0→NumCPU、1→1、负→1、n→n
- 路由覆盖:`target()` 在覆盖/回退两分支的取值(最高价值纯函数)
- `amqp.Delivery → Properties/Message → amqp.Publishing` 转换往返
- init 模板对 golden 文件 + 解析回环
- 导入 fail-fast:worker 出错取消其余

**集成(build tag `integration`,默认排除):**
- 对真实 RabbitMQ(docker-compose/testcontainers)播种队列 → 导出 → 导入 → 断言往返一致
- 非破坏导出后队列消息仍在;`--ack` drain 后队列空
- `--concurrency>1` / `--concurrency 0` 吞吐 + 结果完整性

## v1 范围(YAGNI)

**做:** export + import + init;单 AMQP 驱动;默认非破坏导出 + `--ack` drain;导入并发(默认 1,`0`=CPU 核心数);JSONL + meta 头;通用 flag + 驱动 YAML 配置;命令同包按文件拆分。

**不做(靠架构后续接入,不改通用层):** Kafka/RocketMQ;跨驱动导入;`--continue-on-error`;导出并发;规则映射式路由重写;分区感知并发。