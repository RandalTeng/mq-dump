# mq-dump 日志 / 默认输出 / 文件拆分 设计文档

> 状态:设计定稿,待用户复核。本文档是 brainstorming 阶段产物,复核通过后进入 writing-plans 出实现计划。
> 基线:master `2967209`,已实现 v1(export/import/init + AMQP 驱动 + kong CLI)。

## 目标

在既有 mq-dump 上补充三项特性,均不破坏现有公共接口(`mq.Driver`/`mq.Factory`)与 v1 dump 格式:

1. **日志记录**:引入 `log/slog` 结构化日志,**默认写文件**(`mq-dump.log`),`--log-file` 改路径 / `-` 转 stderr;`--log-level` 控级别。stdout 专用于 dump 数据(`-f -`),stderr 专用于致命错误——日志默认不占二者。
2. **默认输出文件名**:导出不带 `-f` 时,默认写 `<队列名>.jsonl`;显式 `-f -` 才写 stdout。
3. **大文件拆分**:导出按消息条数拆分(`--split-count N`),生成独立**清单文件**索引各纯数据分片;导入按清单聚合读取。

## 核心设计原则(延续 v1)

1. **不破坏公共接口**:新能力经**可选接口**(`mq.Namer`)与**默认 logger**(`slog.SetDefault`)接入;外部驱动无需改动即可编译,按需选择实现。
2. **通用层不知驱动私有**:队列名归 AMQP 私有配置;通用层经 `mq.Namer` 拿到"建议名",不感知语义。
3. **拆分/清单归 dump 层**:新增 `dump.Writer`/`dump.Reader` 抽象,管道只调 `Write`/`Read`,不掺文件轮转。
4. **向后兼容 dump 格式**:不拆分时单文件内嵌 meta(v1 格式原样);仅拆分时才出清单 + 纯数据分片。

## 决策记录

| 决策点 | 选择 | 理由 |
|---|---|---|
| 日志库 | 标准库 `log/slog` | 无新依赖;Go 1.25 自带;结构化 + 级别 |
| 日志默认目标 | 文件 `mq-dump.log`(cwd,追加写) | 统一文件日志;stdout/stderr 留给业务(dump / 错误) |
| 日志目标覆盖 | `--log-file <路径>`;`--log-file -` → stderr | 需要控制台日志时显式转 stderr |
| 队列名获取 | 可选接口 `mq.Namer` | 非破坏;外部驱动 opt-in,类比 `io.WriterTo` |
| 无 `-f` 默认 | `<队列名>.jsonl` | 直接满足诉求;显式 `-f -` 保留 stdout |
| 拆分触发 | 消息条数 `--split-count N`,默认 0=关 | 用户选定;实现直观 |
| 拆分索引 | 独立清单 `.mqdump.json`(单行 JSON)+ 纯数据分片 | meta 只存一份;分片无冗余头;可单独校验 |
| 导入模式判定 | **按内容**:首行含 `parts` → 清单模式,否则单文件 | 不依赖扩展名/额外配置;清单自描述 |
| 不拆分格式 | 单文件内嵌 meta(v1 不变) | 向后兼容;常见情况零额外文件 |
| 轮转逻辑位置 | `dump.Writer` 接口(single/split 两实现) | 管道保持编排职责单一 |
| logger 传递 | `slog.SetDefault` + 包级 `slog.*` | 非破坏;驱动/管道直接调用 |
| `--split-count` 归属 | `config.Common` | 与既有 export-only 的 `Count`/`Timeout` 同置一处 |

## 特性一:日志(slog)

### 配置(config.Common 新增)

```go
LogLevel string `default:"info" help:"日志级别 debug|info|warn|error"`
LogFile  string `default:"mq-dump.log" help:"日志文件路径;\"-\"=stderr"`
```

### 装配(command.Run)

`Run` 在解析后、分发前构建 logger 并 `slog.SetDefault`:

- 目标:`LogFile == "-"` → stderr;否则 `os.OpenFile(LogFile, O_CREATE|O_WRONLY|O_APPEND, 0o644)` 追加写(默认 `mq-dump.log`)。
- **stdout/stderr 固定业务专用**:stdout 只承载 `-f -` 的 dump 数据,stderr 只承载 kong 的致命错误;日志默认落文件,不污染二者(除非显式 `--log-file -`)。
- Handler:`slog.NewTextHandler`,级别由 `LogLevel` 解析(非法值报错退出)。
- 文件句柄在进程退出时关闭(`Run` 内 defer)。
- **仅长任务建文件日志**:文件日志仅为 `export`/`import` 装配;`init`、`--help`、参数错误等短命令不建 `mq-dump.log`,避免污染 cwd。装配点放在子命令 `Run` 内(而非全局 `Run` 顶部),或按选中命令名判定。

### 事件与级别

| 事件 | 级别 | 字段 |
|---|---|---|
| 配置装配完成 | debug | driver, log_level, split_count |
| 连接打开/关闭 | info | driver, addr(脱敏:仅 host:port) |
| 导出开始 | info | driver, queue, split_count, out |
| 输出文件创建 | info | file(single 基文件 / 每个分片) |
| 分片轮转 | info | part_file, part_seq, part_count |
| 清单写入 | debug | manifest, parts, total |
| 导入开始 | info | driver, source, target, concurrency |
| 读到分片 | debug | part_file, seq |
| 进度 | info | 每 10000 条:done(导出/导入各计) |
| 单条 emit/publish | debug | seq |
| 收到中断信号 | warn | signal;触发优雅取消 |
| 完成 | info | total, files, elapsed |
| 错误 | error | err(在 main/命令边界记录一次,不重复) |

> URI 脱敏:日志只输出 `host:port`,绝不打印含账号口令的完整 URI。

## 特性二:默认输出 `<队列名>.jsonl`

### 可选接口(mq 包新增)

```go
// Namer 由驱动可选实现,给出无 -f 时的默认 dump 基名(不含扩展名)。
type Namer interface {
	DumpName() string
}
```

AMQP `*Driver` 实现:`func (d *Driver) DumpName() string { return d.cfg.Export.Queue }`。

### 输出路径解析(command 层)

导出目标按下述规则解析(替换现有 `openDumpWriter` 调用点):

1. `-f -` → stdout(`nopWriteCloser`)。
2. `-f <path>`(非空非 `-`)→ 该路径为基名。
3. `-f` 空 →
   - 驱动实现 `mq.Namer` 且 `DumpName()` 非空 → 基名 = `DumpName()`;
   - 否则报错:`未指定 -f 且驱动 %q 无默认名,请用 -f 指定输出`。

基名 → 实际文件:不拆分时 `<基名>.jsonl`(若 `-f` 已带 `.jsonl` 则不重复加)。

## 特性三:按条数拆分 + 清单

### 配置(config.Common 新增)

```go
SplitCount int `help:"导出按消息条数拆分;每 N 条一个文件;0=不拆"`
```

### dump.Writer 抽象(替换裸 io.Writer)

```go
// Writer 是导出写目标:先 WriteMeta 一次,再逐条 Write,最后 Close。
type Writer interface {
	WriteMeta() error          // single 写内嵌头;split 为 no-op(meta 进清单)
	Write(m model.Message) error
	Close() error              // split 收尾写最终清单
}
```

- **single**(`SplitCount==0`):现有 `Encoder` 行为——`<基名>.jsonl` 首行 meta + 逐条消息。
- **split**(`SplitCount>0`):
  - 基名派生:`-f <path>` 去掉尾部 `.jsonl` 扩展名即为基名(如 `-f orders.jsonl`→基名 `orders`);`-f` 空则基名 = `Namer.DumpName()`。
  - 分片名 `<基名>-000.jsonl`、`<基名>-001.jsonl` …(三位零填充,超 999 自然进位到四位)。
  - 分片为**纯数据**(无 meta 头);每写满 N 条 rotate 到下一分片。
  - 清单 `<基名>.mqdump.json`:**每完成一个分片后重写一次**,收尾再写一次终态。崩溃时清单覆盖所有**已完成**分片,进行中分片不在列——可安全重导已完成部分。
  - `-f -`(stdout)+ `SplitCount>0` → 报错:`拆分导出不支持写 stdout`。

### 清单格式(.mqdump.json)

```json
{
  "format_version": 1,
  "driver": "amqp",
  "created_at": "2026-07-03T00:00:00Z",
  "parts": [
    {"file": "orders-000.jsonl", "count": 100000},
    {"file": "orders-001.jsonl", "count": 45000}
  ],
  "total": 145000
}
```

`file` 相对清单所在目录(整套可迁移)。`format_version` 复用 `dump.FormatVersion`。上例为便于阅读做了缩进,**磁盘上清单为单行紧凑 JSON**——与单文件首行 meta 同为"首行即头",便于统一按首行判定模式。

### dump.Reader 抽象(替换裸 io.Reader)

```go
// Reader 是导入读源:先 Meta 校验驱动,再逐条 Read 到 ok=false。
type Reader interface {
	Meta() (Meta, error)
	Read() (model.Message, bool, error)
	Close() error
}
```

导入侧**按内容自动判定**模式(不依赖扩展名,command 层无需任何额外清单配置):读 `-f` 目标首行 JSON,

- 首行含 `parts` 字段 → **manifest reader**:该文件即清单,取 driver/parts,按 `file` 相对清单目录按序打开各分片纯数据流拼接;`Meta()` 由清单头(format_version/driver/created_at)构造。
- 首行含 `format_version` 但无 `parts`(单文件内嵌 meta)→ **single reader**:现有行为,首行 meta + 逐条。含 `-f -` stdin、`-f X.jsonl`。

> 因此重新导入**无需定义任何"清单 meta"配置项**:清单文件自描述(driver 从清单读、分片路径从 `parts` 读),`-f` 指向清单文件即可。约定扩展名 `.mqdump.json` 仅为默认导出命名,不参与判定。

## 模块改动

- `config/common.go`:+`LogLevel` `LogFile` `SplitCount`。
- `internal/command/run.go`:+logger 装配(`setupLogger` → `slog.SetDefault`),defer 关文件。
- `internal/command/io.go`:`openDumpWriter` → `resolveExportWriter(common, driver)` 返回 `dump.Writer`;`openDumpReader` → `resolveImportReader(common, driver)` 返回 `dump.Reader`。
- `internal/command/export.go` / `import.go`:改用新 resolver 与 `dump.Writer/Reader`;传 `SplitCount`。
- `internal/dump/`:+`writer.go`(single/split Writer)、`reader.go`(single/manifest Reader)、`manifest.go`(Manifest 结构 + 读写)。现有 `codec.go` 的 `Encoder`/`Decoder` 被 Writer/Reader 复用或内联。
- `internal/pipeline/run.go`:`Export` 签名 `io.Writer`→`dump.Writer`;`Import` 签名 `io.Reader`→`dump.Reader`;meta 校验移入 Reader。
- `mq/driver.go`:+`Namer` 可选接口(不改 `Driver`/`Factory`)。
- `mq/amqp/amqp.go`:实现 `DumpName()`;连接/导出/导入关键路径加 slog。

## 扩展性:多队列导入/导出(前瞻分析)

问:未来支持"一次导出/导入多个队列"是否被当前设计卡住?**结论:不卡——多队列是驱动私有职责,通用层与管道无需改动。** 现在只需守住下面几处向后兼容,即可零返工接入:

- **驱动私有配置可增长**:`amqp.Config.Export.Queue string` 后续可加 `Queues []string`(与 `Queue` 并存保兼容)。这是驱动私有 YAML 的演进,不触碰 `config.Common`。
- **管道天然支持**:`Driver.Export(emit)` 由驱动内部驱动——驱动可消费 N 个队列、把消息全部 `emit`;`Import(next)` 同理。`pipeline` 不含队列概念,无需改。
- **清单可承载来源**:`parts[]` 为对象数组,后续可给每片加可选 `queue` 字段标注来源,`format_version` 视需要递增;当前不加,但结构已预留。
- **消息自带来源路由**:AMQP 来源 exchange/routing key 已存于消息私有 `Properties`,多队列导入时逐条路由信息不丢。
- **默认命名**:`mq.Namer.DumpName()` 返回单一基名,单队列足够;多队列时由驱动决定命名(集合名/首队列名),或走单一清单聚合多队列分片——届时再定,不影响当前接口。

**当前唯一要守住的约束**:清单 `parts` 保持对象数组(已是),`model.Message` 来源信息继续走驱动私有 `Properties`(已是)。故当前设计不给多队列埋破坏性改动。

## 测试

| 用例 | 断言 |
|---|---|
| split writer 边界轮转 | 写 2N+k 条 → 3 个分片,条数 N/N/k |
| 清单内容 | parts 列表、count、total 与实际一致;file 相对路径 |
| manifest reader 聚合 | 按序读全部分片,总条数一致;driver 校验失败报错 |
| single↔split 回环 | 导出 N 条按 k 拆 → 导入 → 同 N 条,内容逐字节等 |
| 导入模式自动判定 | 首行含 `parts`→manifest reader;仅 `format_version`→single reader |
| Namer 默认名 | 有 Namer→`<queue>.jsonl`;无 Namer 且无 -f→报错 |
| split+stdout | `SplitCount>0` 且 `-f -` → 报错 |
| logger 默认落文件 | 不带 `--log-file` → 写 cwd `mq-dump.log`;`--log-file -` → stderr;非法 `--log-level` 报错 |

集成测试(build tag `integration`)沿用真实 RabbitMQ,补一条拆分导出→导入回环。

## 不做(YAGNI)

- 按字节大小拆分(`--split-size`)——本期只按条数。
- 日志 JSON 格式开关——只 text handler。
- 导入端 glob/整目录聚合——只认清单文件。
- 分片压缩、断点续导、清单校验和。
