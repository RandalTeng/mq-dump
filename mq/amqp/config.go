package amqp

import (
	"fmt"

	"github.com/goccy/go-json"

	"github.com/RandalTeng/mq-dump/model"
)

// Config 是 AMQP 驱动私有配置(仅从 --config YAML 解析)。
type Config struct {
	Connection ConnConfig   `yaml:"connection"`
	Export     ExportConfig `yaml:"export"`
	Import     ImportConfig `yaml:"import"`
}

// ConnConfig 是连接参数。
type ConnConfig struct {
	URI string `yaml:"uri"`
}

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

// ExportConfig 是导出参数。
type ExportConfig struct {
	Queue    string `yaml:"queue"`
	Mode     string `yaml:"mode"` // drain(默认) | requeue | peek
	Prefetch int    `yaml:"prefetch"`
}

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

// ImportConfig 是导入参数(含路由覆盖)。
type ImportConfig struct {
	Exchange   string `yaml:"exchange"`
	RoutingKey string `yaml:"routing_key"`
	Persistent bool   `yaml:"persistent"`
	Confirm    bool   `yaml:"confirm"`
	Mandatory  bool   `yaml:"mandatory"`
}

// target 决定导入目标(按优先级互斥判定):
//   - Import.Exchange 非空 → 覆盖 exchange;且 Import.RoutingKey 非空时一并覆盖 key(否则保留原始 key);
//   - 否则 Import.RoutingKey 非空 → 走默认交换机 "" 直投队列(routing key=队列名),忽略消息原始 exchange(与 export requeue 同机制);
//   - 否则(两者皆空)→ 用消息原始路由。
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
