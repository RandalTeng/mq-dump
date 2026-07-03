package amqp

import (
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

// ExportConfig 是导出参数。
type ExportConfig struct {
	Queue    string `yaml:"queue"`
	Ack      bool   `yaml:"ack"`
	Prefetch int    `yaml:"prefetch"`
}

// ImportConfig 是导入参数(含路由覆盖)。
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
