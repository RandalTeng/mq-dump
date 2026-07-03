package amqp

import (
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/RandalTeng/mq-dump/mq"
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
