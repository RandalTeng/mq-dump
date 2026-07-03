package amqp

import (
	"testing"

	"github.com/goccy/go-json"

	"github.com/RandalTeng/mq-dump/model"
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
