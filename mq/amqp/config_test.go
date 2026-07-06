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

func TestTargetDirectQueue(t *testing.T) {
	// 空 exchange + 非空 routing_key:忽略原始 exchange,走默认交换机直投队列。
	d := &Driver{cfg: Config{Import: ImportConfig{RoutingKey: "orders"}}}
	ex, key := d.target(msgWithRoute("amq.topic", "orig.key"))
	if ex != "" || key != "orders" {
		t.Errorf("direct-queue = %q/%q, want \"\"/orders", ex, key)
	}
}

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
