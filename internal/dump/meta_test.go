package dump

import (
	"testing"

	"github.com/goccy/go-json"
)

func TestMetaRoundTrip(t *testing.T) {
	m := Meta{FormatVersion: 1, Driver: "amqp", CreatedAt: "2026-07-02T12:00:00Z"}
	b, _ := json.Marshal(m)
	var got Meta
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got != m {
		t.Errorf("got %+v want %+v", got, m)
	}
}

func TestMetaCheckDriver(t *testing.T) {
	m := Meta{FormatVersion: 1, Driver: "amqp"}
	if err := m.CheckDriver("amqp"); err != nil {
		t.Errorf("same driver should pass: %v", err)
	}
	if err := m.CheckDriver("kafka"); err == nil {
		t.Error("mismatched driver should error")
	}
}
