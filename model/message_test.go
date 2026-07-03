package model

import (
	"testing"
	"time"

	"github.com/goccy/go-json"
)

func TestMessageJSONRoundTrip(t *testing.T) {
	in := Message{
		Body:       []byte("hello"),
		Timestamp:  time.Unix(1700000000, 0).UTC(),
		Properties: json.RawMessage(`{"exchange":"a"}`),
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Message
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(out.Body) != "hello" {
		t.Errorf("body = %q, want hello", out.Body)
	}
	if !out.Timestamp.Equal(in.Timestamp) {
		t.Errorf("ts = %v, want %v", out.Timestamp, in.Timestamp)
	}
	if string(out.Properties) != `{"exchange":"a"}` {
		t.Errorf("props = %s", out.Properties)
	}
}
