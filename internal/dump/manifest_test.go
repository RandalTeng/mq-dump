package dump

import (
	"bytes"
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	m := Manifest{
		FormatVersion: FormatVersion,
		Driver:        "amqp",
		CreatedAt:     "2026-07-03T00:00:00Z",
		Parts:         []Part{{File: "orders-000.jsonl", Count: 3}, {File: "orders-001.jsonl", Count: 2}},
		Total:         5,
	}
	var buf bytes.Buffer
	if err := WriteManifest(&buf, m); err != nil {
		t.Fatal(err)
	}
	if bytes.Count(buf.Bytes(), []byte("\n")) != 1 { // 单行 JSON + 结尾换行
		t.Errorf("manifest not single-line: %q", buf.String())
	}
	got, err := ReadManifest(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 5 || len(got.Parts) != 2 || got.Parts[1].File != "orders-001.jsonl" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}
