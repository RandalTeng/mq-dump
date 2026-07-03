package dump

import (
	"bytes"
	"testing"

	"github.com/RandalTeng/mq-dump/model"
)

func TestEncoderDecoderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, "amqp")
	if err := enc.WriteMeta(); err != nil {
		t.Fatal(err)
	}
	msgs := []model.Message{{Body: []byte("m1")}, {Body: []byte("m2")}}
	for _, m := range msgs {
		if err := enc.Write(m); err != nil {
			t.Fatal(err)
		}
	}

	dec := NewDecoder(bytes.NewReader(buf.Bytes()))
	meta, err := dec.ReadMeta()
	if err != nil {
		t.Fatal(err)
	}
	if meta.Driver != "amqp" {
		t.Errorf("meta driver = %q", meta.Driver)
	}
	var got []model.Message
	for {
		m, ok, err := dec.Read()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, m)
	}
	if len(got) != 2 || string(got[0].Body) != "m1" || string(got[1].Body) != "m2" {
		t.Errorf("got %d msgs: %+v", len(got), got)
	}
}

func TestDecoderMissingMeta(t *testing.T) {
	dec := NewDecoder(bytes.NewReader([]byte(`{"body":"eA=="}` + "\n")))
	if _, err := dec.ReadMeta(); err == nil {
		t.Error("first line without format_version should error")
	}
}
