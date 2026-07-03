package pipeline

import (
	"bytes"
	"context"
	"testing"

	"github.com/RandalTeng/mq-dump/internal/dump"
	"github.com/RandalTeng/mq-dump/model"
)

// fakeDriver:Export 产出 N 条,Import 收集。
type fakeDriver struct {
	out []model.Message
	in  []model.Message
}

func (f *fakeDriver) Export(ctx context.Context, emit func(model.Message) error) error {
	for _, m := range f.out {
		if err := emit(m); err != nil {
			return err
		}
	}
	return nil
}
func (f *fakeDriver) Import(ctx context.Context, next func() (model.Message, bool, error)) error {
	for {
		m, ok, err := next()
		if err != nil || !ok {
			return err
		}
		f.in = append(f.in, m)
	}
}
func (f *fakeDriver) Close() error { return nil }

func TestExportThenImportRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	src := &fakeDriver{out: []model.Message{{Body: []byte("m1")}, {Body: []byte("m2")}}}
	if err := Export(context.Background(), &buf, "amqp", 0, src); err != nil {
		t.Fatal(err)
	}

	dst := &fakeDriver{}
	if err := Import(context.Background(), bytes.NewReader(buf.Bytes()), "amqp", dst); err != nil {
		t.Fatal(err)
	}
	if len(dst.in) != 2 || string(dst.in[0].Body) != "m1" {
		t.Errorf("imported %d: %+v", len(dst.in), dst.in)
	}
}

func TestExportCountLimit(t *testing.T) {
	var buf bytes.Buffer
	src := &fakeDriver{out: []model.Message{{Body: []byte("1")}, {Body: []byte("2")}, {Body: []byte("3")}}}
	if err := Export(context.Background(), &buf, "amqp", 2, src); err != nil {
		t.Fatal(err)
	}
	dec := dump.NewDecoder(bytes.NewReader(buf.Bytes()))
	_, _ = dec.ReadMeta()
	var n int
	for {
		_, ok, _ := dec.Read()
		if !ok {
			break
		}
		n++
	}
	if n != 2 {
		t.Errorf("wrote %d msgs, want 2 (count limit)", n)
	}
}

func TestImportDriverMismatch(t *testing.T) {
	var buf bytes.Buffer
	_ = Export(context.Background(), &buf, "amqp", 0, &fakeDriver{})
	if err := Import(context.Background(), bytes.NewReader(buf.Bytes()), "kafka", &fakeDriver{}); err == nil {
		t.Error("driver mismatch should error")
	}
}
