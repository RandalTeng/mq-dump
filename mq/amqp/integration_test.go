//go:build integration

package amqp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	rabbit "github.com/rabbitmq/amqp091-go"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/internal/dump"
	"github.com/RandalTeng/mq-dump/internal/pipeline"
	"github.com/RandalTeng/mq-dump/mq"
)

// 需真实 RabbitMQ:`docker compose -f deploy/docker-compose.yaml up -d`,再 `go test -tags integration ./mq/amqp/`。
// 连接串来自 AMQP_URI,缺省 amqp://guest:guest@localhost:5672/。

func uri() string {
	if v := os.Getenv("AMQP_URI"); v != "" {
		return v
	}
	return "amqp://guest:guest@localhost:5672/"
}

// setup 建立一条连接与通道,声明一个非独占、非自动删除的临时队列(驱动用独立连接消费,
// 故不能 exclusive;导出后消费者断开仍需存活以便断言条数,故不能 auto-delete),返回队列名。
func setup(t *testing.T) (*rabbit.Connection, *rabbit.Channel, string) {
	t.Helper()
	conn, err := rabbit.Dial(uri())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	q, err := ch.QueueDeclare("", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	return conn, ch, q.Name
}

// seed 向默认 exchange 用 routing key = 队列名发布 n 条消息。
func seed(t *testing.T, ch *rabbit.Channel, queue string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		err := ch.PublishWithContext(context.Background(), "", queue, false, false,
			rabbit.Publishing{ContentType: "text/plain", Body: []byte{byte('A' + i)}})
		if err != nil {
			t.Fatalf("publish seed %d: %v", i, err)
		}
	}
	// 等待消息落入队列
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if queueLen(t, ch, queue) >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func queueLen(t *testing.T, ch *rabbit.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, false, true, true, false, nil)
	if err != nil {
		t.Fatalf("inspect queue: %v", err)
	}
	return q.Messages
}

func openDriver(t *testing.T, cfg Config, common config.Common) mq.Driver {
	t.Helper()
	cfg.Connection.URI = uri()
	d, err := factory{}.Open(common, &cfg)
	if err != nil {
		t.Fatalf("open driver: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func exportToBuf(t *testing.T, cfg Config, common config.Common) []byte {
	t.Helper()
	// 空闲超时确保 Export 在耗尽后返回
	if common.Timeout == 0 {
		common.Timeout = 2 * time.Second
	}
	d := openDriver(t, cfg, common)
	var buf bytes.Buffer
	w := dump.NewSingleWriter(&buf, "amqp")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := pipeline.Export(ctx, w, common.Count, d); err != nil {
		t.Fatalf("export: %v", err)
	}
	return buf.Bytes()
}

func countMessages(t *testing.T, b []byte) int {
	t.Helper()
	dec := dump.NewDecoder(bytes.NewReader(b))
	if _, err := dec.ReadMeta(); err != nil {
		t.Fatalf("read meta: %v", err)
	}
	n := 0
	for {
		_, ok, err := dec.Read()
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !ok {
			return n
		}
		n++
	}
}

// TestExportDrain:drain(默认)导出后队列被清空。
func TestExportDrain(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	seed(t, ch, queue, 5)

	b := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "drain"}}, config.Common{})
	if got := countMessages(t, b); got != 5 {
		t.Errorf("exported %d, want 5", got)
	}
	time.Sleep(300 * time.Millisecond)
	if got := queueLen(t, ch, queue); got != 0 {
		t.Errorf("queue has %d after drain, want 0", got)
	}
}

// TestExportRequeue:requeue 非破坏全导,导出后队列条数不变,dump 完整,不死循环。
func TestExportRequeue(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	seed(t, ch, queue, 5)

	b := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "requeue"}}, config.Common{})
	if got := countMessages(t, b); got != 5 {
		t.Errorf("exported %d msgs, want 5", got)
	}
	time.Sleep(300 * time.Millisecond)
	if got := queueLen(t, ch, queue); got != 5 {
		t.Errorf("queue has %d after requeue export, want 5", got)
	}
}

// TestExportRequeueCount:requeue + -n N(N=队列深度)。回归 pipeline 写满 -n 后
// cancel ctx、末条 republish 被打断而假报 "context canceled" 的 bug。exportToBuf
// 内部对 Export 返回的 err 直接 Fatalf,故导出成功即证明无假报错。
func TestExportRequeueCount(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	seed(t, ch, queue, 5)

	b := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "requeue"}}, config.Common{Count: 5})
	if got := countMessages(t, b); got != 5 {
		t.Errorf("exported %d msgs, want 5", got)
	}
	time.Sleep(300 * time.Millisecond)
	if got := queueLen(t, ch, queue); got != 5 {
		t.Errorf("queue has %d after requeue+count export, want 5", got)
	}
}

// TestExportPeek:peek 只取队头 N 条,非破坏,队列条数不变。
func TestExportPeek(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	seed(t, ch, queue, 5)

	b := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "peek"}}, config.Common{Count: 3})
	if got := countMessages(t, b); got != 3 {
		t.Errorf("peeked %d msgs, want 3", got)
	}
	time.Sleep(300 * time.Millisecond)
	if got := queueLen(t, ch, queue); got != 5 {
		t.Errorf("queue has %d after peek, want 5", got)
	}
}

// TestImportRoundTripWithRouting:导出后清空,导入(路由覆盖回同一队列)应恢复条数。
func TestImportRoundTripWithRouting(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	seed(t, ch, queue, 4)

	// drain 导出(清空队列)
	dumpBytes := exportToBuf(t, Config{Export: ExportConfig{Queue: queue, Mode: "drain"}}, config.Common{})
	if got := countMessages(t, dumpBytes); got != 4 {
		t.Fatalf("exported %d, want 4", got)
	}

	// 导入:路由覆盖到默认 exchange + routing key = 队列名(即回到本队列)
	importCfg := Config{Import: ImportConfig{Exchange: "", RoutingKey: queue, Confirm: true, Persistent: false}}
	d := openDriver(t, importCfg, config.Common{Concurrency: 4})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := pipeline.Import(ctx, dump.NewSingleReaderForTest(bytes.NewReader(dumpBytes)), "amqp", d); err != nil {
		t.Fatalf("import: %v", err)
	}

	// 断言:队列恢复 4 条
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if queueLen(t, ch, queue) >= 4 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := queueLen(t, ch, queue); got != 4 {
		t.Errorf("queue has %d after import, want 4", got)
	}
}

// TestSplitRoundTrip:拆分导出到临时目录(--split-count),再从清单聚合导入回同一队列。
func TestSplitRoundTrip(t *testing.T) {
	conn, ch, queue := setup(t)
	defer conn.Close()
	const n, k = 5, 2 // 5 条按每片 2 → 3 分片(2/2/1)
	seed(t, ch, queue, n)

	// 拆分 drain 导出到 <dir>/dump 基名
	dir := t.TempDir()
	stem := filepath.Join(dir, "dump")
	d := openDriver(t, Config{Export: ExportConfig{Queue: queue, Mode: "drain"}}, config.Common{Timeout: 2 * time.Second})
	w, err := dump.NewSplitWriter(stem, "amqp", k)
	if err != nil {
		t.Fatalf("split writer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := pipeline.Export(ctx, w, 0, d); err != nil {
		t.Fatalf("split export: %v", err)
	}

	// 校验清单分片数
	mf, err := os.Open(stem + ".mqdump.json")
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	man, err := dump.ReadManifest(mf)
	mf.Close()
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if man.Total != n || len(man.Parts) != 3 {
		t.Fatalf("manifest total=%d parts=%d, want %d/3", man.Total, len(man.Parts), n)
	}

	// 从清单聚合导入,路由回同一队列
	r, err := dump.OpenReader(stem + ".mqdump.json")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer r.Close()
	importCfg := Config{Import: ImportConfig{RoutingKey: queue, Confirm: true}}
	id := openDriver(t, importCfg, config.Common{Concurrency: 4})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	if err := pipeline.Import(ctx2, r, "amqp", id); err != nil {
		t.Fatalf("split import: %v", err)
	}

	// 断言:队列恢复 n 条
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if queueLen(t, ch, queue) >= n {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := queueLen(t, ch, queue); got != n {
		t.Errorf("queue has %d after split import, want %d", got, n)
	}
}
