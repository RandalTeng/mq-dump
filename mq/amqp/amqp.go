package amqp

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/model"
	"github.com/RandalTeng/mq-dump/mq"
)

//go:embed template.yaml
var configTemplate string

func init() { mq.Register("amqp", factory{}) }

type factory struct{}

func (factory) NewConfig() any         { return &Config{} }
func (factory) ConfigTemplate() string { return configTemplate }
func (factory) Open(c config.Common, cfg any) (mq.Driver, error) {
	ac, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("amqp: bad config type %T", cfg)
	}
	if ac.Connection.URI == "" {
		return nil, fmt.Errorf("amqp: connection.uri is required")
	}
	conn, err := amqp.Dial(ac.Connection.URI)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	slog.Info("amqp connected", "addr", sanitizeURI(ac.Connection.URI))
	return &Driver{conn: conn, cfg: *ac, common: c}, nil
}

// Driver 是 AMQP 驱动实例。
type Driver struct {
	conn   *amqp.Connection
	cfg    Config
	common config.Common
}

// Close 关闭底层连接。
func (d *Driver) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

// DumpName 实现 mq.Namer:无 -f 时默认 dump 基名 = 导出队列名。
func (d *Driver) DumpName() string { return d.cfg.Export.Queue }

// Export 按 cfg.Export.Mode 分派:drain / requeue / peek。
func (d *Driver) Export(ctx context.Context, emit func(model.Message) error) error {
	mode, err := resolveMode(d.cfg.Export.Mode)
	if err != nil {
		return err
	}
	slog.Info("amqp export start", "queue", d.cfg.Export.Queue, "mode", mode, "prefetch", d.prefetch())
	switch mode {
	case ModeRequeue:
		return d.exportRequeue(ctx, emit)
	case ModePeek:
		return d.exportPeek(ctx, emit)
	default:
		return d.exportDrain(ctx, emit)
	}
}

// prefetch 返回配置的预取量,<=0 时取默认 100。
func (d *Driver) prefetch() int {
	if d.cfg.Export.Prefetch > 0 {
		return d.cfg.Export.Prefetch
	}
	return 100
}

// consumeChannel 开 channel、设 Qos(prefetch)、以手动 ack 方式 consume 源队列。
// 返回 channel(调用方负责 Close)与投递流。
func (d *Driver) consumeChannel(prefetch int) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := d.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("qos: %w", err)
	}
	deliveries, err := ch.Consume(d.cfg.Export.Queue, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("consume %q: %w", d.cfg.Export.Queue, err)
	}
	return ch, deliveries, nil
}

// exportDrain 破坏性抽干:emit 落盘后 ack;队列取空(idle timeout)或到 Count 即止。
func (d *Driver) exportDrain(ctx context.Context, emit func(model.Message) error) error {
	ch, deliveries, err := d.consumeChannel(d.prefetch())
	if err != nil {
		return err
	}
	defer ch.Close()
	idle := d.common.Timeout
	var count int
	timer := newIdleTimer(idle)
	defer timer.stop()
	for {
		timer.reset(idle)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
			return nil
		case dv, ok := <-deliveries:
			if !ok {
				return nil
			}
			if err := emit(deliveryToMessage(dv)); err != nil {
				_ = dv.Nack(false, true)
				return err
			}
			_ = dv.Ack(false)
			count++
			if d.common.Count > 0 && count >= d.common.Count {
				return nil
			}
		}
	}
}

// exportRequeue 非破坏全导:起始读队列深度快照 N;逐条 emit 落盘后,
// 先把副本重发回原队(默认交换机+队列名)并等 confirm,再 ack 原件;导满 N 或 Count 即止。
// 须先停该队列其他消费者(见 template.yaml 警告)。
func (d *Driver) exportRequeue(ctx context.Context, emit func(model.Message) error) error {
	ch, err := d.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()
	if err := ch.Qos(d.prefetch(), 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("confirm mode: %w", err)
	}
	// 起始快照:必须在 Consume 之前读,否则消息被预取为 unacked,ready 计数归零。
	// 该条数作为终止上界,防止导入自己刚重发的副本。
	q, err := ch.QueueDeclarePassive(d.cfg.Export.Queue, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("inspect queue %q: %w", d.cfg.Export.Queue, err)
	}
	limit := q.Messages
	if d.common.Count > 0 && d.common.Count < limit {
		limit = d.common.Count
	}
	if limit == 0 {
		return nil
	}
	deliveries, err := ch.Consume(d.cfg.Export.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %q: %w", d.cfg.Export.Queue, err)
	}
	idle := d.common.Timeout
	var count int
	timer := newIdleTimer(idle)
	defer timer.stop()
	for {
		timer.reset(idle)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
			return nil
		case dv, ok := <-deliveries:
			if !ok {
				return nil
			}
			m := deliveryToMessage(dv)
			if err := emit(m); err != nil {
				_ = dv.Nack(false, true)
				return err
			}
			// 先重发副本回原队并 confirm,成功后再 ack 原件(顺序不可反,防丢)。
			if err := d.republish(ctx, ch, m); err != nil {
				_ = dv.Nack(false, true)
				return err
			}
			_ = dv.Ack(false)
			count++
			if count >= limit {
				return nil
			}
		}
	}
}

// republish 把消息重发回被导出队列本身:默认交换机 "" + routingKey=队列名,
// 只投该队列、不经原 exchange、不扇出;等 publisher confirm 成功。
func (d *Driver) republish(ctx context.Context, ch *amqp.Channel, m model.Message) error {
	pub := messageToPublishing(m)
	queue := d.cfg.Export.Queue
	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, pub)
	if err != nil {
		return fmt.Errorf("requeue to %q: %w", queue, err)
	}
	if confirm != nil {
		if ok := confirm.Wait(); !ok {
			return fmt.Errorf("requeue to %q nacked by broker", queue)
		}
	}
	return nil
}

// exportPeek 非破坏抽样:仅取队头 n 条(Count>0 用 Count,否则用 prefetch),
// 全程不 ack/nack;读满 n 或 idle 后一次性 nack(requeue) 释放。仅碰队头 n 条,影响极小。
func (d *Driver) exportPeek(ctx context.Context, emit func(model.Message) error) error {
	n := d.common.Count
	if n <= 0 {
		n = d.prefetch()
	}
	// prefetch 卡在 n:broker 最多投 n 条未确认,天然不会给第 n+1 条。
	ch, deliveries, err := d.consumeChannel(n)
	if err != nil {
		return err
	}
	defer ch.Close()
	idle := d.common.Timeout
	var count int
	var lastTag uint64
	var held bool
	// 收尾:一次性把持有的未确认消息 requeue 回队。
	defer func() {
		if held {
			_ = ch.Nack(lastTag, true, true)
		}
	}()
	timer := newIdleTimer(idle)
	defer timer.stop()
	for {
		timer.reset(idle)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
			return nil
		case dv, ok := <-deliveries:
			if !ok {
				return nil
			}
			if err := emit(deliveryToMessage(dv)); err != nil {
				return err // defer 的 Nack multiple 会连同本条一起 requeue
			}
			lastTag = dv.DeliveryTag
			held = true
			count++
			if count >= n {
				return nil
			}
		}
	}
}

// Import 用 Workers() 个 worker 并发 publish,每 worker 独立 channel。
func (d *Driver) Import(ctx context.Context, next func() (model.Message, bool, error)) error {
	n := d.common.Workers()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < n; i++ {
		g.Go(func() error {
			ch, err := d.conn.Channel()
			if err != nil {
				return fmt.Errorf("open channel: %w", err)
			}
			defer ch.Close()
			if d.cfg.Import.Confirm {
				if err := ch.Confirm(false); err != nil {
					return fmt.Errorf("confirm mode: %w", err)
				}
			}
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				m, ok, err := next()
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
				if err := d.publish(ctx, ch, m); err != nil {
					return err
				}
			}
		})
	}
	return g.Wait()
}

func (d *Driver) publish(ctx context.Context, ch *amqp.Channel, m model.Message) error {
	ex, key := d.target(m)
	pub := messageToPublishing(m)
	if d.cfg.Import.Persistent {
		pub.DeliveryMode = amqp.Persistent
	}
	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, ex, key, d.cfg.Import.Mandatory, false, pub)
	if err != nil {
		return fmt.Errorf("publish to %q/%q: %w", ex, key, err)
	}
	if d.cfg.Import.Confirm && confirm != nil {
		if ok := confirm.Wait(); !ok {
			return fmt.Errorf("publish to %q/%q nacked by broker", ex, key)
		}
	}
	return nil
}

// sanitizeURI 仅保留 host:port,剥除账号口令,避免日志泄露凭据。
func sanitizeURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return "?"
	}
	return u.Host
}

// idleTimer 是可复用的空闲计时器;idle<=0 时 C() 返回 nil(永不触发)。
type idleTimer struct{ t *time.Timer }

func newIdleTimer(idle time.Duration) idleTimer {
	if idle <= 0 {
		return idleTimer{}
	}
	return idleTimer{t: time.NewTimer(idle)}
}

func (i idleTimer) C() <-chan time.Time {
	if i.t == nil {
		return nil
	}
	return i.t.C
}

func (i idleTimer) reset(idle time.Duration) {
	if i.t == nil {
		return
	}
	if !i.t.Stop() {
		select {
		case <-i.t.C:
		default:
		}
	}
	i.t.Reset(idle)
}

func (i idleTimer) stop() {
	if i.t != nil {
		i.t.Stop()
	}
}
