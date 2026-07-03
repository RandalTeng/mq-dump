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

// Export 从队列 consume,逐条 emit;emit 成功后按配置 ack/nack。
func (d *Driver) Export(ctx context.Context, emit func(model.Message) error) error {
	ch, err := d.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()
	prefetch := d.cfg.Export.Prefetch
	if prefetch <= 0 {
		prefetch = 100
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	deliveries, err := ch.Consume(d.cfg.Export.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %q: %w", d.cfg.Export.Queue, err)
	}
	slog.Info("amqp export start", "queue", d.cfg.Export.Queue, "prefetch", prefetch, "ack", d.cfg.Export.Ack)
	idle := d.common.Timeout
	var count int
	for {
		var timeout <-chan time.Time
		if idle > 0 {
			t := time.NewTimer(idle)
			timeout = t.C
			defer t.Stop()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-timeout:
			return nil
		case dv, ok := <-deliveries:
			if !ok {
				return nil
			}
			if err := emit(deliveryToMessage(dv)); err != nil {
				_ = dv.Nack(false, true)
				return err
			}
			if d.cfg.Export.Ack {
				_ = dv.Ack(false)
			} else {
				_ = dv.Nack(false, true)
			}
			count++
			if d.common.Count > 0 && count >= d.common.Count {
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
