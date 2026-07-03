package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/internal/pipeline"
	"github.com/RandalTeng/mq-dump/mq"
)

// ExportCmd 导出:通用 flag + 驱动 YAML。
type ExportCmd struct{}

// Run 执行导出。
func (c *ExportCmd) Run(common *config.Common) error {
	closer, err := setupLogger(common.LogLevel, common.LogFile)
	if err != nil {
		return err
	}
	defer closer()

	f, ok := mq.Get(common.Driver)
	if !ok {
		return fmt.Errorf("unknown driver %q", common.Driver)
	}
	cfg := f.NewConfig()
	if err := config.LoadDriverYAML(common.Config, cfg); err != nil {
		return err
	}
	d, err := f.Open(*common, cfg)
	if err != nil {
		return err
	}
	defer d.Close()

	namer, _ := d.(mq.Namer)
	w, err := resolveExportWriter(common, common.Driver, namer)
	if err != nil {
		return err
	}
	defer w.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return pipeline.Export(ctx, w, common.Count, d)
}
