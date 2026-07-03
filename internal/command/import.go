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

// ImportCmd 导入:通用 flag + 驱动 YAML。
type ImportCmd struct{}

// Run 执行导入。
func (c *ImportCmd) Run(common *config.Common) error {
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

	r, err := openDumpReader(common.DumpFile)
	if err != nil {
		return err
	}
	defer r.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return pipeline.Import(ctx, r, common.Driver, d)
}
