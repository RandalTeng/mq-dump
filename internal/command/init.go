package command

import (
	"fmt"
	"os"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/mq"
)

// InitCmd 生成驱动配置模板。
type InitCmd struct {
	Output string `short:"o" help:"模板输出路径;缺省写 stdout"`
}

// Run 输出驱动配置模板。
func (c *InitCmd) Run(common *config.Common) error {
	f, ok := mq.Get(common.Driver)
	if !ok {
		return fmt.Errorf("unknown driver %q", common.Driver)
	}
	tpl := f.ConfigTemplate()
	if c.Output == "" || c.Output == "-" {
		_, err := fmt.Fprint(os.Stdout, tpl)
		return err
	}
	return os.WriteFile(c.Output, []byte(tpl), 0o644)
}
