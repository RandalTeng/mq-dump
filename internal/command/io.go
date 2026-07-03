package command

import (
	"fmt"
	"os"
	"strings"

	"github.com/RandalTeng/mq-dump/config"
	"github.com/RandalTeng/mq-dump/internal/dump"
	"github.com/RandalTeng/mq-dump/mq"
)

// resolveExportWriter 依据 -f / 拆分 / 驱动 Namer 决定导出 Writer。
//
//	-f -             → stdout(single;拆分下报错)
//	-f <path>        → 该路径(去 .jsonl 得基名)
//	-f 空 + Namer    → <DumpName()>
//	-f 空 + 无 Namer → 报错
func resolveExportWriter(c *config.Common, driver string, namer mq.Namer) (dump.Writer, error) {
	if c.DumpFile == "-" {
		if c.SplitCount > 0 {
			return nil, fmt.Errorf("拆分导出不支持写 stdout")
		}
		return dump.NewSingleWriter(os.Stdout, driver), nil
	}
	stem := strings.TrimSuffix(c.DumpFile, ".jsonl")
	if stem == "" {
		if namer == nil || namer.DumpName() == "" {
			return nil, fmt.Errorf("未指定 -f 且驱动 %q 无默认名,请用 -f 指定输出", driver)
		}
		stem = namer.DumpName()
	}
	if c.SplitCount > 0 {
		return dump.NewSplitWriter(stem, driver, c.SplitCount)
	}
	f, err := os.Create(stem + ".jsonl")
	if err != nil {
		return nil, err
	}
	return dump.NewSingleWriterFile(f, driver), nil
}
