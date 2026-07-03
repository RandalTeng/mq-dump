package command

import (
	"github.com/alecthomas/kong"
)

// Run 解析命令行参数并执行对应子命令。
//
// 行为:
//   - 无参数时自动注入 --help,打印帮助并以 0 退出;
//   - 参数错误(未知 flag/命令、缺失必填项等)时打印用法帮助与错误信息,
//     并以非零码退出(kong 的 usage-error 约定)。
//
// opts 追加在内置选项之后,供测试注入 kong.Writers / kong.Exit 等。
func Run(args []string, opts ...kong.Option) {
	// 无参数 → 等价于用户显式输入 --help。
	if len(args) == 0 {
		args = []string{"--help"}
	}

	var cli CLI
	options := append([]kong.Option{
		kong.Name("mq-dump"),
		kong.Description("消息队列导入/导出工具(v1: AMQP)"),
		kong.UsageOnError(),
	}, opts...)

	parser := kong.Must(&cli, options...)
	kctx, err := parser.Parse(args)
	parser.FatalIfErrorf(err) // 参数错误:打印用法+错误后退出
	kctx.FatalIfErrorf(kctx.Run(&cli.Common))
}
