#!/bin/sh
# mq-dump 容器入口。
# 规则:
#   - 无参数            → mq-dump --help(由 Dockerfile CMD 提供)
#   - 首参是 mq-dump 子命令(export/import/init)或以 '-' 开头的 flag
#                       → 透传给 mq-dump(自定义命令)
#   - 首参是可执行文件 / sh / bash 等
#                       → 直接 exec 该命令(保留 shell 能力,便于调试)
set -e

# 无参数:交给 CMD(--help)
if [ "$#" -eq 0 ]; then
	exec mq-dump
fi

case "$1" in
	export|import|init|-*)
		# mq-dump 子命令或 flag,透传
		exec mq-dump "$@"
		;;
	mq-dump)
		# 显式带二进制名,剥掉后透传
		shift
		exec mq-dump "$@"
		;;
	*)
		# 其余:若是可执行命令则直接运行(sh/bash/自定义命令),否则回退给 mq-dump
		if command -v "$1" >/dev/null 2>&1; then
			exec "$@"
		fi
		exec mq-dump "$@"
		;;
esac
