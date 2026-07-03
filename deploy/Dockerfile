# ---- builder ----
FROM golang:1.25-alpine AS builder

WORKDIR /src

# 先拉依赖,利用层缓存
COPY go.mod go.sum ./
RUN go mod download

# 再拷源码构建静态二进制
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mq-dump ./cmd/mq-dump

# ---- runtime ----
# 用 alpine 而非 scratch:保留 shell,支持自定义命令与调试
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /out/mq-dump /usr/local/bin/mq-dump
COPY deploy/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

WORKDIR /work

# entrypoint 决定:参数透传给 mq-dump,或落回 shell
ENTRYPOINT ["docker-entrypoint.sh"]
# 缺省显示帮助
CMD ["--help"]
