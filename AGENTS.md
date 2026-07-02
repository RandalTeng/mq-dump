# Repository Guidelines

> Status: greenfield. The repo currently has no code — this document defines the
> **target** architecture and conventions to build against. Paths marked *(planned)*
> do not exist yet; create them as described so the layout stays coherent.

## Development Workflow (MUST)

Two mandatory confirmation gates — the assistant NEVER skips either:

1. **Before changing code**: propose the change (files, approach, diff intent) and
   get the developer's explicit confirmation. Do NOT edit source files until
   confirmed.
2. **Before committing**: after code is written and confirmed, do NOT run
   `git commit` automatically. Present what will be committed and wait for the
   developer's explicit go-ahead.

Read-only actions (reading files, running tests/build/lint, searching) need no
confirmation. These gates apply to source code; docs/specs still follow the same
"confirm before commit" rule.

## Project Overview

`amqp-dump` is a Go CLI tool that **exports** messages from a message broker to a
portable dump file and **imports** them back. It ships with an **AMQP** driver
(RabbitMQ) and is structured so Kafka, RocketMQ, and other brokers can be added
without touching the core. Behavior is driven by **command-line flags and/or a
config file**, with flags taking precedence.

## Architecture & Data Flow

Pluggable-driver design. The core owns config, the dump format, and the
export/import pipelines; each broker is an interchangeable implementation behind
one interface.

```
                 flags + config file
                          |
                     internal/config
                          |
   export:  Driver.Consume() ->[]Message-> encode -> dump file (JSONL/stdout)
   import:  dump file -> decode ->[]Message-> Driver.Publish()
                          |
        internal/mq (Driver interface + registry)
             |            |             |
          amqp/       kafka/(planned)  rocketmq/(planned)
```

- **Export**: load config -> open `Driver` -> stream messages -> serialize each to
  the dump file (one JSON object per line).
- **Import**: read dump file -> deserialize each line -> open `Driver` -> publish.
- Brokers are selected by name/scheme via a **registry**; adding one means
  implementing `Driver` and registering it — no changes to `cmd/` or the pipeline.
- All broker I/O takes a `context.Context` and honors cancellation for clean
  shutdown mid-stream.

## Key Directories

*(target layout — create as needed)*

- `cmd/amqp-dump/` — CLI entry point; flag/subcommand parsing only, no business logic.
- `internal/mq/` — `Driver` interface + registry (`driver.go`).
- `internal/mq/amqp/` — AMQP/RabbitMQ driver.
- `internal/config/` — config struct, file loading, flag binding, precedence rules.
- `internal/model/` — serializable `Message` (headers, routing key, exchange, body, timestamp).
- `testdata/` — sample dump files and config fixtures for tests.

## Development Commands

```bash
go mod tidy                                  # sync dependencies
go build -o bin/amqp-dump ./cmd/amqp-dump    # build binary
go run ./cmd/amqp-dump export \              # run export
  --uri amqp://guest:guest@localhost:5672/ --queue orders --out dump.jsonl
go run ./cmd/amqp-dump import \              # run import
  --uri amqp://guest:guest@localhost:5672/ --exchange orders --in dump.jsonl
go run ./cmd/amqp-dump --config config.yaml export   # via config file
go test ./...                                # unit tests
go vet ./...                                 # static checks
gofmt -l .                                   # list unformatted files (should be empty)
```

Adopt `golangci-lint run` once a `.golangci.yml` exists.

## Code Conventions & Common Patterns

- **Formatting**: `gofmt`/`goimports`, tabs for indentation. CI must reject unformatted code.
- **Naming**: packages lowercase, single word, no underscores; exported identifiers
  carry doc comments that begin with the identifier name.
- **Errors**: return, don't panic in library code; wrap with context via
  `fmt.Errorf("consume %q: %w", queue, err)`; handle/log only at `main`.
- **Context**: `ctx context.Context` is the first parameter of every I/O method;
  respect `ctx.Done()` in streaming loops.
- **Driver plugin pattern**: define the `Driver` interface once in `internal/mq`,
  register implementations in `init()`, look them up by name. Never special-case a
  broker in the core pipeline.
- **Config precedence**: flags > environment > config file > defaults. Use struct
  tags for the file format; keep one config struct as the single source of truth.
- **Concurrency**: stream messages over channels; bound throughput with a prefetch
  / worker limit; always propagate cancellation.

## Important Files

*(planned — the ones an assistant will touch first)*

- `go.mod` — module path (e.g. `github.com/randal/amqp-dump`) and Go version.
- `cmd/amqp-dump/main.go` — CLI wiring and subcommand dispatch.
- `internal/mq/driver.go` — `Driver` interface + registry; the core extension point.
- `internal/mq/amqp/amqp.go` — reference driver implementation.
- `internal/config/config.go` — config schema and loading/precedence logic.
- `internal/model/message.go` — dump record shape (the on-disk contract).
- `config.example.yaml` — documented sample configuration.

## Runtime/Tooling Preferences

- **Language/runtime**: Go 1.22+ with Go modules (no `GOPATH` layout).
- **Package manager**: `go mod` only; pin dependency versions in `go.mod`/`go.sum`.
- **Suggested deps**: `github.com/rabbitmq/amqp091-go` (AMQP). Prefer stdlib
  `flag` + `encoding/json`/`gopkg.in/yaml.v3` for config; add `cobra`/`viper` only
  if subcommand/config needs outgrow the stdlib.
- **External runtime**: none beyond a reachable broker; the binary is self-contained.

## Testing & QA

- **Framework**: stdlib `testing`, table-driven tests; keep fixtures in `testdata/`.
- **Unit tests**: exercise config precedence, dump encode/decode round-trips, and
  registry lookup against a **fake `Driver`** — no live broker required.
- **Integration tests**: gate behind `//go:build integration` and run against a real
  broker (docker-compose or `testcontainers`); keep them out of the default `go test`.
- **Gates before merge**: `go build ./...`, `go test ./...`, `go vet ./...`, and a
  clean `gofmt -l .`.
