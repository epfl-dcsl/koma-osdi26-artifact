# Rakaia — gRPC

This directory contains the Rakaia kernel module source for the **gRPC**
configuration. It is the same `AF_RAKAIA` module as `../plain-tcp/`, with
additional hooks needed by the Go-side gRPC integration.

This variant is the baseline used for the gRPC figures (Figure 9 and the
gRPC rows of Figure 11).

## Layout

- `Makefile`, `rakaia.h`, `rakaiasock.c`, `strparser.[ch]`, `tcp_send.[ch]`,
  `logging.h` — kernel-module sources.
- `reload.sh` — convenience script: `make`, `rmmod rakaia`, `insmod rakaia.ko`,
  then tail `dmesg` for `strp` lines. Useful when iterating on the module
  by hand.

Kernel patches are shared across all variants and live at
[`../patches/`](../patches/) (applied together by
`bench/fabs/run_all.sh --patch-kernel`).

## Companion Go modules

The gRPC reproduction also requires patched Go modules pinned in
`bench/fabs/fab_utils.py`:

- `github.com/rainayangg/grpc-go v0.0.0-20260325234553-a764f0be0b0d`
- `github.com/rainayangg/net-go v0.0.0-20260313180441-b5081d74e7cf`

The harness rewrites the server's `go.mod` to point at these forks when a
`grpc-rakaia-go` (or `silo-grpc-rakaia-go`) server config is selected; the
unmodified `grpc-go` config uses upstream `google.golang.org/grpc`.

## Build

Same out-of-tree flow as the plain-TCP variant — must run on the server
after booting `6.8.0-rakaia`:

```bash
cd rakaia/grpc
make
ls rakaia.ko
```

See [../../bench/README.md](../../bench/README.md) for the full
reproduction flow.
