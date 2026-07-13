# Rakaia — TLS (kTLS)

This directory contains the Rakaia kernel module source for the **kTLS**
configuration. It is the same `AF_RAKAIA` module as `../plain-tcp/`, with
additional hooks for in-kernel TLS.

This variant is the baseline used for the kTLS figures (Figure 10 and the
TLS rows of Figure 11).

## Layout

- `Makefile`, `rakaia.h`, `rakaiasock.c`, `strparser.[ch]`, `tcp_send.[ch]`,
  `tls.h` — kernel-module sources.

The kernel patches needed for the kTLS variant live one level up at
[`../patches/`](../patches/):

- `rakaia.patch` — `AF_RAKAIA` address family + UAPI header + hooks (shared
  with the plain-TCP and gRPC variants).
- `ktls-module.patch` — additional patch to `net/tls/tls_sw.c` required by
  Rakaia's interaction with kTLS.

Both patches are applied together by `bench/fabs/run_all.sh --patch-kernel`
before the kernel is built; the resulting `tls.ko` is installed via the
standard `make modules_install`. `bench/fabs/run_all.sh --verify-ktls`
confirms that the `tls` module loaded after reboot is the patched build.

## Build

Out-of-tree, on the server, after booting `6.8.0-rakaia`:

```bash
cd rakaia/tls
make
ls rakaia.ko
```

See [../../bench/README.md](../../bench/README.md) for the full
reproduction flow.
