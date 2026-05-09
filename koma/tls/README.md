# Koma — TLS (kTLS)

This directory contains the Koma kernel module source for the **kTLS**
configuration. It is the same `AF_KOMA` module as `../plain-tcp/`, with
additional hooks for in-kernel TLS.

This variant is the baseline used for the kTLS figures
(`fig:ktls_eval`, TLS rows of `fig:silo`).

## Layout

- `Makefile`, `koma.h`, `komasock.c`, `strparser.[ch]`, `tcp_send.[ch]`,
  `tls.h` — kernel-module sources.

The kernel patches needed for the kTLS variant live one level up at
[`../patches/`](../patches/):

- `koma.patch` — `AF_KOMA` address family + UAPI header + hooks (shared
  with the plain-TCP and gRPC variants).
- `ktls-module.patch` — additional patch to `net/tls/tls_sw.c` required by
  Koma's interaction with kTLS.

Both patches are applied together by `cloudlab-patch-koma-kernel` before
the kernel is built; the resulting `tls.ko` is installed via the standard
`make modules_install`. `cloudlab-verify-ktls-module` confirms that the
`tls` module loaded after reboot is the patched build.

## Build

Out-of-tree, on the server, after booting `6.8.0-koma`:

```bash
cd koma/tls
make
ls koma.ko
```

See [../../bench/README.md](../../bench/README.md) for the full
reproduction flow.
