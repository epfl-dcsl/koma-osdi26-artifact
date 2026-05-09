# Koma — Plain TCP

This directory contains the Koma kernel module source for the **plain-TCP**
configuration. Koma is a network-stack architecture that enables message
scheduling for TCP traffic without changing the TCP layer itself; it is
implemented as an out-of-tree Linux kernel module that registers a new
`AF_KOMA` address family.

This variant is the baseline used for the synthetic-TCP figures
(Figure 7, Figure 8) and the non-TLS Silo runs (Figure 11).

## Layout

- `Makefile`, `koma.h`, `komasock.c`, `strparser.[ch]`, `tcp_send.[ch]` —
  kernel-module sources. The module builds out-of-tree against
  `/lib/modules/$(uname -r)/build`.

The kernel patches required by all variants live one level up at
[`../patches/`](../patches/) — `koma.patch` adds
`AF_KOMA`/`PF_KOMA`/`SOL_KOMA` plus the hooks in `tcp.c`, `tcp_output.c`,
`kcmsock.c`, `socket.c`, `strparser.c`, and selinux that the module
relies on; `ktls-module.patch` adds the in-kernel TLS hooks needed by
the kTLS variant. Both are applied together by `bench/fabs/run_all.sh --patch-kernel`.

## Build

The module must be built **on the server**, **after** booting into the
patched `6.8.0-koma` kernel produced by `bench/fabs/run_all.sh --patch-kernel`.

```bash
cd koma/plain-tcp
make
ls koma.ko    # produced here
```

The harness loads/unloads the module automatically per experiment via
`insmod ./koma.ko` / `rmmod koma`; manual usage is the same.

## How this is used by the artifact

The kernel patch is applied by `bench/fabs/run_all.sh --patch-kernel`,
which invokes the underlying fab task in
[../../bench/fabs/fabfile.py](../../bench/fabs/fabfile.py). The module
produced by `make` here is loaded by the harness whenever a `koma`-family
server config (`koma`, `silo-koma`, …) is selected. See
[../../bench/README.md](../../bench/README.md) for the full reproduction
flow.
