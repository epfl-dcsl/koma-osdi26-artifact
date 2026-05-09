# Koma OSDI Benchmark Harness

This directory contains the benchmark harness used by the OSDI artifact. It is
trimmed to the paper performance reproduction matrix documented in
`../plan/agents/reproduction-matrix.md`.

## Layout

- `exp_config/`: YAML run parameters for paper-required experiments.
- `fabs/`: Fabric tasks for CloudLab setup and benchmark execution.
- `plot/`: Paper-style TikZ/LaTeX figure rendering from existing CSV inputs.
- `servers/`: Synthetic, TLS, gRPC, and Silo server sources.

The harness expects the artifact repository layout:

- Koma sources in `../koma/plain-tcp`, `../koma/grpc`, and `../koma/tls`.
- Dependencies in `../third_party`.
- Generated outputs in `../results`.

## Entry Point

Use `fabs/run_all.sh` to list or launch supported experiment tasks:

```bash
bench/fabs/run_all.sh --list
```

## Plotting

The benchmark harness also includes a simple paper-style plotting flow under
`plot/`.

Examples:

```bash
python3 bench/plot/render.py --list
python3 bench/plot/render.py --check
python3 bench/plot/render.py --prepare-only
python3 bench/plot/render.py synthetic_100us synthetic_grpc
```

The renderer uses the newest timestamped sanitized CSV under
`results/<ExperimentType>/` for each paper figure input and falls back to
header-only placeholders when data is missing. See `plot/README.md` for the
exact mapping and optional auxiliary local inputs.

For PDF compilation, install a system LaTeX toolchain such as:

```bash
sudo apt install latexmk texlive-latex-extra texlive-pictures texlive-science texlive-extra-utils ghostscript
```

`texlive-extra-utils` provides `pdfcrop`, which the renderer uses to trim the
generated standalone figure PDFs to their actual content.
