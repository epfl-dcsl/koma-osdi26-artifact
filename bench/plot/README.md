# Plotting

This directory renders paper-style evaluation figures from the artifact repo's
own benchmark results.

## Inputs

For each experiment type, the renderer looks for:

`results/<ExperimentType>/<YYYYMMDD-HHMMSS>/sanitized_<ExperimentType>_<YYYYMMDD-HHMMSS>.csv`

If multiple timestamp directories exist, the newest one is chosen by directory
name. If an experiment directory or sanitized CSV is missing, the renderer
creates a header-only placeholder CSV so the figure still renders with the
expected layout and legends.

Optional auxiliary local inputs:

- `bench/plot/data/silo_ccdf_downsampled_*.csv` for the Silo CCDF panel
- `third_party/schedsim/scripts/data/...` or `bench/plot/data/schedsim/...` for
  schedsim comparison curves

If those files are absent, the corresponding curves render as empty series.

## Usage

List the supported figures:

```bash
python3 bench/plot/render.py --list
```

Check which inputs resolve to real data versus placeholders:

```bash
python3 bench/plot/render.py --check
```

Prepare the normalized per-figure build directories without compiling:

```bash
python3 bench/plot/render.py --prepare-only
```

Render every known figure:

```bash
python3 bench/plot/render.py
```

Render a subset:

```bash
python3 bench/plot/render.py synthetic_100us synthetic_grpc silo
```

Rendered PDFs are written to `bench/plot/out/`. Temporary build files live
under `bench/plot/out/build/`.

## LaTeX Prerequisites

Rendering requires one of:

- `latexmk`
- `pdflatex`

If neither is installed, `--check` and `--prepare-only` still work.

On Ubuntu/Debian, a practical install command is:

```bash
sudo apt install latexmk texlive-latex-extra texlive-pictures texlive-science texlive-extra-utils ghostscript
```

This provides the compiler and the TeX packages used by the current templates,
including TikZ/pgfplots, `subcaption`, `siunitx`, `xstring`, `float`, and
`geometry`. `texlive-extra-utils` provides `pdfcrop`, which the renderer uses
to trim standalone PDFs so they do not keep a large blank page around the
figure content.
