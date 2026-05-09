#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


PLOT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PLOT_DIR.parents[1]
STYLE_FILE = PLOT_DIR / "paper-style.tex"
DEFAULT_TEMPLATE_DIR = PLOT_DIR / "templates"
DEFAULT_PLOT_DATA_DIR = PLOT_DIR / "data"
DEFAULT_RESULTS_DIR = REPO_ROOT / "results"
DEFAULT_SCHEDSIM_DATA_DIR = REPO_ROOT / "third_party" / "schedsim" / "scripts" / "data"
DEFAULT_OUTPUT_DIR = PLOT_DIR / "out"

SYNTHETIC_HEADER = (
    "dist_config, lancet_load, n_repetition, load_koma(QPS), "
    "load_partition(QPS), load_floating(QPS), load_kcm_floating(QPS), "
    "load_pool(QPS), latency_koma, latency_partition, latency_floating, "
    "latency_kcm_floating, latency_pool\n"
)
TLS_HEADER = (
    "dist_config, lancet_load, n_repetition, load_floating-tls(QPS), "
    "load_koma-tls(QPS), load_pool-tls(QPS), latency_floating-tls, "
    "latency_koma-tls, latency_pool-tls\n"
)
GRPC_HEADER = (
    "dist_config, lancet_load, n_repetition, load_grpc-go(QPS), "
    "load_grpc-koma-go(QPS), latency_grpc-go, latency_grpc-koma-go\n"
)
SILO_HEADER = (
    "dist_config, lancet_load, n_repetition, load_silo-koma(QPS), "
    "load_silo-partition(QPS), load_silo-floating(QPS), "
    "load_silo-kcm_floating(QPS), load_silo-pool(QPS), latency_silo-koma, "
    "latency_silo-partition, latency_silo-floating, "
    "latency_silo-kcm_floating, latency_silo-pool\n"
)
SILO_TLS_HEADER = (
    "dist_config, lancet_load, n_repetition, load_silo-koma-tls(QPS), "
    "load_silo-floating-tls(QPS), load_silo-pool-tls(QPS), "
    "latency_silo-koma-tls, latency_silo-floating-tls, latency_silo-pool-tls\n"
)
SILO_GRPC_HEADER = (
    "dist_config, lancet_load, n_repetition, load_silo-grpc-go(QPS), "
    "load_silo-grpc-koma-go(QPS), latency_silo-grpc-go, "
    "latency_silo-grpc-koma-go\n"
)
SCHEDSIM_HEADER = "Req/time_unit,99th (us)\n"
SILO_CCDF_HEADER = "latency_us,ccdf\n"


@dataclass(frozen=True)
class InputSpec:
    relative_path: str
    header: str
    experiment_type: str | None = None
    placeholder_content: str | None = None


@dataclass(frozen=True)
class FigureSpec:
    name: str
    template_name: str
    description: str
    inputs: tuple[InputSpec, ...]


@dataclass(frozen=True)
class InputResolution:
    spec: InputSpec
    source: Path | None
    source_kind: str


FIGURES = (
    FigureSpec(
        name="synthetic_100us",
        template_name="synthetic_100us.tex",
        description="Synthetic TCP figure for 100us service time.",
        inputs=(
            InputSpec("synthetic_20_100us.csv", SYNTHETIC_HEADER, "Vanilla100_Conn20"),
            InputSpec("synthetic_80_100us.csv", SYNTHETIC_HEADER, "Vanilla100_Conn80"),
            InputSpec("synthetic_5000_100us.csv", SYNTHETIC_HEADER, "Vanilla100_Conn5000"),
            InputSpec("schedsim/single_queue/FIFO_1000_20_80_d.csv", SCHEDSIM_HEADER),
            InputSpec("schedsim/single_queue/FIFO_1000_20_80_m.csv", SCHEDSIM_HEADER),
            InputSpec("schedsim/single_queue/FIFO_1000_20_80_b.csv", SCHEDSIM_HEADER),
        ),
    ),
    FigureSpec(
        name="synthetic_20us",
        template_name="synthetic_20us.tex",
        description="Synthetic TCP figure for 20us service time.",
        inputs=(
            InputSpec("synthetic_80_20us.csv", SYNTHETIC_HEADER, "Vanilla20_Conn80"),
            InputSpec("schedsim/single_queue/FIFO_1000_20_80_d.csv", SCHEDSIM_HEADER),
            InputSpec("schedsim/single_queue/FIFO_1000_20_80_m.csv", SCHEDSIM_HEADER),
            InputSpec("schedsim/single_queue/FIFO_1000_20_80_b.csv", SCHEDSIM_HEADER),
        ),
    ),
    FigureSpec(
        name="synthetic_grpc",
        template_name="synthetic_grpc.tex",
        description="Synthetic gRPC figure.",
        inputs=(
            InputSpec("synthetic_grpc_24conn.csv", GRPC_HEADER, "GRPC20_Conn24"),
            InputSpec("synthetic_grpc_5000conn.csv", GRPC_HEADER, "GRPC20_Conn5000"),
        ),
    ),
    FigureSpec(
        name="ktls_eval",
        template_name="ktls_eval.tex",
        description="kTLS evaluation figure.",
        inputs=(
            InputSpec("synthetic_80_100us.csv", SYNTHETIC_HEADER, "Vanilla100_Conn80"),
            InputSpec("synthetic_80_20us.csv", SYNTHETIC_HEADER, "Vanilla20_Conn80"),
            InputSpec("ktls_100us.csv", TLS_HEADER, "TLS100_Conn80"),
            InputSpec("ktls_20us.csv", TLS_HEADER, "TLS20_Conn80"),
            InputSpec("schedsim/single_queue/FIFO_1000_20_80_d.csv", SCHEDSIM_HEADER),
            InputSpec("schedsim/single_queue/FIFO_1000_20_80_m.csv", SCHEDSIM_HEADER),
            InputSpec("schedsim/single_queue/FIFO_1000_20_80_b.csv", SCHEDSIM_HEADER),
        ),
    ),
    FigureSpec(
        name="silo",
        template_name="silo.tex",
        description="Combined Silo figure.",
        inputs=(
            InputSpec("silo_bench.csv", SILO_HEADER, "Silo_Conn80"),
            InputSpec("silo_tls.csv", SILO_TLS_HEADER, "Silo_Conn80_TLS"),
            InputSpec("silo_grpc_24conn.csv", SILO_GRPC_HEADER, "Silo_GRPC_Conn24"),
            InputSpec("silo_grpc_5000conn.csv", SILO_GRPC_HEADER, "Silo_GRPC_Conn5000"),
            InputSpec("silo_ccdf_downsampled_Delivery.csv", SILO_CCDF_HEADER, placeholder_content=""),
            InputSpec("silo_ccdf_downsampled_StockLevel.csv", SILO_CCDF_HEADER, placeholder_content=""),
            InputSpec("silo_ccdf_downsampled_all.csv", SILO_CCDF_HEADER, placeholder_content=""),
            InputSpec("silo_ccdf_downsampled_NewOrder.csv", SILO_CCDF_HEADER, placeholder_content=""),
            InputSpec("silo_ccdf_downsampled_Payment.csv", SILO_CCDF_HEADER, placeholder_content=""),
            InputSpec("silo_ccdf_downsampled_OrderStatus.csv", SILO_CCDF_HEADER, placeholder_content=""),
        ),
    ),
)
FIGURES_BY_NAME = {figure.name: figure for figure in FIGURES}

FIGURE_STAR_ENV_RE = re.compile(r"\\begin\{figure\*\}(\[[^\]]*\])?")
DRAFT_NOTE_RE = re.compile(
    r"\s*\(\\add\{results (?:will be updated|in \(c\) will be updated).*?Cloudlab\}\)",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Render paper-style benchmark figures from the latest sanitized CSV "
            "under results/<ExperimentType>/<timestamp>/."
        )
    )
    parser.add_argument(
        "figures",
        nargs="*",
        help="Figure names to render. Default: all known figures.",
    )
    parser.add_argument(
        "--results-dir",
        default=str(DEFAULT_RESULTS_DIR),
        help="Root results directory. Default: %(default)s",
    )
    parser.add_argument(
        "--template-dir",
        default=str(DEFAULT_TEMPLATE_DIR),
        help="Directory containing the local figure templates. Default: %(default)s",
    )
    parser.add_argument(
        "--plot-data-dir",
        default=str(DEFAULT_PLOT_DATA_DIR),
        help=(
            "Directory for optional local auxiliary plot data such as "
            "silo_ccdf_downsampled_*.csv. Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--schedsim-data-dir",
        default=str(DEFAULT_SCHEDSIM_DATA_DIR),
        help="Directory for optional schedsim CSV inputs. Default: %(default)s",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for rendered PDFs and build files. Default: %(default)s",
    )
    parser.add_argument(
        "--compiler",
        choices=("latexmk", "pdflatex"),
        help="Force a specific LaTeX compiler instead of auto-detecting one.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List the supported figures and exit.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate local templates and report which inputs resolve to data or placeholders.",
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Generate per-figure build directories and normalized CSV inputs without compiling.",
    )
    return parser.parse_args()


def detect_compiler(preferred: str | None) -> str | None:
    if preferred:
        return preferred if shutil.which(preferred) else None
    for candidate in ("latexmk", "pdflatex"):
        if shutil.which(candidate):
            return candidate
    return None


def normalize_figure_selection(names: list[str]) -> list[FigureSpec]:
    if not names:
        return list(FIGURES)

    selected: list[FigureSpec] = []
    for name in names:
        if name == "all":
            return list(FIGURES)
        figure = FIGURES_BY_NAME.get(name)
        if figure is None:
            known = ", ".join(spec.name for spec in FIGURES)
            raise SystemExit(f"Unknown figure {name!r}. Known figures: {known}")
        selected.append(figure)
    return selected


def sanitize_template(template_text: str) -> str:
    template_text = FIGURE_STAR_ENV_RE.sub(r"\\begin{figure}\1", template_text)
    template_text = template_text.replace(r"\end{figure*}", r"\end{figure}")
    template_text = DRAFT_NOTE_RE.sub("", template_text)
    return template_text


def latest_sanitized_csv(results_dir: Path, experiment_type: str) -> Path | None:
    experiment_dir = results_dir / experiment_type
    if not experiment_dir.is_dir():
        return None

    newest: tuple[str, Path] | None = None
    for child in experiment_dir.iterdir():
        if not child.is_dir():
            continue
        csvs = sorted(child.glob(f"sanitized_{experiment_type}_*.csv"))
        if not csvs:
            continue
        candidate = (child.name, csvs[-1])
        if newest is None or candidate[0] > newest[0]:
            newest = candidate
    return newest[1] if newest else None


def resolve_input(
    spec: InputSpec,
    results_dir: Path,
    plot_data_dir: Path,
    schedsim_data_dir: Path,
) -> InputResolution:
    if spec.experiment_type is not None:
        source = latest_sanitized_csv(results_dir, spec.experiment_type)
        if source is not None:
            return InputResolution(spec=spec, source=source, source_kind="results")
        return InputResolution(spec=spec, source=None, source_kind="empty")

    plot_data_source = plot_data_dir / spec.relative_path
    if plot_data_source.is_file():
        return InputResolution(spec=spec, source=plot_data_source, source_kind="plot-data")

    if spec.relative_path.startswith("schedsim/"):
        schedsim_rel = Path(spec.relative_path).relative_to("schedsim")
        schedsim_source = schedsim_data_dir / schedsim_rel
        if schedsim_source.is_file():
            return InputResolution(spec=spec, source=schedsim_source, source_kind="schedsim")

    return InputResolution(spec=spec, source=None, source_kind="empty")


def materialize_input(resolution: InputResolution, data_dir: Path) -> Path:
    target = data_dir / resolution.spec.relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    if resolution.source is not None:
        shutil.copyfile(resolution.source, target)
    else:
        target.write_text(
            resolution.spec.placeholder_content
            if resolution.spec.placeholder_content is not None
            else resolution.spec.header,
            encoding="utf-8",
        )
    return target


def prepare_build_dir(
    figure: FigureSpec,
    template_dir: Path,
    output_dir: Path,
    results_dir: Path,
    plot_data_dir: Path,
    schedsim_data_dir: Path,
) -> tuple[Path, list[InputResolution]]:
    template_path = template_dir / figure.template_name
    if not template_path.is_file():
        raise FileNotFoundError(f"Missing template: {template_path}")
    if not STYLE_FILE.is_file():
        raise FileNotFoundError(f"Missing style file: {STYLE_FILE}")

    build_dir = output_dir / "build" / figure.name
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)

    template_text = sanitize_template(template_path.read_text(encoding="utf-8"))
    (build_dir / "template.tex").write_text(template_text, encoding="utf-8")
    shutil.copyfile(STYLE_FILE, build_dir / "paper-style.tex")

    data_dir = build_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    resolutions = [
        resolve_input(spec, results_dir, plot_data_dir, schedsim_data_dir)
        for spec in figure.inputs
    ]
    for resolution in resolutions:
        materialize_input(resolution, data_dir)

    wrapper = r"""\documentclass{article}
\usepackage[paperwidth=7.5in,paperheight=12in,margin=0.25in]{geometry}
\input{paper-style.tex}
\begin{document}
\pagestyle{empty}
\input{template.tex}
\end{document}
"""
    (build_dir / "render.tex").write_text(wrapper, encoding="utf-8")
    return build_dir, resolutions


def compile_figure(build_dir: Path, compiler: str) -> None:
    if compiler == "latexmk":
        commands = [
            [
                "latexmk",
                "-pdf",
                "-interaction=nonstopmode",
                "-halt-on-error",
                "render.tex",
            ]
        ]
    elif compiler == "pdflatex":
        commands = [
            ["pdflatex", "-interaction=nonstopmode", "-halt-on-error", "render.tex"],
            ["pdflatex", "-interaction=nonstopmode", "-halt-on-error", "render.tex"],
        ]
    else:
        raise ValueError(f"Unsupported compiler: {compiler}")

    log_path = build_dir / "render.stdout.log"
    for command in commands:
        result = subprocess.run(
            command,
            cwd=build_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        log_path.write_text(result.stdout, encoding="utf-8")
        if result.returncode != 0:
            raise RuntimeError(
                f"LaTeX compilation failed for {build_dir.name}. See {log_path}."
            )

    pdfcrop = shutil.which("pdfcrop")
    rendered_pdf = build_dir / "render.pdf"
    cropped_pdf = build_dir / "render-crop.pdf"
    if pdfcrop and rendered_pdf.is_file():
        result = subprocess.run(
            [pdfcrop, "--margins", "2", str(rendered_pdf), str(cropped_pdf)],
            cwd=build_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        log_path.write_text(result.stdout, encoding="utf-8")
        if result.returncode != 0:
            raise RuntimeError(
                f"PDF crop failed for {build_dir.name}. See {log_path}."
            )
        shutil.move(cropped_pdf, rendered_pdf)


def summarize_resolutions(resolutions: list[InputResolution]) -> str:
    resolved = sum(resolution.source is not None for resolution in resolutions)
    empty = len(resolutions) - resolved
    return f"{resolved} resolved, {empty} placeholder"


def render_figures(
    figures: list[FigureSpec],
    template_dir: Path,
    results_dir: Path,
    plot_data_dir: Path,
    schedsim_data_dir: Path,
    output_dir: Path,
    compiler: str,
) -> None:
    for figure in figures:
        build_dir, resolutions = prepare_build_dir(
            figure,
            template_dir,
            output_dir,
            results_dir,
            plot_data_dir,
            schedsim_data_dir,
        )
        compile_figure(build_dir, compiler)

        rendered_pdf = build_dir / "render.pdf"
        if not rendered_pdf.is_file():
            raise RuntimeError(f"Expected PDF was not produced: {rendered_pdf}")

        target_pdf = output_dir / f"{figure.name}.pdf"
        shutil.copyfile(rendered_pdf, target_pdf)
        print(f"[ok] {figure.name}: {target_pdf} ({summarize_resolutions(resolutions)})")


def prepare_figures(
    figures: list[FigureSpec],
    template_dir: Path,
    results_dir: Path,
    plot_data_dir: Path,
    schedsim_data_dir: Path,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for figure in figures:
        build_dir, resolutions = prepare_build_dir(
            figure,
            template_dir,
            output_dir,
            results_dir,
            plot_data_dir,
            schedsim_data_dir,
        )
        print(f"[prepared] {figure.name}: {build_dir} ({summarize_resolutions(resolutions)})")


def check_figures(
    figures: list[FigureSpec],
    template_dir: Path,
    results_dir: Path,
    plot_data_dir: Path,
    schedsim_data_dir: Path,
    compiler: str | None,
) -> int:
    print(f"template dir: {template_dir}")
    print(f"results dir: {results_dir}")
    print(f"plot data dir: {plot_data_dir}")
    print(f"schedsim data dir: {schedsim_data_dir}")
    print(f"compiler: {compiler or 'missing'}")

    if not template_dir.is_dir():
        print(f"[missing] template directory: {template_dir}")
        return 1
    if not STYLE_FILE.is_file():
        print(f"[missing] style file: {STYLE_FILE}")
        return 1

    status = 0
    for figure in figures:
        template_path = template_dir / figure.template_name
        if not template_path.is_file():
            print(f"[missing] {figure.name}: template {template_path}")
            status = 1
            continue

        resolutions = [
            resolve_input(spec, results_dir, plot_data_dir, schedsim_data_dir)
            for spec in figure.inputs
        ]
        print(f"[ok] {figure.name}: {summarize_resolutions(resolutions)}")
        for resolution in resolutions:
            if resolution.source is None:
                print(f"  - {resolution.spec.relative_path}: placeholder")
            else:
                print(
                    f"  - {resolution.spec.relative_path}: "
                    f"{resolution.source_kind} -> {resolution.source}"
                )
    return status


def list_figures() -> None:
    for figure in FIGURES:
        print(f"{figure.name:16} {figure.description}")


def main() -> int:
    args = parse_args()
    if args.list:
        list_figures()
        return 0

    figures = normalize_figure_selection(args.figures)
    template_dir = Path(args.template_dir).expanduser().resolve()
    results_dir = Path(args.results_dir).expanduser().resolve()
    plot_data_dir = Path(args.plot_data_dir).expanduser().resolve()
    schedsim_data_dir = Path(args.schedsim_data_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    compiler = detect_compiler(args.compiler)

    if args.check:
        return check_figures(
            figures,
            template_dir,
            results_dir,
            plot_data_dir,
            schedsim_data_dir,
            compiler,
        )

    if args.prepare_only:
        prepare_figures(
            figures,
            template_dir,
            results_dir,
            plot_data_dir,
            schedsim_data_dir,
            output_dir,
        )
        return 0

    if compiler is None:
        print(
            "No LaTeX compiler found. Install either `latexmk` or `pdflatex`, "
            "or rerun with `--prepare-only` or `--check`.",
            file=sys.stderr,
        )
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    render_figures(
        figures,
        template_dir,
        results_dir,
        plot_data_dir,
        schedsim_data_dir,
        output_dir,
        compiler,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
