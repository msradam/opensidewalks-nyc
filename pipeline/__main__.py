"""Entry point: python -m pipeline <command> [options]

Commands:
  build            Run all six stages (or from a given stage onward).
  validate         Run only the OSW validator on existing staged output.
  clean            Wipe data/ and output/ for a fresh build.

Examples:
  python -m pipeline build
  python -m pipeline build --stage 3
  python -m pipeline validate
  python -m pipeline clean
"""

import sys
import shutil
from pathlib import Path

import click
import yaml

# Stage imports. Each stage is independently importable.
from pipeline.stages import acquire, clean, schema_map, assemble, validate, export


STAGES = [
    (1, "acquire",    acquire.run),
    (2, "clean",      clean.run),
    (3, "schema_map", schema_map.run),
    (4, "assemble",   assemble.run),
    (5, "validate",   validate.run),
    (6, "export",     export.run),
]

REPO_ROOT = Path(__file__).parent.parent


def _load_config():
    sources_path = REPO_ROOT / "config" / "sources.yaml"
    build_path   = REPO_ROOT / "config" / "build.yaml"
    with open(sources_path) as f:
        sources = yaml.safe_load(f)
    with open(build_path) as f:
        build = yaml.safe_load(f)
    return sources, build


@click.group()
def main():
    """opensidewalks-nyc: OpenSidewalks-conformant NYC pedestrian graph pipeline."""
    pass


@main.command()
@click.option(
    "--stage", "-s",
    type=click.IntRange(1, 6),
    default=1,
    show_default=True,
    help="Start from this stage number (assumes earlier stage artifacts exist). "
         "For the address geocoding index, use pipeline/sources/fetch_open_data.py "
         "and pipeline/sources/build_address_index.py instead.",
)
def build(stage: int):
    """Run the full pipeline (or from a given stage onward)."""
    sources, build_cfg = _load_config()

    click.echo(f"\n{'='*60}")
    click.echo("  opensidewalks-nyc pipeline .  OpenSidewalks v0.3 NYC")
    click.echo(f"{'='*60}")
    if stage > 1:
        click.echo(f"  Resuming from stage {stage}")
    click.echo()

    for stage_num, stage_name, stage_fn in STAGES:
        if stage_num < stage:
            click.echo(f"  [skip] Stage {stage_num}: {stage_name}")
            continue

        click.echo(f"\n{'─'*60}")
        click.echo(f"  Stage {stage_num}: {stage_name}")
        click.echo(f"{'─'*60}")

        try:
            stage_fn(sources=sources, build_cfg=build_cfg, repo_root=REPO_ROOT)
        except Exception as exc:
            click.echo(f"\n[ERROR] Stage {stage_num} ({stage_name}) failed:", err=True)
            click.echo(f"  {exc}", err=True)
            click.echo("\nPipeline aborted. Fix the error and re-run with "
                       f"--stage {stage_num} to resume.", err=True)
            sys.exit(1)

        click.echo(f"\n  [done] Stage {stage_num}: {stage_name}")

    click.echo(f"\n{'='*60}")
    click.echo("  Pipeline complete.")
    click.echo(f"{'='*60}")
    click.echo()
    click.echo("  Outputs:")
    output_dir = REPO_ROOT / "output"
    for f in sorted(output_dir.glob("*")):
        size = f.stat().st_size
        click.echo(f"    {f.name:40s}  {size / 1_048_576:.1f} MB")
    click.echo()


@main.command()
def validate():
    """Run the OSW validator on existing staged output only."""
    sources, build_cfg = _load_config()
    click.echo("\nRunning OSW validator on existing staged output...")
    validate_module = __import__("pipeline.stages.validate", fromlist=["run"])
    validate_module.run(sources=sources, build_cfg=build_cfg, repo_root=REPO_ROOT)
    click.echo("Done. See output/validation_report.md")


@main.command("clean")
def clean_cmd():
    """Wipe data/ and output/ for a fresh build."""
    data_dir   = REPO_ROOT / "data"
    output_dir = REPO_ROOT / "output"

    click.confirm(
        f"This will delete all contents of {data_dir} and {output_dir}. Continue?",
        abort=True,
    )

    for directory in [data_dir, output_dir]:
        if directory.exists():
            shutil.rmtree(directory)
            click.echo(f"  Removed {directory}")

    # Recreate empty directory structure.
    for sub in ["data/raw", "data/clean", "data/staged", "output"]:
        (REPO_ROOT / sub).mkdir(parents=True, exist_ok=True)

    click.echo("Clean complete.")


if __name__ == "__main__":
    main()
