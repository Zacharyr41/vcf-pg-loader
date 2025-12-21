#!/usr/bin/env python3
from __future__ import annotations

from typing import Annotated

import typer

from .content.part1_background import build_part1
from .content.part2_tool import build_part2
from .presenter import Presenter

app = typer.Typer(
    name="vcf-pg-loader-demo",
    help="Interactive terminal demo of vcf-pg-loader",
    add_completion=False,
    invoke_without_command=True,
)


def parse_comma_separated(value: str | None) -> list[int] | None:
    if value is None:
        return None
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def _build_presenter() -> Presenter:
    presenter = Presenter(title="vcf-pg-loader Demo")
    build_part1(presenter)
    build_part2(presenter)
    return presenter


@app.callback()
def main(
    ctx: typer.Context,
    sections: Annotated[
        str | None,
        typer.Option(
            "--sections",
            "-s",
            help="Comma-separated section numbers to show (e.g., '1,3,5')",
        ),
    ] = None,
    parts: Annotated[
        str | None,
        typer.Option(
            "--parts",
            "-p",
            help="Comma-separated part numbers to show (e.g., '1' or '2')",
        ),
    ] = None,
    list_sections: Annotated[
        bool,
        typer.Option(
            "--list",
            "-l",
            help="List all available sections and exit",
        ),
    ] = False,
) -> None:
    """
    Interactive terminal demo of vcf-pg-loader.

    Run without arguments to start the full demo.

    Examples:

        python -m demo                    # Full demo

        python -m demo --list             # List all sections

        python -m demo --parts 1          # Run only Part I (Background)

        python -m demo --sections 4,5,6   # Run specific sections

        python -m demo -p 2 -s 7,8        # Part II, sections 7-8 only
    """
    if ctx.invoked_subcommand is not None:
        return

    presenter = _build_presenter()

    if list_sections:
        presenter.list_sections()
        raise typer.Exit()

    section_list = parse_comma_separated(sections)
    part_list = parse_comma_separated(parts)

    presenter.present(sections=section_list, parts=part_list)


@app.command()
def sections() -> None:
    """List all available sections in the demo."""
    presenter = _build_presenter()
    presenter.list_sections()


if __name__ == "__main__":
    app()
