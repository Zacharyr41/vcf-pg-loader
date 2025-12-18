from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from rich.console import Console, RenderableType
from rich.panel import Panel
from rich.text import Text


@dataclass
class Slide:
    title: str
    content: Callable[[], RenderableType] | RenderableType
    section: int
    part: int

    def render(self) -> RenderableType:
        if callable(self.content):
            return self.content()
        return self.content


@dataclass
class Section:
    number: int
    title: str
    part: int
    slides: list[Slide] = field(default_factory=list)

    def add_slide(self, title: str, content: Callable[[], RenderableType] | RenderableType) -> None:
        self.slides.append(Slide(title=title, content=content, section=self.number, part=self.part))


@dataclass
class Part:
    number: int
    title: str
    sections: list[Section] = field(default_factory=list)


class Presenter:
    def __init__(self, title: str = "vcf-pg-loader Demo") -> None:
        self.console = Console()
        self.title = title
        self.parts: list[Part] = []
        self._current_part: Part | None = None
        self._current_section: Section | None = None

    def part(self, number: int, title: str) -> Part:
        p = Part(number=number, title=title)
        self.parts.append(p)
        self._current_part = p
        return p

    def section(self, number: int, title: str) -> Section:
        if self._current_part is None:
            raise ValueError("Must create a part before creating sections")
        s = Section(number=number, title=title, part=self._current_part.number)
        self._current_part.sections.append(s)
        self._current_section = s
        return s

    def slide(self, title: str, content: Callable[[], RenderableType] | RenderableType) -> Slide:
        if self._current_section is None:
            raise ValueError("Must create a section before creating slides")
        sl = Slide(
            title=title,
            content=content,
            section=self._current_section.number,
            part=self._current_part.number if self._current_part else 0,
        )
        self._current_section.slides.append(sl)
        return sl

    def _render_title_bar(self, current: int, total: int) -> None:
        progress = f"[{current}/{total}]"
        title_text = Text()
        title_text.append("  " + self.title + "  ", style="bold white on blue")
        title_text.append("  " + progress + "  ", style="dim")
        self.console.print(title_text)
        self.console.print()

    def _render_section_header(self, section: Section) -> None:
        header = Text()
        header.append(f"Part {section.part} ", style="bold cyan")
        header.append("│ ", style="dim")
        header.append(f"Section {section.number}: ", style="bold yellow")
        header.append(section.title, style="bold white")
        self.console.print(Panel(header, border_style="cyan", padding=(0, 1)))
        self.console.print()

    def _wait_for_enter(self) -> bool:
        try:
            self.console.print(
                "\n[dim]Press [bold]ENTER[/bold] to continue, [bold]q[/bold] to quit, "
                "[bold]s[/bold] to skip section...[/dim]"
            )
            response = input().strip().lower()
            if response == "q":
                return False
            if response == "s":
                return None
            return True
        except (KeyboardInterrupt, EOFError):
            return False

    def _clear_screen(self) -> None:
        self.console.clear()

    def list_sections(self) -> None:
        self.console.print(Panel(f"[bold]{self.title}[/bold]", border_style="blue"))
        self.console.print()

        for part in self.parts:
            self.console.print(f"[bold cyan]Part {part.number}: {part.title}[/bold cyan]")
            for section in part.sections:
                slide_count = len(section.slides)
                self.console.print(
                    f"  [yellow]Section {section.number}:[/yellow] {section.title} "
                    f"[dim]({slide_count} slides)[/dim]"
                )
            self.console.print()

    def present(
        self,
        sections: list[int] | None = None,
        parts: list[int] | None = None,
    ) -> None:
        all_slides: list[tuple[Section, Slide]] = []

        for part in self.parts:
            if parts and part.number not in parts:
                continue
            for section in part.sections:
                if sections and section.number not in sections:
                    continue
                for slide in section.slides:
                    all_slides.append((section, slide))

        if not all_slides:
            self.console.print("[red]No slides to present with the given filters.[/red]")
            return

        total = len(all_slides)
        current_section_num = -1
        i = 0

        while i < total:
            section, slide = all_slides[i]

            self._clear_screen()
            self._render_title_bar(i + 1, total)

            if section.number != current_section_num:
                self._render_section_header(section)
                current_section_num = section.number

            if slide.title:
                self.console.print(f"[bold white]{slide.title}[/bold white]")
                self.console.print()

            rendered = slide.render()
            self.console.print(rendered)

            result = self._wait_for_enter()
            if result is False:
                self.console.print("\n[yellow]Demo ended.[/yellow]")
                break
            elif result is None:
                while i < total and all_slides[i][0].number == current_section_num:
                    i += 1
            else:
                i += 1

        if i >= total:
            self._clear_screen()
            self.console.print(
                Panel(
                    "[bold green]Demo Complete![/bold green]\n\n"
                    "[dim]Thank you for watching the vcf-pg-loader demo.[/dim]",
                    border_style="green",
                    padding=(1, 2),
                )
            )
