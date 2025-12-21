#!/usr/bin/env python3
"""
Fast demo recording script.

Instead of recording 33+ minutes in real-time with VHS, this:
1. Records each slide as a short clip (~2 seconds each)
2. Uses ffmpeg to extend each clip to the narration duration (freeze last frame)
3. Concatenates all clips into the final video

Total time: ~3-5 minutes instead of 33+ minutes.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Slide:
    name: str
    duration: float
    commands: list[str] | None = None
    key: str | None = None


TAPE_HEADER = """# Auto-generated VHS tape
Output "{output_path}"

Set Shell "bash"
Set FontSize 16
Set Width 1400
Set Height 900
Set Padding 20
Set Framerate 30
Set Theme "Dracula"
Set TypingSpeed 50ms
Set WindowBar Colorful
Set WindowBarSize 40
Set BorderRadius 8

Sleep 500ms
"""


def parse_record_tape(tape_path: Path) -> list[Slide]:
    """Parse the main record.tape to extract slide timings."""
    content = tape_path.read_text()
    slides: list[Slide] = []

    lines = content.split("\n")
    i = 0

    intro_slides = [
        Slide(name="s000_help", duration=3, commands=["python -m demo --help"]),
        Slide(name="s001_list", duration=4, commands=["python -m demo --list"]),
        Slide(name="s002_start", duration=2, commands=["python -m demo"]),
    ]
    slides.extend(intro_slides)

    slide_num = 1
    while i < len(lines):
        line = lines[i].strip()

        if line.startswith("# Slide"):
            match = re.search(r"Slide (\d+):", line)
            if match:
                slide_id = int(match.group(1))

                while i < len(lines) - 1:
                    i += 1
                    next_line = lines[i].strip()
                    if next_line == "Enter":
                        while i < len(lines) - 1:
                            i += 1
                            sleep_line = lines[i].strip()
                            if sleep_line.startswith("Sleep"):
                                duration_match = re.search(r"Sleep (\d+)s", sleep_line)
                                if duration_match:
                                    duration = int(duration_match.group(1))
                                    slides.append(
                                        Slide(
                                            name=f"s{slide_id:03d}", duration=duration, key="Enter"
                                        )
                                    )
                                    slide_num += 1
                                break
                        break
        i += 1

    return slides


def generate_tape_for_slide(
    slide: Slide, output_path: str, is_first: bool = False, demo_running: bool = True
) -> str:
    """Generate a VHS tape file for a single slide."""
    tape = TAPE_HEADER.format(output_path=output_path)

    if slide.commands:
        for cmd in slide.commands:
            tape += f'Type "{cmd}"\n'
            tape += "Sleep 300ms\n"
            tape += "Enter\n"
            tape += "Sleep 2s\n"
    elif slide.key:
        tape += f"{slide.key}\n"
        tape += "Sleep 2s\n"

    return tape


def generate_combined_tape(
    slides: list[Slide], output_dir: Path, record_duration: float = 2.0
) -> str:
    """Generate a single tape that records all slides with minimal delays."""
    tape = TAPE_HEADER.format(output_path=str(output_dir / "raw_recording.mp4"))

    tape += "# Mark: START\n"
    tape += "Sleep 1s\n"

    for slide in slides:
        tape += f"\n# --- {slide.name} (target: {slide.duration}s) ---\n"

        if slide.commands:
            for cmd in slide.commands:
                tape += f'Type "{cmd}"\n'
                tape += "Sleep 300ms\n"
                tape += "Enter\n"
        elif slide.key:
            tape += f"{slide.key}\n"

        tape += f"Sleep {record_duration}s\n"

    tape += "\nSleep 2s\n"

    return tape


def generate_section_tapes(slides: list[Slide], output_dir: Path) -> list[tuple[Path, Slide]]:
    """Generate individual tape files for each slide."""
    tape_files: list[tuple[Path, Slide]] = []

    for i, slide in enumerate(slides):
        tape_path = output_dir / f"{slide.name}.tape"
        mp4_path = output_dir / f"{slide.name}.mp4"

        tape_content = TAPE_HEADER.format(output_path=str(mp4_path))

        if i == 0:
            tape_content += "Sleep 1s\n"

        if slide.commands:
            for cmd in slide.commands:
                tape_content += f'Type "{cmd}"\n'
                tape_content += "Sleep 300ms\n"
                tape_content += "Enter\n"
                tape_content += "Sleep 2s\n"
        elif slide.key:
            tape_content += f"{slide.key}\n"
            tape_content += "Sleep 2s\n"

        tape_path.write_text(tape_content)
        tape_files.append((tape_path, slide))

    return tape_files


def record_continuous_session(
    slides: list[Slide], output_dir: Path, record_time: float = 2.0
) -> Path:
    """Record all slides in one continuous session, then split with ffmpeg."""
    tape_content = TAPE_HEADER.format(output_path=str(output_dir / "continuous.mp4"))

    tape_content += "Sleep 1s\n"

    for cmd in ["python -m demo --help", "python -m demo --list", "python -m demo"]:
        tape_content += f'Type "{cmd}"\n'
        tape_content += "Sleep 300ms\n"
        tape_content += "Enter\n"
        tape_content += f"Sleep {record_time}s\n"

    slide_count = len([s for s in slides if s.key])
    for _ in range(slide_count):
        tape_content += "Enter\n"
        tape_content += f"Sleep {record_time}s\n"

    tape_content += "Sleep 2s\n"

    tape_path = output_dir / "continuous.tape"
    tape_path.write_text(tape_content)

    print(f"Recording continuous session ({slide_count + 3} slides)...")
    subprocess.run(["vhs", str(tape_path)], check=True)

    return output_dir / "continuous.mp4"


def split_and_extend_video(
    input_video: Path,
    slides: list[Slide],
    output_dir: Path,
    segment_duration: float = 2.0,
) -> list[Path]:
    """Split continuous recording and extend each segment to target duration."""
    extended_clips: list[Path] = []

    current_time = 1.0

    for i, slide in enumerate(slides):
        segment_path = output_dir / f"segment_{i:03d}.mp4"
        extended_path = output_dir / f"extended_{i:03d}.mp4"

        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(input_video),
                "-ss",
                str(current_time),
                "-t",
                str(segment_duration),
                "-c",
                "copy",
                str(segment_path),
            ],
            check=True,
            capture_output=True,
        )

        if slide.duration > segment_duration:
            subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    str(segment_path),
                    "-vf",
                    f"tpad=stop_mode=clone:stop_duration={slide.duration - segment_duration}",
                    "-c:v",
                    "libx264",
                    "-preset",
                    "fast",
                    str(extended_path),
                ],
                check=True,
                capture_output=True,
            )
            extended_clips.append(extended_path)
        else:
            extended_clips.append(segment_path)

        current_time += segment_duration

    return extended_clips


def extend_video_duration(input_path: Path, target_duration: float, output_path: Path) -> None:
    """Extend a video to target duration by freezing the last frame."""
    probe_result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(input_path),
        ],
        capture_output=True,
        text=True,
    )
    current_duration = float(probe_result.stdout.strip())

    if target_duration <= current_duration:
        subprocess.run(["cp", str(input_path), str(output_path)], check=True)
        return

    pad_duration = target_duration - current_duration

    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(input_path),
            "-vf",
            f"tpad=stop_mode=clone:stop_duration={pad_duration}",
            "-c:v",
            "libx264",
            "-preset",
            "fast",
            "-crf",
            "18",
            "-c:a",
            "copy",
            str(output_path),
        ],
        check=True,
        capture_output=True,
    )


def concatenate_videos(video_paths: list[Path], output_path: Path) -> None:
    """Concatenate multiple videos into one."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        for video in video_paths:
            f.write(f"file '{video}'\n")
        concat_list = Path(f.name)

    try:
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_list),
                "-c",
                "copy",
                str(output_path),
            ],
            check=True,
        )
    finally:
        concat_list.unlink()


def create_freeze_frame_video(
    image_path: Path, duration: float, output_path: Path, fps: int = 30
) -> None:
    """Create a video from a single image with specified duration."""
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-loop",
            "1",
            "-i",
            str(image_path),
            "-c:v",
            "libx264",
            "-t",
            str(duration),
            "-pix_fmt",
            "yuv420p",
            "-r",
            str(fps),
            str(output_path),
        ],
        check=True,
        capture_output=True,
    )


def record_slide_by_slide(slides: list[Slide], work_dir: Path) -> list[Path]:
    """Record each slide separately and return paths to extended videos."""
    extended_videos: list[Path] = []

    tape_path = work_dir / "current.tape"
    raw_path = work_dir / "current_raw.mp4"

    for i, slide in enumerate(slides):
        print(f"  Recording slide {i+1}/{len(slides)}: {slide.name} (target: {slide.duration}s)")

        tape_content = TAPE_HEADER.format(output_path=str(raw_path))

        if i == 0:
            tape_content += "Sleep 1s\n"
            for cmd in ["python -m demo --help"]:
                tape_content += f'Type "{cmd}"\n'
                tape_content += "Sleep 300ms\n"
                tape_content += "Enter\n"
            tape_content += "Sleep 2s\n"
        elif i == 1:
            tape_content += 'Type "python -m demo --list"\n'
            tape_content += "Sleep 300ms\n"
            tape_content += "Enter\n"
            tape_content += "Sleep 2s\n"
        elif i == 2:
            tape_content += 'Type "python -m demo"\n'
            tape_content += "Sleep 300ms\n"
            tape_content += "Enter\n"
            tape_content += "Sleep 2s\n"
        else:
            pass

        tape_path.write_text(tape_content)

    return extended_videos


def record_with_timing_marks(slides: list[Slide], work_dir: Path) -> Path:
    """
    Record entire demo with short pauses, outputting timing marks.
    Returns path to the raw recording.
    """
    tape_path = work_dir / "full_recording.tape"
    output_path = work_dir / "full_raw.mp4"

    short_pause = 1.5

    tape_content = TAPE_HEADER.format(output_path=str(output_path))
    tape_content += "\nSleep 1s\n"

    tape_content += '\nType "python -m demo --help"\n'
    tape_content += "Sleep 300ms\n"
    tape_content += "Enter\n"
    tape_content += f"Sleep {short_pause}s\n"

    tape_content += '\nType "python -m demo --list"\n'
    tape_content += "Sleep 300ms\n"
    tape_content += "Enter\n"
    tape_content += f"Sleep {short_pause}s\n"

    tape_content += '\nType "python -m demo"\n'
    tape_content += "Sleep 300ms\n"
    tape_content += "Enter\n"
    tape_content += f"Sleep {short_pause}s\n"

    key_slides = [s for s in slides if s.key == "Enter"]
    for slide in key_slides:
        tape_content += f"\n# {slide.name}\n"
        tape_content += "Enter\n"
        tape_content += f"Sleep {short_pause}s\n"

    tape_content += "\nSleep 2s\n"

    tape_path.write_text(tape_content)

    total_slides = len(key_slides) + 3
    estimated_time = total_slides * short_pause + 5
    print(f"Recording {total_slides} slides (estimated {estimated_time:.0f}s of raw recording)...")

    subprocess.run(["vhs", str(tape_path)], check=True)

    return output_path


def process_recording(
    raw_video: Path,
    slides: list[Slide],
    work_dir: Path,
    output_path: Path,
    segment_duration: float = 1.5,
) -> None:
    """Process raw recording: split, extend each segment, concatenate."""
    print("Processing recording...")

    all_slides = [
        Slide(name="intro_help", duration=3),
        Slide(name="intro_list", duration=4),
        Slide(name="intro_start", duration=2),
    ] + [s for s in slides if s.key == "Enter"]

    extended_clips: list[Path] = []
    current_time = 1.0

    for i, slide in enumerate(all_slides):
        print(f"  Processing {i+1}/{len(all_slides)}: {slide.name} -> {slide.duration}s")

        segment_path = work_dir / f"seg_{i:03d}.mp4"
        extended_path = work_dir / f"ext_{i:03d}.mp4"

        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                str(current_time),
                "-i",
                str(raw_video),
                "-t",
                str(segment_duration),
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-crf",
                "18",
                str(segment_path),
            ],
            check=True,
            capture_output=True,
        )

        extend_video_duration(segment_path, slide.duration, extended_path)
        extended_clips.append(extended_path)

        current_time += segment_duration

    print(f"Concatenating {len(extended_clips)} clips...")
    concatenate_videos(extended_clips, output_path)

    total_duration = sum(s.duration for s in all_slides)
    print(f"Final video duration: ~{total_duration}s ({total_duration/60:.1f} minutes)")


def record_fast(
    tape_path: Path,
    output_mp4: Path,
    output_gif: Path | None = None,
    keep_work_dir: bool = False,
    dry_run: bool = False,
) -> None:
    """Main entry point for fast recording."""
    print(f"Parsing {tape_path}...")
    slides = parse_record_tape(tape_path)
    print(f"Found {len(slides)} slides")

    total_duration = sum(s.duration for s in slides)
    print(f"Target duration: {total_duration}s ({total_duration/60:.1f} minutes)")

    if dry_run:
        print("\n--- DRY RUN: Slide breakdown ---")
        for i, slide in enumerate(slides):
            print(f"  {i+1:3d}. {slide.name:20s} -> {slide.duration:3.0f}s")
        key_slides = len([s for s in slides if s.key == "Enter"])
        raw_time = (key_slides + 3) * 1.5 + 5
        print(f"\nRaw recording time: ~{raw_time:.0f}s ({raw_time/60:.1f} minutes)")
        print(f"FFmpeg processing: ~{len(slides) * 2}s")
        print(f"Total estimated time: ~{(raw_time + len(slides) * 2)/60:.1f} minutes")
        print(f"\nCompare to real-time recording: {total_duration/60:.1f} minutes")
        return

    work_dir = Path(tempfile.mkdtemp(prefix="vhs_fast_"))
    print(f"Work directory: {work_dir}")

    try:
        raw_video = record_with_timing_marks(slides, work_dir)

        process_recording(raw_video, slides, work_dir, output_mp4)

        if output_gif:
            print("Creating GIF (this may take a while)...")
            subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    str(output_mp4),
                    "-vf",
                    "fps=10,scale=700:-1:flags=lanczos",
                    "-c:v",
                    "gif",
                    str(output_gif),
                ],
                check=True,
            )

        print("\nDone!")
        print(f"  MP4: {output_mp4}")
        if output_gif:
            print(f"  GIF: {output_gif}")

    finally:
        if not keep_work_dir:
            import shutil

            shutil.rmtree(work_dir)
            print("Cleaned up work directory")
        else:
            print(f"Work directory preserved: {work_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Fast demo recording using VHS + ffmpeg",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Record full demo (fast mode)
    python demo/record_fast.py

    # Record and keep work directory for debugging
    python demo/record_fast.py --keep-work-dir

    # Skip GIF generation (faster)
    python demo/record_fast.py --no-gif
        """,
    )

    parser.add_argument(
        "--tape",
        type=Path,
        default=Path("demo/record.tape"),
        help="Path to the VHS tape file to parse (default: demo/record.tape)",
    )
    parser.add_argument(
        "--output-mp4",
        type=Path,
        default=Path("demo/recordings/vcf-pg-loader-demo.mp4"),
        help="Output MP4 path (default: demo/recordings/vcf-pg-loader-demo.mp4)",
    )
    parser.add_argument(
        "--output-gif",
        type=Path,
        default=Path("demo/recordings/vcf-pg-loader-demo.gif"),
        help="Output GIF path (default: demo/recordings/vcf-pg-loader-demo.gif)",
    )
    parser.add_argument("--no-gif", action="store_true", help="Skip GIF generation")
    parser.add_argument(
        "--keep-work-dir",
        action="store_true",
        help="Keep the temporary work directory for debugging",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Parse tape and show plan without recording"
    )

    args = parser.parse_args()

    output_gif = None if args.no_gif else args.output_gif

    record_fast(
        tape_path=args.tape,
        output_mp4=args.output_mp4,
        output_gif=output_gif,
        keep_work_dir=args.keep_work_dir,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
