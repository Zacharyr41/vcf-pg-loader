#!/bin/bash
# Helper script to create demo recordings using VHS
# Requirements: brew install vhs ffmpeg ttyd

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

show_help() {
    echo "Demo Recording Script"
    echo ""
    echo "Usage: ./demo/record.sh [options]"
    echo ""
    echo "Options:"
    echo "  --full     Record the full demo (default, slow - 33+ minutes)"
    echo "  --fast     Record using fast mode (recommended - ~5 minutes)"
    echo "  --part1    Record Part I (Background) only"
    echo "  --part2    Record Part II (Tool) only"
    echo "  --all      Record all versions (full, part1, part2)"
    echo "  --help     Show this help message"
    echo ""
    echo "Requirements:"
    echo "  brew install vhs ffmpeg ttyd"
    echo ""
    echo "Output:"
    echo "  GIF and MP4 files are saved to demo/recordings/"
    echo ""
    echo "Fast mode records each slide with minimal delay, then uses ffmpeg"
    echo "to extend each segment to the narration duration. Much faster!"
}

check_deps() {
    if ! command -v vhs &> /dev/null; then
        echo "Error: vhs is not installed"
        echo "Install with: brew install vhs"
        exit 1
    fi
}

record_full() {
    echo "Recording full demo (this will take 33+ minutes)..."
    vhs demo/record.tape
    echo "Done! Output: demo/recordings/vcf-pg-loader-demo.{gif,mp4}"
}

record_fast() {
    echo "Recording full demo using fast mode..."
    uv run python demo/record_fast.py "$@"
    echo "Done! Output: demo/recordings/vcf-pg-loader-demo.{gif,mp4}"
}

record_part1() {
    echo "Recording Part I (Background)..."
    vhs demo/record-part1.tape
    echo "Done! Output: demo/recordings/vcf-pg-loader-part1.{gif,mp4}"
}

record_part2() {
    echo "Recording Part II (Tool)..."
    vhs demo/record-part2.tape
    echo "Done! Output: demo/recordings/vcf-pg-loader-part2.{gif,mp4}"
}

record_all() {
    record_full
    record_part1
    record_part2
}

# Main
check_deps

case "${1:-}" in
    --help|-h)
        show_help
        ;;
    --fast)
        record_fast
        ;;
    --part1)
        record_part1
        ;;
    --part2)
        record_part2
        ;;
    --all)
        record_all
        ;;
    --full)
        record_full
        ;;
    "")
        echo "No option specified. Use --fast (recommended) or --full."
        echo ""
        show_help
        exit 1
        ;;
    *)
        echo "Unknown option: $1"
        show_help
        exit 1
        ;;
esac
