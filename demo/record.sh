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
    echo "  --full     Record the full demo (default)"
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
}

check_deps() {
    if ! command -v vhs &> /dev/null; then
        echo "Error: vhs is not installed"
        echo "Install with: brew install vhs"
        exit 1
    fi
}

record_full() {
    echo "Recording full demo..."
    vhs demo/record.tape
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
    --part1)
        record_part1
        ;;
    --part2)
        record_part2
        ;;
    --all)
        record_all
        ;;
    --full|"")
        record_full
        ;;
    *)
        echo "Unknown option: $1"
        show_help
        exit 1
        ;;
esac
