# vcf-pg-loader Interactive Demo

An interactive terminal presentation showcasing vcf-pg-loader's capabilities, designed for ~30 minute walkthroughs.

## Quick Start

```bash
# Run the full demo
python -m demo

# List available sections
python -m demo --list
```

## Structure

The demo is organized into 2 parts with 8 sections:

**Part I: Background**
- Section 1: VCF Files: From DNA to Data
- Section 2: Anatomy of a VCF File
- Section 3: VCF in Rare Disease Research

**Part II: The Tool**
- Section 4: Previous Tools (GEMINI & slivar)
- Section 5: vcf-pg-loader Architecture
- Section 6: Research Pipeline Walkthrough
- Section 7: Performance & Compliance
- Section 8: Future: Vector Embeddings

## Navigation

| Key | Action |
|-----|--------|
| `Enter` | Next slide |
| `s` | Skip to next section |
| `q` | Quit demo |

## Selective Presentation

Run specific parts or sections for targeted audiences:

```bash
# Part I only (for those new to VCF/genomics)
python -m demo --parts 1

# Part II only (for those familiar with VCF)
python -m demo --parts 2

# Specific sections
python -m demo --sections 4,5,6

# Combine filters
python -m demo -p 2 -s 7,8
```

## Recording with VHS

Create GIF/MP4 recordings for sharing using [VHS](https://github.com/charmbracelet/vhs):

### Install Dependencies

```bash
brew install vhs ffmpeg ttyd
```

### Create Recordings

```bash
# Full demo
./demo/record.sh --full

# Part 1 only
./demo/record.sh --part1

# Part 2 only
./demo/record.sh --part2

# All versions
./demo/record.sh --all
```

Recordings are saved to `demo/recordings/` as both GIF and MP4.

### Customize Recordings

Edit the `.tape` files to adjust:
- `Set FontSize` - Terminal font size
- `Set Width/Height` - Terminal dimensions
- `Set Theme` - Color theme (Dracula, Monokai, etc.)
- `Set TypingSpeed` - Typing animation speed
- `Sleep` durations between slides

## File Structure

```
demo/
├── presentation.py      # CLI entry point
├── presenter.py         # Presentation engine
├── content/
│   ├── part1_background.py   # Sections 1-3
│   └── part2_tool.py         # Sections 4-8
├── components/
│   ├── vcf_snippets.py       # Code examples & tables
│   └── diagrams.py           # ASCII diagrams
├── record.tape               # Full demo VHS script
├── record-part1.tape         # Part 1 VHS script
├── record-part2.tape         # Part 2 VHS script
├── record.sh                 # Recording helper
└── recordings/               # Output directory
```
