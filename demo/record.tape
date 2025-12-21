# VHS Recording: vcf-pg-loader Full Demo
# Run with: vhs demo/record.tape
# Output: demo/recordings/vcf-pg-loader-demo.gif
# Timings based on ~130 words per minute narration pace

Output demo/recordings/vcf-pg-loader-demo.gif
Output demo/recordings/vcf-pg-loader-demo.mp4

# Terminal settings
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

# Start recording
Sleep 1s

# Show help first
Type "python -m demo --help"
Sleep 500ms
Enter
Sleep 3s

# List sections
Type "python -m demo --list"
Sleep 500ms
Enter
Sleep 4s

# Start the full demo
Type "python -m demo"
Sleep 500ms
Enter
Sleep 2s

# ============================================
# Part I, Section 1: VCF Files: From DNA to Data (4 slides)
# ============================================

# Slide 1: What is a VCF file? (~50 words)
Enter
Sleep 25s

# Slide 2: The Journey (~180 words - long with numbered steps)
Enter
Sleep 85s

# Slide 3: Why VCF? (~150 words)
Enter
Sleep 70s

# Slide 4: Reference vs Sample (~50 words)
Enter
Sleep 25s

# ============================================
# Part I, Section 2: Anatomy of a VCF File (14 slides)
# ============================================

# Slide 5: VCF Header (~40 words)
Enter
Sleep 20s

# Slide 6: VCF Data Line (~30 words)
Enter
Sleep 15s

# Slide 7: CHROM & POS (~60 words)
Enter
Sleep 30s

# Slide 8: ID (~50 words)
Enter
Sleep 25s

# Slide 9: REF & ALT (~70 words)
Enter
Sleep 35s

# Slide 10: QUAL & FILTER (~50 words)
Enter
Sleep 25s

# Slide 11: INFO (~150 words - detailed depth explanation)
Enter
Sleep 70s

# Slide 12: FORMAT & SAMPLE (~280 words - detailed explanation)
Enter
Sleep 130s

# Slide 13: A Simple SNP (~50 words)
Enter
Sleep 25s

# Slide 14: Variant Types (~250 words - all types explained)
Enter
Sleep 115s

# Slide 15: Deletion Example (~50 words)
Enter
Sleep 25s

# Slide 16: Multi-allelic Variants (~120 words)
Enter
Sleep 55s

# Slide 17: INFO Field Numbers (~60 words)
Enter
Sleep 30s

# Slide 18: Genotype Notation (~60 words)
Enter
Sleep 30s

# ============================================
# Part I, Section 3: VCF in Rare Disease Research (6 slides)
# ============================================

# Slide 19: The Challenge (~50 words)
Enter
Sleep 25s

# Slide 20: Impact Levels (~60 words)
Enter
Sleep 30s

# Slide 21: Filtering Criteria (~50 words)
Enter
Sleep 25s

# Slide 22: Inheritance Patterns (~280 words - detailed Mendelian)
Enter
Sleep 130s

# Slide 23: Filtering Cascade (~50 words)
Enter
Sleep 25s

# Slide 24: Why SQL? (~60 words)
Enter
Sleep 30s

# ============================================
# Part II, Section 4: Previous Tools (3 slides)
# ============================================

# Slide 25: GEMINI (~70 words)
Enter
Sleep 35s

# Slide 26: slivar (~50 words)
Enter
Sleep 25s

# Slide 27: The Gap (~50 words)
Enter
Sleep 25s

# ============================================
# Part II, Section 5: vcf-pg-loader Architecture (13 slides)
# ============================================

# Slide 28: Tool Comparison (~40 words)
Enter
Sleep 20s

# Slide 29: Architecture Overview (~50 words)
Enter
Sleep 25s

# Slide 30: VCF Parser (~130 words)
Enter
Sleep 60s

# Slide 31: Normalizer (~120 words)
Enter
Sleep 55s

# Slide 32: Schema Manager (~100 words)
Enter
Sleep 45s

# Slide 33: Binary COPY (~130 words)
Enter
Sleep 60s

# Slide 34: PostgreSQL (~120 words)
Enter
Sleep 55s

# Slide 35: Data Flow - VCF Input (~60 words)
Enter
Sleep 30s

# Slide 36: Data Flow - Parser (~50 words)
Enter
Sleep 25s

# Slide 37: Data Flow - Normalization (~50 words)
Enter
Sleep 25s

# Slide 38: Data Flow - Binary COPY (~50 words)
Enter
Sleep 25s

# Slide 39: Data Flow - Query Ready (~50 words)
Enter
Sleep 25s

# Slide 40: Zero-Config (~50 words)
Enter
Sleep 25s

# ============================================
# Part II, Section 6: Research Pipeline Walkthrough (9 slides)
# ============================================

# Slide 41: Pipeline - Trio VCF Files (~80 words)
Enter
Sleep 40s

# Slide 42: Pipeline - vcf-pg-loader (~70 words)
Enter
Sleep 35s

# Slide 43: Pipeline - Unified Database (~70 words)
Enter
Sleep 35s

# Slide 44: Pipeline - Inheritance Queries (~80 words)
Enter
Sleep 40s

# Slide 45: Load the Data (~120 words - expanded with timing details)
Enter
Sleep 55s

# Slide 46: Query for Candidates (~60 words)
Enter
Sleep 30s

# Slide 47: Compound Heterozygotes (~60 words)
Enter
Sleep 30s

# Slide 48: Adding Samples (~70 words)
Enter
Sleep 35s

# Slide 49: Iterative Research (~60 words)
Enter
Sleep 30s

# ============================================
# Part II, Section 7: Performance & Compliance (4 slides)
# ============================================

# Slide 50: Benchmarks (~50 words)
Enter
Sleep 25s

# Slide 51: Why PostgreSQL? (~50 words)
Enter
Sleep 25s

# Slide 52: Clinical Compliance (~50 words)
Enter
Sleep 25s

# Slide 53: Validation (~40 words)
Enter
Sleep 20s

# ============================================
# Part II, Section 8: Future: Vector Embeddings (4 slides)
# ============================================

# Slide 54: The Vision (~50 words)
Enter
Sleep 25s

# Slide 55: Vector Architecture (~50 words)
Enter
Sleep 25s

# Slide 56: Applications (~60 words)
Enter
Sleep 30s

# Slide 57: Thank You (~50 words)
Enter
Sleep 25s

# Final pause before end
Sleep 5s
