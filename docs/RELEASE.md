# Release Runbook

This document describes how to release a new version of vcf-pg-loader.

## Quick Start

```bash
# 1. Check versions are in sync
make version-check

# 2. Run pre-flight validation
make release-check VERSION=0.5.4

# 3. (Optional) Test the release workflow
#    Push to a release/* branch or use test-release workflow

# 4. Trigger the release via GitHub Actions UI
#    Actions → Release → Run workflow → version=0.5.4
```

## Pre-Release Checklist

Before triggering a release, ensure:

- [ ] All CI tests pass on the `main` branch
- [ ] **Version sync check passes**: `make version-check`
- [ ] Version bump has NOT been done manually (the workflow handles this)
- [ ] CHANGELOG.md is updated with release notes (if maintained)
- [ ] No pending PRs that should be included
- [ ] You have the required permissions to trigger workflow dispatch

## Triggering a Release

1. Go to **Actions** → **Release** workflow
2. Click **Run workflow**
3. Fill in the inputs:
   - **Version**: Semver format (e.g., `0.5.4`) - without `v` prefix
   - **Skip Bioconda**: Check to skip BioConda recipe update
   - **Skip nf-core**: Check to skip nf-core module verification
   - **Create nf-core PR**: Check to auto-create PR to nf-core/modules (requires `NF_CORE_PAT`)
   - **nf-core update branch**: Branch name to update existing PR (see below)
   - **Dry run**: Check to test without publishing anything
4. Click **Run workflow**

## Workflow Inputs

| Input | Default | Description |
|-------|---------|-------------|
| `version` | (required) | Version to release, e.g., `0.5.4` |
| `skip_bioconda` | `false` | Skip BioConda recipe PR creation |
| `skip_nfcore` | `false` | Skip nf-core module version verification |
| `create_nfcore_pr` | `false` | Create new PR to nf-core/modules fork |
| `nfcore_update_branch` | `''` | Push to existing PR branch instead of creating new PR |
| `dry_run` | `false` | Build but don't publish; useful for testing |

## nf-core Module Management

### The Chicken-and-Egg Problem

Releasing with nf-core involves dependencies:

```
1. nf-core module references Docker image → needs Docker pushed first
2. Docker image needs version tag → needs release to happen first
3. BioConda must be merged → BioContainers builds from it (~30 min)
4. nf-core PR may need iteration → based on reviewer feedback
```

### Local Development Commands

```bash
# Lint the local nf-core module
make nf-core-lint

# Show diff against upstream nf-core/modules
make nf-core-diff

# Check fork status and open PRs
make nf-core-status
```

### Creating/Updating nf-core PRs

#### Option 1: Via Makefile (manual control)

```bash
# Create a new branch in your fork (for new PRs)
make nf-core-create-branch VERSION=0.5.4

# Update an existing branch (for PR iterations)
make nf-core-update-branch BRANCH=update-vcfpgloader-0.5.4

# Check status
make nf-core-status
```

#### Option 2: Via Release Workflow (automated)

Use workflow inputs:
- `create_nfcore_pr: true` - Create new PR
- `nfcore_update_branch: branch-name` - Update existing branch

### Workflow: Initial Submission

While your initial PR to nf-core/modules is pending (e.g., [PR #9579](https://github.com/nf-core/modules/pull/9579)):

1. Use `nfcore_update_branch` with the branch name of your open PR
2. The workflow pushes updates to that branch, updating the existing PR

```
Version: 0.5.4
nfcore_update_branch: vcfpgloader-load   # Your PR's branch name
```

### Workflow: Normal Updates

Once your module is in nf-core/modules:

1. Use `create_nfcore_pr: true`
2. Leave `nfcore_update_branch` empty
3. The workflow creates a new update PR from upstream/master

```
Version: 0.5.5
create_nfcore_pr: true
```

### Recommended Release Flow with nf-core

```bash
# 1. Before release: Create nf-core PR early (optional, helps parallelize)
make nf-core-create-branch VERSION=0.5.4
# → Manually create PR from the branch

# 2. Run release workflow
#    - Version bump, Docker push, PyPI publish happen
#    - BioConda PR created
#    - Use: nfcore_update_branch=update-vcfpgloader-0.5.4

# 3. After BioConda merges (~30 min for BioContainers):
#    - nf-core PR can be merged
#    - Or iterate with: make nf-core-update-branch BRANCH=...
```

## Expected Timeline

| Job | Duration | Description |
|-----|----------|-------------|
| Version Validation | ~30s | Validates semver format |
| Bump & Tag | ~2 min | Updates version files, creates git tag |
| nf-core Verification | ~1 min | Verifies module files have correct version |
| Docker Build & Push | 5-10 min | Multi-platform Docker image to GHCR |
| Python Build | ~2 min | Builds wheel and sdist |
| PyPI Publish | ~1 min | Publishes to PyPI via trusted publisher |
| GitHub Release | ~1 min | Creates release with artifacts |
| Wait for PyPI | 1-5 min | Polls until package is available |
| BioConda PR | ~3 min | Creates PR to bioconda-recipes |
| nf-core PR | ~2 min | Creates/updates PR to nf-core/modules (if enabled) |

**Total**: ~15-25 minutes for a full release

## What Gets Published

1. **Git tag**: `v{version}` pushed to repository
2. **PyPI**: Package published to https://pypi.org/project/vcf-pg-loader/
3. **Docker**: Image pushed to `ghcr.io/zacharyr41/vcf-pg-loader:{version}`
4. **GitHub Release**: Created with auto-generated notes and artifacts
5. **BioConda PR**: Opened against bioconda/bioconda-recipes
6. **nf-core PR**: Opened against nf-core/modules (if enabled)

## Failure Recovery

### Version Validation Failed

**Cause**: Invalid version format (not X.Y.Z)

**Fix**: Re-run with correct semver format

### Bump & Tag Failed

**Cause**: Usually git push issues or version already exists

**Manual fix**:
```bash
# If tag exists but version bump wasn't pushed
git fetch --tags
git tag -d v0.5.4
git push origin :refs/tags/v0.5.4

# Then re-run the workflow
```

### Docker Build Failed

**Cause**: Dockerfile issues, GHCR authentication, or build errors

**Manual fix**:
```bash
# Build and push manually
docker build -t ghcr.io/zacharyr41/vcf-pg-loader:0.5.4 .
docker push ghcr.io/zacharyr41/vcf-pg-loader:0.5.4
docker tag ghcr.io/zacharyr41/vcf-pg-loader:0.5.4 ghcr.io/zacharyr41/vcf-pg-loader:latest
docker push ghcr.io/zacharyr41/vcf-pg-loader:latest
```

### PyPI Publish Failed

**Cause**: Trusted publisher misconfiguration or package already exists

**Check**: Ensure the `pypi` environment exists with trusted publisher configured

**Manual fix** (if version doesn't exist on PyPI):
```bash
git checkout v0.5.4
python -m build
pip install twine
twine upload dist/*
```

### GitHub Release Failed

**Cause**: Tag doesn't exist or permissions issue

**Manual fix**: Create release manually via GitHub UI from the tag

### BioConda PR Failed

**Cause**: PAT issues, fork not set up, or recipe generation failed

**Symptoms**: An issue is automatically created with manual steps

**Manual fix**:
```bash
# Clone your bioconda-recipes fork
git clone https://github.com/YOUR_USERNAME/bioconda-recipes
cd bioconda-recipes
git remote add upstream https://github.com/bioconda/bioconda-recipes
git fetch upstream master
git checkout -b update-vcf-pg-loader-0.5.4 upstream/master

# Generate recipe
pip install grayskull
grayskull pypi vcf-pg-loader==0.5.4 -o recipes/

# Commit and push
git add recipes/vcf-pg-loader
git commit -m "Update vcf-pg-loader to 0.5.4"
git push origin update-vcf-pg-loader-0.5.4

# Create PR via GitHub UI or:
gh pr create --repo bioconda/bioconda-recipes \
  --title "Update vcf-pg-loader to 0.5.4" \
  --body "Updates vcf-pg-loader to version 0.5.4"
```

### nf-core PR Failed

**Cause**: PAT issues or fork not set up

**Manual fix**:
```bash
# Clone your nf-core-modules fork
git clone https://github.com/YOUR_USERNAME/nf-core-modules
cd nf-core-modules
git remote add upstream https://github.com/nf-core/modules
git fetch upstream master
git checkout -b update-vcfpgloader-0.5.4 upstream/master

# Copy module files from this repo
cp -r /path/to/vcf-pg-loader/nf-core/modules/vcfpgloader modules/nf-core/

# Lint
pip install nf-core
nf-core modules lint vcfpgloader/load

# Commit and push
git add modules/nf-core/vcfpgloader
git commit -m "Update vcfpgloader module to 0.5.4"
git push origin update-vcfpgloader-0.5.4

# Create PR
gh pr create --repo nf-core/modules \
  --title "Update vcfpgloader module to 0.5.4" \
  --body "Updates vcfpgloader/load module to version 0.5.4"
```

## Dry Run Mode

Use dry run to test the workflow without publishing:

1. Check **Dry run** when triggering
2. The workflow will:
   - Validate version format
   - Run `bump-my-version` locally (not pushed)
   - Build Python package (uploaded as artifact)
   - Skip PyPI, Docker, GitHub Release, BioConda, nf-core

Review the workflow output and download build artifacts to verify.

## Version Management

### Version Locations

The following files contain version references, all managed by `bump-my-version`:

| File | Pattern |
|------|---------|
| `pyproject.toml` | `version = "X.Y.Z"` |
| `bioconda/meta.yaml` | `{% set version = "X.Y.Z" %}` |
| `nf-core/modules/vcfpgloader/load/main.nf` | Container tag |
| `nf-core/modules/vcfpgloader/load/environment.yml` | BioConda package |

### Checking Version Sync

```bash
# Quick check - shows all versions and whether they match
make version-check

# Or run the script directly
./scripts/check-version-sync.sh
```

Output:
```
Version References
==================
File                                          Version
----                                          -------
pyproject.toml                                0.5.3
bioconda/meta.yaml                            0.5.3
nf-core/.../main.nf (container)               0.5.3
nf-core/.../environment.yml (bioconda)        0.5.3

All versions in sync: 0.5.3
```

### Pre-Flight Validation

Before releasing, run the full validation:

```bash
# Without target version (checks current state)
make release-check

# With target version (shows what would change)
make release-check VERSION=0.5.4

# Skip tests for faster validation
./scripts/validate-release.sh --skip-tests 0.5.4
```

This checks:
- Version sync across all files
- Git state (clean, on main)
- bump-my-version preview
- Tests pass (optional)

### CI Version Sync Check

The CI workflow automatically checks version sync on every PR to `main`. If versions drift, the PR will fail with a clear error message showing which files are out of sync.

## Test Release Workflow

Before doing a real release, you can test everything with the dry-run workflow:

### Option 1: Push to release/* branch

```bash
git checkout -b release/test-0.5.4
git push origin release/test-0.5.4
```

This triggers `.github/workflows/test-release.yml` which:
- Checks version sync
- Builds the Python package
- Builds Docker image (no push)
- Validates nf-core module
- Generates summary of what would be published

### Option 2: Manual trigger

1. Go to **Actions** → **Test Release**
2. Click **Run workflow**
3. Optionally enter a version to test bump preview
4. Review the workflow summary

## Post-Release Verification

After a successful release:

1. **PyPI**: Verify at https://pypi.org/project/vcf-pg-loader/{version}/
2. **Docker**: Test with `docker pull ghcr.io/zacharyr41/vcf-pg-loader:{version}`
3. **GitHub Release**: Check release notes at https://github.com/Zacharyr41/vcf-pg-loader/releases
4. **BioConda**: Monitor PR status (takes ~30 min to build after merge)
5. **nf-core**: Monitor PR if created

## Rollback

If a release has critical issues:

1. **PyPI**: Cannot delete releases, but you can yank:
   - Go to https://pypi.org/manage/project/vcf-pg-loader/releases/
   - Yank the version (marks as not recommended)

2. **Docker**: Push a fixed version or delete the tag from GHCR

3. **Git**: Delete the tag (use sparingly):
   ```bash
   git tag -d v0.5.4
   git push origin :refs/tags/v0.5.4
   ```

4. **GitHub Release**: Delete via the UI

5. **BioConda/nf-core**: Close the PR if not merged
