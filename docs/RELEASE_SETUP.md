# Release Setup Guide

One-time setup required before using the automated release workflow.

## Prerequisites

- Repository admin access
- GitHub account with PAT creation permissions
- Forked repositories (see below)

## 1. PyPI Trusted Publisher

Configure PyPI to accept publishes from GitHub Actions without API tokens.

### Create the `pypi` Environment

1. Go to **Settings** → **Environments**
2. Click **New environment**
3. Name it exactly: `pypi`
4. (Optional) Add protection rules:
   - Require reviewers for releases
   - Limit to `main` branch
5. Click **Save protection rules**

### Configure Trusted Publisher on PyPI

1. Go to https://pypi.org/manage/account/publishing/
2. Click **Add a new pending publisher** (if package doesn't exist yet) or go to your project settings
3. Fill in:
   - **PyPI Project Name**: `vcf-pg-loader`
   - **Owner**: `Zacharyr41`
   - **Repository name**: `vcf-pg-loader`
   - **Workflow name**: `release.yml`
   - **Environment name**: `pypi`
4. Click **Add**

The workflow uses `pypa/gh-action-pypi-publish@release/v1` which handles OIDC authentication automatically.

## 2. BioConda PAT Setup

Required for automated BioConda recipe PRs.

### Fork bioconda-recipes

1. Go to https://github.com/bioconda/bioconda-recipes
2. Click **Fork**
3. Keep the default name: `bioconda-recipes`
4. Your fork URL: `https://github.com/YOUR_USERNAME/bioconda-recipes`

### Create Personal Access Token

1. Go to https://github.com/settings/tokens?type=beta (Fine-grained tokens)
2. Click **Generate new token**
3. Configure:
   - **Token name**: `vcf-pg-loader-bioconda`
   - **Expiration**: 90 days (or your preference)
   - **Repository access**: Select repositories → `YOUR_USERNAME/bioconda-recipes`
   - **Permissions**:
     - **Contents**: Read and write
     - **Pull requests**: Read and write
     - **Metadata**: Read-only (auto-selected)
4. Click **Generate token**
5. Copy the token immediately

### Add Secret to Repository

1. Go to vcf-pg-loader repo → **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret**
3. Name: `BIOCONDA_PAT`
4. Value: Paste the token
5. Click **Add secret**

## 3. nf-core Modules PAT Setup (Optional)

Required only if you want automated PRs to nf-core/modules.

### Fork nf-core/modules

1. Go to https://github.com/nf-core/modules
2. Click **Fork**
3. Name it: `nf-core-modules` (the workflow expects this name)
4. Your fork URL: `https://github.com/YOUR_USERNAME/nf-core-modules`

### Create Personal Access Token

1. Go to https://github.com/settings/tokens?type=beta
2. Click **Generate new token**
3. Configure:
   - **Token name**: `vcf-pg-loader-nfcore`
   - **Expiration**: 90 days
   - **Repository access**: Select repositories → `YOUR_USERNAME/nf-core-modules`
   - **Permissions**:
     - **Contents**: Read and write
     - **Pull requests**: Read and write
     - **Metadata**: Read-only
4. Click **Generate token**
5. Copy the token

### Add Secret to Repository

1. Go to vcf-pg-loader repo → **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret**
3. Name: `NF_CORE_PAT`
4. Value: Paste the token
5. Click **Add secret**

## 4. GHCR (GitHub Container Registry)

Docker publishing uses `GITHUB_TOKEN` automatically. No additional setup required.

Verify packages are enabled:
1. Go to **Settings** → **Actions** → **General**
2. Under **Workflow permissions**, ensure **Read and write permissions** is selected

## 5. Verify Setup

### Check Secrets Exist

Go to **Settings** → **Secrets and variables** → **Actions**

Required secrets:
- [x] `BIOCONDA_PAT` - For BioConda recipe PRs

Optional secrets:
- [ ] `NF_CORE_PAT` - For nf-core/modules PRs (only if using `create_nfcore_pr` option)

### Check Environments Exist

Go to **Settings** → **Environments**

Required environments:
- [x] `pypi` - For PyPI trusted publisher

### Test with Dry Run

1. Go to **Actions** → **Release**
2. Click **Run workflow**
3. Enter a test version (e.g., `99.0.0`)
4. Check **Dry run**
5. Run and verify all jobs pass

## Troubleshooting

### "Resource not accessible by integration"

- Check that workflow permissions are set to "Read and write"
- Verify the secret names match exactly

### PyPI publish fails with "Invalid token"

- Ensure the `pypi` environment exists
- Verify trusted publisher is configured with exact workflow name (`release.yml`)
- Check that environment name matches (`pypi`)

### BioConda PR fails

- Verify `BIOCONDA_PAT` secret exists
- Check token hasn't expired
- Ensure fork exists at `YOUR_USERNAME/bioconda-recipes`
- Token needs repo scope for the fork

### nf-core PR fails

- Verify `NF_CORE_PAT` secret exists
- Check token hasn't expired
- Ensure fork exists at `YOUR_USERNAME/nf-core-modules` (exact name matters)

## Token Renewal

PATs expire. Set a calendar reminder to renew them before expiration.

To renew:
1. Create new token following steps above
2. Update the repository secret with new value
3. Delete the old token from your GitHub settings

## Security Notes

- Use fine-grained PATs with minimal scope
- Limit token access to specific repositories (forks only)
- Set reasonable expiration times
- Never commit tokens to the repository
- Review workflow runs periodically for suspicious activity
