# Encryption at Rest

HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption

While encryption at rest is "addressable" (not required) under HIPAA, it is strongly recommended for clinical genomic data. This document covers the encryption options available with vcf-pg-loader.

## Encryption Options

### 1. Column-Level Encryption (Built-in)

vcf-pg-loader includes AES-256-GCM encryption for PHI in the `phi_vault.sample_id_mapping` table. This encrypts the `original_id` column while maintaining the anonymous UUID mapping.

**Pros:**
- Application-controlled encryption
- Key rotation without database restart
- Works with any PostgreSQL deployment
- No additional infrastructure required

**Cons:**
- Only protects specific columns
- Application must manage keys
- Slight performance overhead

**Setup:**
```bash
# Generate a new encryption key
vcf-pg-loader security generate-key

# Set the key in your environment
export VCF_PG_LOADER_PHI_KEY="<base64-encoded-key>"

# Verify encryption is enabled
vcf-pg-loader security check-encryption
```

### 2. PostgreSQL Transparent Data Encryption (TDE)

PostgreSQL Enterprise and some forks (EnterpriseDB, Percona) support TDE.

**Pros:**
- Encrypts all data transparently
- No application changes required
- Protects against physical disk theft

**Cons:**
- Enterprise feature (not in community PostgreSQL)
- Key management complexity
- Performance impact on all operations

### 3. Filesystem-Level Encryption

Use LUKS/dm-crypt (Linux) or FileVault (macOS) to encrypt the PostgreSQL data directory.

**Pros:**
- Encrypts everything on disk
- OS-level, no database changes
- Protects against physical theft

**Cons:**
- Data unencrypted when system is running
- Key stored in memory while mounted
- Requires OS-level configuration

See: `scripts/setup-encrypted-volume.sh` for Docker volume encryption.

### 4. Cloud Provider Encryption

All major cloud providers offer encryption at rest:

| Provider | Service | Documentation |
|----------|---------|---------------|
| AWS | RDS PostgreSQL | [docs/deployment/aws-rds-encryption.md](../deployment/aws-rds-encryption.md) |
| GCP | Cloud SQL | [docs/deployment/gcp-cloudsql-encryption.md](../deployment/gcp-cloudsql-encryption.md) |
| Azure | Database for PostgreSQL | [docs/deployment/azure-encryption.md](../deployment/azure-encryption.md) |

## Column-Level Encryption Details

### Algorithm

- **Cipher:** AES-256-GCM (Galois/Counter Mode)
- **Key Size:** 256 bits (32 bytes)
- **IV Size:** 96 bits (12 bytes) - randomly generated per encryption
- **Authentication:** Built-in with GCM mode

### Key Storage Options

#### Environment Variable (Default)

```bash
export VCF_PG_LOADER_PHI_KEY="<base64-encoded-32-byte-key>"
```

#### Key File

```bash
# Create key file with restricted permissions
vcf-pg-loader security generate-key --output /etc/vcf-pg-loader/phi.key
chmod 600 /etc/vcf-pg-loader/phi.key

# Point to key file
export VCF_PG_LOADER_PHI_KEY_FILE="/etc/vcf-pg-loader/phi.key"
```

#### Cloud KMS (Enterprise)

For production deployments, use cloud KMS for key management:

- AWS KMS
- GCP Cloud KMS
- Azure Key Vault

See cloud deployment guides for configuration details.

### Key Rotation

Key rotation re-encrypts all data with a new key without downtime:

```bash
# Generate new key
NEW_KEY=$(vcf-pg-loader security generate-key --raw)

# Rotate (old key from environment, new key provided)
vcf-pg-loader security rotate-key \
  --new-key "$NEW_KEY" \
  --batch-size 1000

# Update environment with new key
export VCF_PG_LOADER_PHI_KEY="$NEW_KEY"
```

**Best Practices:**
- Rotate keys annually or per security policy
- Test rotation in non-production first
- Back up old key until rotation verified
- Audit key rotation events

## Database Schema

The `phi_vault.sample_id_mapping` table includes:

```sql
CREATE TABLE phi_vault.sample_id_mapping (
    mapping_id BIGSERIAL PRIMARY KEY,
    anonymous_id UUID NOT NULL UNIQUE,
    original_id TEXT NOT NULL,  -- Plaintext (for legacy/non-encrypted)
    original_id_encrypted BYTEA,  -- AES-256-GCM ciphertext
    encryption_iv BYTEA,  -- 12-byte IV for decryption
    -- ... other columns
);
```

When encryption is enabled:
- `original_id_encrypted` contains the ciphertext
- `encryption_iv` contains the IV
- `original_id` is still stored (for queries) but can be cleared

## Verifying Encryption

### Check Configuration

```bash
vcf-pg-loader security check-encryption
```

Output:
```
PHI Encryption Status
  Status: Enabled
  Algorithm: AES-256-GCM
  Key Source: environment
  Library: cryptography 42.0.0
```

### Check Database

```sql
-- Count encrypted vs unencrypted mappings
SELECT
    COUNT(*) FILTER (WHERE original_id_encrypted IS NOT NULL) as encrypted,
    COUNT(*) FILTER (WHERE original_id_encrypted IS NULL) as unencrypted
FROM phi_vault.sample_id_mapping;
```

## Security Considerations

1. **Key Security:** The encryption key is the single point of security. Protect it appropriately.

2. **Memory Safety:** Keys are held in memory during operation. Consider memory-safe deployments.

3. **Backup Encryption:** Ensure database backups are also encrypted (pg_dump + encryption).

4. **Audit Logging:** All key operations should be logged (handled by vcf-pg-loader audit system).

5. **Defense in Depth:** Column encryption should complement, not replace, other security measures:
   - TLS in transit
   - Access controls
   - Network segmentation
   - Audit logging

## Compliance Checklist

- [ ] Encryption key generated with cryptographically secure random source
- [ ] Key stored in secure location (KMS, HSM, or encrypted secrets manager)
- [ ] Key rotation policy documented and tested
- [ ] Encryption status verified with `security check-encryption`
- [ ] Database shows encrypted values in `original_id_encrypted`
- [ ] Backup encryption configured
- [ ] Key access logged and audited
- [ ] Recovery procedures documented and tested
