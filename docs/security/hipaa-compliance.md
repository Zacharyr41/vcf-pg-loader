# HIPAA Security Rule Compliance

This document maps vcf-pg-loader's security controls to the HIPAA Security Rule (45 CFR Part 164 Subpart C) requirements for electronic Protected Health Information (ePHI).

## Quick Reference

```bash
# Run all compliance checks
vcf-pg-loader compliance check

# Check specific control
vcf-pg-loader compliance check --check TLS_ENABLED

# Generate compliance report
vcf-pg-loader compliance report --format json
```

## Compliance Status Overview

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| §164.312(a)(1) Access Control | ✓ | `auth/` module |
| §164.312(a)(2)(i) Unique User ID | ✓ | UUID-based users |
| §164.312(a)(2)(ii) Emergency Access | ✓ | `auth/emergency_access.py` |
| §164.312(a)(2)(iii) Automatic Logoff | ✓ | `auth/session_manager.py` |
| §164.312(a)(2)(iv) Encryption | ✓ | `security/encryption.py` |
| §164.312(b) Audit Controls | ✓ | `audit/` module |
| §164.312(d) Authentication | ✓ | `auth/mfa.py` |
| §164.312(e)(1) Transmission Security | ✓ | `tls.py` |
| §164.316(b)(2)(i) 6-Year Retention | ✓ | `audit/retention.py` |

---

## §164.312(a)(1) - Access Control (Standard)

> "Implement technical policies and procedures for electronic information systems that maintain electronic protected health information to allow access only to those persons or software programs that have been granted access rights."

### Implementation

**Role-Based Access Control (RBAC)**
- `src/vcf_pg_loader/auth/roles.py` - Role management with minimum necessary principle
- `src/vcf_pg_loader/auth/permissions.py` - Granular permission system
- Roles: `admin`, `analyst`, `auditor`, `viewer`

**User Management**
- `src/vcf_pg_loader/auth/users.py` - User lifecycle management
- `src/vcf_pg_loader/auth/authentication.py` - Argon2id password hashing (NIST 800-63B)

**Session Control**
- `src/vcf_pg_loader/auth/session_manager.py` - Concurrent session limits
- Default: max 3 concurrent sessions per user

### Compliance Check

```bash
vcf-pg-loader compliance check --check AUTH_REQUIRED
vcf-pg-loader compliance check --check RBAC_CONFIGURED
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `VCF_PG_LOADER_MAX_SESSIONS` | `3` | Max concurrent sessions per user |
| `VCF_PG_LOADER_LOCKOUT_THRESHOLD` | `5` | Failed login attempts before lockout |
| `VCF_PG_LOADER_LOCKOUT_DURATION` | `30` | Lockout duration in minutes |

---

## §164.312(a)(2)(i) - Unique User Identification (REQUIRED)

> "Assign a unique name and/or number for identifying and tracking user identity."

### Implementation

- UUID-based `user_id` primary key in `users` table
- Unique `username` constraint
- Email uniqueness enforced

**Database Schema** (`src/vcf_pg_loader/db/schema/users_tables.sql`):
```sql
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    -- ...
);
```

### Audit Trail

All actions are logged with:
- `user_id` - Unique identifier
- `user_name` - Username for readability
- `session_id` - UUID linking to specific session

---

## §164.312(a)(2)(ii) - Emergency Access Procedure (REQUIRED)

> "Establish (and implement as needed) procedures for obtaining necessary electronic protected health information during an emergency."

### Implementation

**Module**: `src/vcf_pg_loader/auth/emergency_access.py`

**Features**:
- Time-limited tokens (max 24 hours, default 60 minutes)
- Mandatory justification (minimum 20 characters)
- Emergency type classification
- Post-incident review workflow
- Enhanced audit logging

**Emergency Types**:
- `patient_emergency` - Direct patient care needs
- `system_emergency` - System recovery scenarios
- `disaster_recovery` - Disaster situations
- `legal_requirement` - Legal/regulatory needs

### Usage

```python
from vcf_pg_loader.auth.emergency_access import EmergencyAccessManager, EmergencyType

manager = EmergencyAccessManager(audit_logger=audit_logger)

# Grant emergency access
token = await manager.grant_access(
    conn,
    user_id=123,
    justification="Patient in critical condition requiring immediate genomic analysis for treatment decision",
    emergency_type=EmergencyType.PATIENT_EMERGENCY,
    duration_minutes=60,
)

# Validate token for resource access
is_valid, token, message = await manager.validate_token(conn, token.token_id)

# Complete post-incident review
await manager.complete_review(
    conn,
    token_id=token.token_id,
    reviewed_by=admin_user_id,
    review_notes="Access was appropriate. Patient treatment successful.",
)
```

### Compliance Check

```bash
vcf-pg-loader compliance check --check EMERGENCY_ACCESS
```

### Database Tables

- `emergency_access_tokens` - Active/historical tokens
- `emergency_access_audit` - Detailed action log
- `v_active_emergency_tokens` - View of active tokens
- `v_pending_emergency_reviews` - Tokens awaiting review

---

## §164.312(a)(2)(iii) - Automatic Logoff (REQUIRED)

> "Implement electronic procedures that terminate an electronic session after a predetermined time of inactivity."

### Implementation

**Module**: `src/vcf_pg_loader/auth/session_manager.py`

**Features**:
- Inactivity timeout (default 30 minutes)
- Absolute session timeout (default 8 hours)
- Activity extension on interaction
- Session history tracking

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `VCF_PG_LOADER_INACTIVITY_TIMEOUT` | `30` | Minutes of inactivity before logoff |
| `VCF_PG_LOADER_ABSOLUTE_TIMEOUT` | `8` | Maximum session duration in hours |
| `VCF_PG_LOADER_EXTEND_ON_ACTIVITY` | `true` | Extend session on activity |

### Compliance Check

```bash
vcf-pg-loader compliance check --check SESSION_TIMEOUT
```

---

## §164.312(a)(2)(iv) - Encryption and Decryption (Addressable)

> "Implement a mechanism to encrypt and decrypt electronic protected health information."

**Note**: While "addressable," encryption is effectively required for PHI breach safe harbor under 45 CFR 164.402.

### Implementation

**Module**: `src/vcf_pg_loader/security/encryption.py`

**Algorithm**: AES-256-GCM (per NIST SP 800-111)
- Cipher: AES-256 (FIPS 197)
- Mode: GCM (Galois/Counter Mode)
- Nonce: 96-bit random per encryption
- Provides: Confidentiality + Integrity

**Key Management**:
- Master key stored externally (environment variable or KMS)
- Data keys stored encrypted in database
- Key rotation support (NIST SP 800-57)
- Key versioning for re-encryption

### Usage

```python
from vcf_pg_loader.security.encryption import EncryptionManager, KeyPurpose

# Initialize with master key
manager = EncryptionManager()  # Reads VCF_PG_LOADER_MASTER_KEY

# Create encryption key for PHI
key = await manager.create_key(
    conn,
    key_name="phi_key",
    purpose=KeyPurpose.PHI_ENCRYPTION,
    expires_days=365,
)

# Get key for encryption
data_key, key_meta = await manager.get_key(conn, KeyPurpose.PHI_ENCRYPTION)

# Encrypt PHI
ciphertext = manager.encrypt(data_key, plaintext_bytes)

# Rotate keys periodically
new_key = await manager.rotate_key(
    conn,
    key_name="phi_key",
    rotated_by=admin_user_id,
    reason="Annual rotation",
)
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `VCF_PG_LOADER_MASTER_KEY` | (required) | Base64-encoded 32-byte master key |
| `VCF_PG_LOADER_PHI_KEY` | - | Legacy column-level PHI key |

### Compliance Check

```bash
vcf-pg-loader compliance check --check ENCRYPTION_AT_REST
```

See also: [Encryption at Rest Guide](./encryption-at-rest.md)

---

## §164.312(b) - Audit Controls (Standard)

> "Implement hardware, software, and/or procedural mechanisms that record and examine activity in information systems that contain or use electronic protected health information."

### Implementation

**Modules**:
- `src/vcf_pg_loader/audit/logger.py` - Async audit logging with batching
- `src/vcf_pg_loader/audit/integrity.py` - SHA-256 hash chain
- `src/vcf_pg_loader/audit/models.py` - Audit event types
- `src/vcf_pg_loader/audit/schema.py` - Schema management

**Features**:
- Comprehensive event logging (auth, data access, config changes)
- Immutable audit logs (UPDATE/DELETE triggers blocked)
- Cryptographic hash chain for tamper detection
- Monthly partitions for retention management
- PHI auto-sanitization in log details

### Audit Event Types

| Event Type | Description |
|------------|-------------|
| `AUTH_LOGIN` | Successful authentication |
| `AUTH_LOGOUT` | User logout |
| `AUTH_FAILED` | Failed authentication attempt |
| `SESSION_TIMEOUT` | Session expired |
| `SESSION_TERMINATED` | Session forcibly ended |
| `DATA_READ` | Data query/read |
| `DATA_WRITE` | Data insert/update |
| `DATA_DELETE` | Data deletion |
| `DATA_EXPORT` | Data export operation |
| `SCHEMA_CHANGE` | Database schema modification |
| `CONFIG_CHANGE` | Configuration change |
| `PERMISSION_CHANGE` | Permission/role change |
| `PHI_ACCESS` | PHI-specific access |
| `EMERGENCY_ACCESS` | Break-glass access |

### Hash Chain Integrity

Each audit entry includes:
- `previous_hash` - Hash of prior entry
- `entry_hash` - SHA-256 of current entry

```bash
# Verify audit integrity
vcf-pg-loader audit verify --start-date 2024-01-01 --end-date 2024-12-31
```

### Compliance Check

```bash
vcf-pg-loader compliance check --check AUDIT_ENABLED
vcf-pg-loader compliance check --check AUDIT_IMMUTABILITY
```

---

## §164.312(d) - Person or Entity Authentication (Standard)

> "Implement procedures to verify that a person or entity seeking access to electronic protected health information is the one claimed."

### Implementation

**Modules**:
- `src/vcf_pg_loader/auth/authentication.py` - Primary authentication
- `src/vcf_pg_loader/auth/mfa.py` - Multi-factor authentication

**Primary Factor** (Something Known):
- Argon2id password hashing (NIST 800-63B)
- Password policy enforcement (complexity, history, expiration)
- Account lockout after failed attempts

**Second Factor** (Something Possessed):
- TOTP per RFC 6238
- Compatible with Google Authenticator, Authy, etc.
- 10 single-use recovery codes

### MFA Usage

```python
from vcf_pg_loader.auth.mfa import MFAManager

manager = MFAManager(audit_logger=audit_logger)

# Begin enrollment
enrollment = await manager.enroll(conn, user_id=123)
# enrollment.provisioning_uri -> QR code for authenticator app
# enrollment.recovery_codes -> Backup codes

# Confirm enrollment with valid TOTP code
success = await manager.confirm_enrollment(conn, user_id=123, code="123456")

# Verify MFA during login
is_valid = await manager.verify_code(conn, user_id=123, code="654321")

# Emergency: use recovery code
is_valid = await manager.verify_recovery_code(conn, user_id=123, recovery_code="ABCD-EFGH")
```

### Compliance Check

```bash
vcf-pg-loader compliance check --check MFA_ENABLED
```

---

## §164.312(e)(1) - Transmission Security (Standard)

> "Implement technical security measures to guard against unauthorized access to electronic protected health information that is being transmitted over an electronic communications network."

### Implementation

**Module**: `src/vcf_pg_loader/tls.py`

**Features**:
- TLS 1.2 minimum (TLS 1.3 preferred)
- Strong cipher suites only
- Certificate verification
- Client certificate support

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `VCF_PG_LOADER_REQUIRE_TLS` | `true` | Require TLS for connections |
| `VCF_PG_LOADER_TLS_VERIFY` | `true` | Verify server certificate |
| `VCF_PG_LOADER_TLS_CA_CERT` | - | CA certificate path |
| `VCF_PG_LOADER_TLS_CLIENT_CERT` | - | Client certificate path |
| `VCF_PG_LOADER_TLS_CLIENT_KEY` | - | Client key path |

### Compliance Check

```bash
vcf-pg-loader compliance check --check TLS_ENABLED
```

See also: [TLS Setup Guide](./tls-setup.md)

---

## §164.316(b)(2)(i) - Documentation Retention (REQUIRED)

> "Retain the documentation required by paragraph (b)(1) of this section for 6 years from the date of its creation or the date when it last was in effect, whichever is later."

### Implementation

**Module**: `src/vcf_pg_loader/audit/retention.py`

**Features**:
- 6-year minimum retention enforced via database constraint
- Deletion blocking within retention window
- Partition archival (detach, preserve, don't delete)
- Retention status monitoring

### Database Constraint

```sql
CREATE TABLE audit_retention_policy (
    policy_id SERIAL PRIMARY KEY,
    retention_years INTEGER NOT NULL CHECK (retention_years >= 6),
    enforce_minimum BOOLEAN NOT NULL DEFAULT true,
    -- ...
);
```

### Usage

```python
from vcf_pg_loader.audit.retention import AuditRetentionManager

manager = AuditRetentionManager()

# Set retention policy (cannot be < 6 years)
policy = await manager.set_retention_policy(
    conn,
    retention_years=7,  # Minimum 6
    enforce_minimum=True,
)

# Check if deletion is allowed
is_allowed, reason = await manager.check_deletion_allowed(conn, partition_date)

# Verify retention integrity
is_valid, issues = await manager.verify_retention_integrity(conn)
```

### Compliance Check

```bash
vcf-pg-loader compliance check --check AUDIT_RETENTION
```

---

## §164.530(j) - Retention and Disposal

> "A covered entity must... retain the policies and procedures... for six years..."

### Data Disposal Implementation

**Module**: `src/vcf_pg_loader/data/disposal.py`

**Features**:
- Two-person authorization (configurable)
- Pre-disposal verification
- Post-disposal verification
- Certificate of destruction generation
- Complete audit trail

### Disposal Workflow

1. **Request**: User requests disposal with reason
2. **Authorize**: Second user authorizes (if two-person enabled)
3. **Execute**: Data is deleted
4. **Verify**: System confirms data removed
5. **Certificate**: Destruction certificate generated

```python
from vcf_pg_loader.data.disposal import DataDisposal

disposal = DataDisposal(pool, audit_logger=audit_logger, require_two_person_auth=True)

# Request disposal
disposal_id = await disposal.request_disposal(
    disposal_type=DisposalType.BATCH,
    target_identifier=str(batch_id),
    reason="Retention period expired",
    authorized_by=user_id,
)

# Second authorization (if required)
await disposal.authorize_disposal(disposal_id, second_authorizer_id)

# Execute
result = await disposal.execute_disposal(disposal_id, executor_id)

# Verify
verification = await disposal.verify_disposal(disposal_id, verifier_id)

# Generate certificate
certificate = await disposal.generate_disposal_certificate(disposal_id)
```

---

## PHI Detection and De-identification

**Module**: `src/vcf_pg_loader/phi/`

### §164.514(b) - De-identification Standard

**PHI Detector** (`phi/detector.py`):
- Scans VCF files for PHI patterns
- Configurable pattern registry
- Risk level assessment

**Patterns Detected** (`phi/patterns.py`):
- SSN, MRN, names, dates of birth
- Email addresses, phone numbers
- Addresses
- Sample IDs with identifying patterns

```bash
# Scan VCF for PHI
vcf-pg-loader phi scan sample.vcf

# Scan with masking
vcf-pg-loader phi scan sample.vcf --mask-output
```

---

## Automated Compliance Checks

| Check ID | Severity | HIPAA Reference | Description |
|----------|----------|-----------------|-------------|
| `TLS_ENABLED` | CRITICAL | §164.312(e)(1) | TLS encryption in transit |
| `AUDIT_ENABLED` | CRITICAL | §164.312(b) | Audit logging active |
| `AUDIT_IMMUTABILITY` | CRITICAL | §164.312(b) | Audit logs are immutable |
| `AUTH_REQUIRED` | CRITICAL | §164.312(a)(1) | Authentication required |
| `RBAC_CONFIGURED` | HIGH | §164.312(a)(1) | Role-based access control |
| `ENCRYPTION_AT_REST` | HIGH | §164.312(a)(2)(iv) | Data encryption configured |
| `SESSION_TIMEOUT` | MEDIUM | §164.312(a)(2)(iii) | Auto-logoff enabled |
| `PASSWORD_POLICY` | MEDIUM | §164.312(d) | Password requirements |
| `LOGIN_LOCKOUT` | MEDIUM | §164.312(a)(1) | Account lockout policy |
| `DATA_CLASSIFICATION` | LOW | §164.530 | Data classification |
| `PHI_VAULT_EXISTS` | CRITICAL | §164.530 | PHI vault schema |
| `EMERGENCY_ACCESS` | CRITICAL | §164.312(a)(2)(ii) | Break-glass procedures |
| `MFA_ENABLED` | CRITICAL | §164.312(d) | Multi-factor authentication |
| `AUDIT_RETENTION` | CRITICAL | §164.316(b)(2)(i) | 6-year retention policy |

---

## Production Checklist

### Encryption
- [ ] Master key generated with CSPRNG
- [ ] Master key stored in KMS/HSM (not environment variable in production)
- [ ] TLS certificates from trusted CA
- [ ] TLS 1.2+ enforced, weak ciphers disabled

### Authentication
- [ ] MFA required for all users
- [ ] Password policy meets NIST 800-63B
- [ ] Account lockout configured
- [ ] Session timeouts appropriate for environment

### Audit
- [ ] Audit logging enabled
- [ ] Immutability triggers verified
- [ ] Hash chain integrity verified
- [ ] 6-year retention policy active
- [ ] Audit log backup process in place

### Access Control
- [ ] RBAC configured with least privilege
- [ ] User accounts reviewed quarterly
- [ ] Emergency access procedures documented and tested
- [ ] Separation of duties implemented

### Data Protection
- [ ] PHI detection run on all VCF files
- [ ] Sample ID anonymization enabled
- [ ] Disposal procedures documented
- [ ] Two-person authorization for deletions

---

## Verification Commands

```bash
# Full compliance check
vcf-pg-loader compliance check

# System health including security
vcf-pg-loader doctor

# Verify audit integrity
vcf-pg-loader audit verify --days 365

# Check TLS connection
vcf-pg-loader doctor --check tls

# List active sessions
vcf-pg-loader auth sessions list

# Review emergency access
vcf-pg-loader auth emergency pending-reviews
```

---

## References

- [HIPAA Security Rule](https://www.hhs.gov/hipaa/for-professionals/security/index.html)
- [NIST SP 800-111 - Storage Encryption](https://csrc.nist.gov/publications/detail/sp/800-111/final)
- [NIST SP 800-63B - Digital Identity](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [45 CFR 164.312 - Technical Safeguards](https://www.ecfr.gov/current/title-45/part-164/subpart-C)
