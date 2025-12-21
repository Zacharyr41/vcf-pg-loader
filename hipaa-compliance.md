# vcf-pg-loader HIPAA Compliance Implementation Plan

## Executive Summary

This document provides a phased implementation plan to make vcf-pg-loader HIPAA-compliant for clinical research use with real patient genomic data. The plan is organized into **6 phases** with **15 discrete implementation chunks**, each designed to be completed in a single Claude Code session.

### Current State Assessment (from GitHub repo)

**Existing Features:**
- Streaming VCF parsing with cyvcf2
- PostgreSQL loading via asyncpg binary COPY
- Basic audit trail (load batch tracking only)
- Docker support with auto-managed PostgreSQL
- TOML configuration files
- CLI with Typer

**Critical Gaps for HIPAA Compliance:**
1. ❌ No user authentication/authorization
2. ❌ No encryption at rest
3. ❌ No enforced TLS for database connections
4. ❌ Audit logging lacks user attribution and immutability
5. ❌ No sample de-identification capabilities
6. ❌ Credentials stored in config files
7. ❌ No session management or automatic logoff
8. ❌ No security documentation

---

## Priority Matrix

| Priority | Component | HIPAA Requirement | Risk Level |
|----------|-----------|-------------------|------------|
| P0 | Encryption in transit (TLS) | 164.312(e)(1) | Critical |
| P0 | Audit logging enhancement | 164.312(b) | Critical |
| P0 | Credential security | 164.312(d) | Critical |
| P1 | User authentication | 164.312(d) | High |
| P1 | Access controls (RBAC) | 164.312(a)(1) | High |
| P1 | Sample de-identification | 164.514(b) | High |
| P2 | Encryption at rest | 164.312(a)(2)(iv) | Medium |
| P2 | Session management | 164.312(a)(2)(iii) | Medium |
| P2 | VCF header sanitization | 164.514(b) | Medium |
| P3 | Security documentation | 164.316 | Medium |
| P3 | Docker hardening | 164.310(c) | Medium |

---

## Phase 0: Foundation & Security Infrastructure (Week 1)

### Chunk 0.1: Secure Configuration Management

**Goal:** Remove credentials from config files, implement secrets management

**Files to create/modify:**
- `src/vcf_pg_loader/config/secrets.py`
- `src/vcf_pg_loader/config/settings.py` (modify)
- `.env.example`
- `docs/security/credentials.md`

---

### Chunk 0.2: TLS Enforcement for PostgreSQL

**Goal:** Require TLS 1.2+ for all database connections

**Files to create/modify:**
- `src/vcf_pg_loader/db/connection.py` (modify)
- `docker/postgres/postgresql.conf`
- `docker/postgres/pg_hba.conf`
- `docker/certs/` (directory structure)

---

## Phase 1: Audit Logging System (Weeks 2-3)

### Chunk 1.1: Comprehensive Audit Schema

**Goal:** Create HIPAA-compliant audit logging database schema

**Files to create/modify:**
- `src/vcf_pg_loader/db/schema/audit_tables.sql`
- `src/vcf_pg_loader/db/schema_manager.py` (modify)

---

### Chunk 1.2: Audit Event Capture System

**Goal:** Implement audit event capture for all PHI access

**Files to create/modify:**
- `src/vcf_pg_loader/audit/events.py`
- `src/vcf_pg_loader/audit/logger.py`
- `src/vcf_pg_loader/audit/models.py`

---

### Chunk 1.3: Audit Log Immutability & Integrity

**Goal:** Ensure audit logs cannot be modified or deleted

**Files to create/modify:**
- `src/vcf_pg_loader/audit/integrity.py`
- `src/vcf_pg_loader/db/schema/audit_triggers.sql`

---

## Phase 2: Authentication & Access Control (Weeks 4-5)

### Chunk 2.1: User Authentication System

**Goal:** Implement user authentication with unique identifiers

**Files to create/modify:**
- `src/vcf_pg_loader/auth/users.py`
- `src/vcf_pg_loader/auth/authentication.py`
- `src/vcf_pg_loader/db/schema/users_tables.sql`
- `src/vcf_pg_loader/cli/auth_commands.py`

---

### Chunk 2.2: Role-Based Access Control

**Goal:** Implement RBAC with minimum necessary access

**Files to create/modify:**
- `src/vcf_pg_loader/auth/roles.py`
- `src/vcf_pg_loader/auth/permissions.py`
- `src/vcf_pg_loader/db/schema/rbac_tables.sql`

---

### Chunk 2.3: Session Management

**Goal:** Implement session tracking with automatic timeout

**Files to create/modify:**
- `src/vcf_pg_loader/auth/sessions.py`
- `src/vcf_pg_loader/middleware/session_middleware.py`

---

## Phase 3: De-identification & PHI Protection (Weeks 6-7)

### Chunk 3.1: Sample ID Anonymization

**Goal:** Replace identifiable sample IDs with cryptographic UUIDs

**Files to create/modify:**
- `src/vcf_pg_loader/phi/anonymizer.py`
- `src/vcf_pg_loader/phi/id_mapping.py`
- `src/vcf_pg_loader/db/schema/id_mapping_tables.sql`

---

### Chunk 3.2: VCF Header Sanitization

**Goal:** Remove PHI from VCF headers before storage

**Files to create/modify:**
- `src/vcf_pg_loader/phi/header_sanitizer.py`
- `src/vcf_pg_loader/parser/vcf_parser.py` (modify)

---

### Chunk 3.3: PHI Detection & Alerting

**Goal:** Scan for potential PHI leakage in metadata

**Files to create/modify:**
- `src/vcf_pg_loader/phi/detector.py`
- `src/vcf_pg_loader/phi/patterns.py`

---

## Phase 4: Encryption & Data Protection (Week 8)

### Chunk 4.1: Encryption at Rest

**Goal:** Implement transparent data encryption for PostgreSQL

**Files to create/modify:**
- `docker/postgres/encryption.conf`
- `src/vcf_pg_loader/db/encryption.py`
- `docs/security/encryption.md`

---

### Chunk 4.2: Secure Data Disposal

**Goal:** Implement secure deletion with verification

**Files to create/modify:**
- `src/vcf_pg_loader/data/disposal.py`
- `src/vcf_pg_loader/cli/data_commands.py` (modify)

---

## Phase 5: Documentation & Compliance (Weeks 9-10)

### Chunk 5.1: Security Documentation

**Goal:** Create required HIPAA documentation templates

**Files to create:**
- `docs/compliance/security-policies.md`
- `docs/compliance/risk-assessment-template.md`
- `docs/compliance/incident-response-plan.md`
- `docs/compliance/baa-template.md`

---

### Chunk 5.2: Docker Security Hardening

**Goal:** Harden container security for HIPAA compliance

**Files to create/modify:**
- `Dockerfile` (modify)
- `docker-compose.yml` (modify)
- `docker/security/` (new directory)
- `docs/deployment/secure-deployment.md`

---

### Chunk 5.3: Compliance Validation CLI

**Goal:** Add CLI commands to verify HIPAA compliance status

**Files to create/modify:**
- `src/vcf_pg_loader/cli/compliance_commands.py`
- `src/vcf_pg_loader/compliance/validator.py`
- `src/vcf_pg_loader/compliance/checks.py`

---

# Claude Code Prompts

## Prompt 0.1: Secure Configuration Management

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader (https://github.com/Zacharyr41/vcf-pg-loader). 
The tool currently stores database credentials in TOML config files, which violates HIPAA 164.312(d).

## Current State
- Config is in `src/vcf_pg_loader/config/` 
- Database URL passed via --db flag or config file
- No secrets management

## Task
Implement secure credential management that:

1. **Create `src/vcf_pg_loader/config/secrets.py`**:
   - Support multiple secrets backends:
     - Environment variables (default)
     - AWS Secrets Manager (optional)
     - HashiCorp Vault (optional)
   - Abstract SecretProvider base class
   - EnvSecretProvider implementation
   - Never log or print secret values
   - Mask secrets in error messages

2. **Modify `src/vcf_pg_loader/config/settings.py`**:
   - Remove any hardcoded credentials
   - Load DB credentials via SecretProvider
   - Validate that secrets are not in config files
   - Add warning if credentials detected in TOML

3. **Create `.env.example`**:
   - Document all required environment variables
   - Include VCF_PG_LOADER_DB_PASSWORD, etc.
   - Add comments explaining each variable

4. **Update CLI**:
   - Remove --db flag's ability to include password in URL
   - Add --db-password-env flag to specify env var name
   - Default to VCF_PG_LOADER_DB_PASSWORD

5. **Add validation**:
   - Raise error if password in connection string
   - Validate password is provided via secure method
   - Log (without secret) which method was used

## Requirements
- Python 3.10+ compatible
- Type hints throughout
- Unit tests in tests/config/test_secrets.py
- Handle missing secrets gracefully with clear error messages
```

---

## Prompt 0.2: TLS Enforcement for PostgreSQL

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. HIPAA 164.312(e)(1) requires 
encryption of ePHI in transit. Currently, the tool doesn't enforce TLS for PostgreSQL connections.

## Current State
- Uses asyncpg for database connections
- Docker Compose includes PostgreSQL service
- No TLS configuration

## Task
Enforce TLS 1.2+ for all database connections:

1. **Modify `src/vcf_pg_loader/db/connection.py`**:
   - Create SSLContext requiring TLS 1.2 minimum
   - Add ssl parameter to asyncpg.create_pool()
   - Support client certificate authentication (optional)
   - Add connection validation to verify TLS is active
   - Refuse to connect if TLS negotiation fails
   - Log TLS version and cipher used (at DEBUG level)

2. **Create PostgreSQL TLS configuration**:
   
   `docker/postgres/conf.d/ssl.conf`:
   ```
   ssl = on
   ssl_min_protocol_version = 'TLSv1.2'
   ssl_cert_file = '/var/lib/postgresql/certs/server.crt'
   ssl_key_file = '/var/lib/postgresql/certs/server.key'
   ssl_ciphers = 'HIGH:MEDIUM:+3DES:!aNULL'
   ```
   
   `docker/postgres/pg_hba.conf`:
   - Only allow hostssl connections
   - Reject plain host connections

3. **Create certificate generation script**:
   
   `scripts/generate-certs.sh`:
   - Generate self-signed CA for development
   - Generate server certificate
   - Generate optional client certificate
   - Set appropriate permissions (600)
   - Output to docker/certs/

4. **Update `docker-compose.yml`**:
   - Mount certificates volume
   - Mount custom pg_hba.conf
   - Add healthcheck verifying TLS

5. **Add CLI validation**:
   - `vcf-pg-loader doctor` should verify TLS capability
   - Show warning if connecting without TLS
   - Add --require-tls flag (default: true)

## Requirements
- asyncpg SSL context configuration
- TLS 1.2 minimum (TLS 1.3 preferred)
- Clear error messages for TLS failures
- Integration test verifying TLS enforcement
- Document in docs/security/tls-setup.md
```

---

## Prompt 1.1: Comprehensive Audit Schema

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. HIPAA 164.312(b) requires 
audit controls recording all ePHI access. Current audit trail only tracks load batches.

## Current State
- `variant_load_audit` table exists for batch tracking
- No user-level access logging
- No query/read access logging

## Task
Create comprehensive HIPAA-compliant audit logging schema:

1. **Create `src/vcf_pg_loader/db/schema/audit_tables.sql`**:

   ```sql
   -- Audit event types enum
   CREATE TYPE audit_event_type AS ENUM (
       'AUTH_LOGIN', 'AUTH_LOGOUT', 'AUTH_FAILED',
       'DATA_READ', 'DATA_WRITE', 'DATA_DELETE', 'DATA_EXPORT',
       'SCHEMA_CHANGE', 'CONFIG_CHANGE', 'PERMISSION_CHANGE',
       'PHI_ACCESS', 'EMERGENCY_ACCESS'
   );
   
   -- Main audit log table
   CREATE TABLE hipaa_audit_log (
       audit_id BIGSERIAL PRIMARY KEY,
       event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
       event_type audit_event_type NOT NULL,
       
       -- WHO
       user_id INTEGER REFERENCES users(user_id),
       user_name TEXT NOT NULL,
       session_id UUID,
       
       -- WHAT
       action TEXT NOT NULL,
       resource_type TEXT,
       resource_id TEXT,
       
       -- WHERE
       client_ip INET,
       client_hostname TEXT,
       application_name TEXT,
       
       -- OUTCOME
       success BOOLEAN NOT NULL,
       error_message TEXT,
       
       -- DETAILS (never include PHI here)
       details JSONB DEFAULT '{}',
       
       -- For search optimization
       created_date DATE GENERATED ALWAYS AS (event_time::date) STORED
   ) PARTITION BY RANGE (created_date);
   
   -- Create partitions for retention management
   -- (generate monthly partitions)
   
   -- Indexes for common queries
   CREATE INDEX idx_audit_user_time ON hipaa_audit_log (user_id, event_time DESC);
   CREATE INDEX idx_audit_event_type ON hipaa_audit_log (event_type, event_time DESC);
   CREATE INDEX idx_audit_resource ON hipaa_audit_log (resource_type, resource_id);
   ```

2. **Add immutability constraints**:
   - Trigger to prevent UPDATE/DELETE on audit_log
   - Separate privileged role for partition management only
   - Row-level security preventing user access to others' logs

3. **Create retention management**:
   - Function to create new monthly partitions
   - Function to archive partitions older than 6 years
   - NEVER delete within retention period

4. **Update `src/vcf_pg_loader/db/schema_manager.py`**:
   - Add audit schema creation to init-db
   - Create initial partitions (current + 12 months ahead)
   - Set up partition maintenance schedule

5. **Create audit query views**:
   - `v_audit_summary_by_user` - activity summary per user
   - `v_audit_phi_access` - all PHI access events
   - `v_audit_failed_auth` - failed authentication attempts

## Requirements
- Partition by date for efficient retention
- 6-year minimum retention (HIPAA requirement)
- No PHI in audit log details
- Immutable once written
- Efficient indexes for compliance queries
```

---

## Prompt 1.2: Audit Event Capture System

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. Need to capture all 
data access events and write them to the audit schema from Prompt 1.1.

## Current State
- Audit schema exists (from previous chunk)
- No event capture mechanism
- Loader doesn't track who is performing operations

## Task
Implement audit event capture for all PHI operations:

1. **Create `src/vcf_pg_loader/audit/models.py`**:
   ```python
   from dataclasses import dataclass
   from datetime import datetime
   from enum import Enum
   from typing import Optional, Any
   from uuid import UUID
   
   class AuditEventType(Enum):
       AUTH_LOGIN = "AUTH_LOGIN"
       AUTH_LOGOUT = "AUTH_LOGOUT"
       AUTH_FAILED = "AUTH_FAILED"
       DATA_READ = "DATA_READ"
       DATA_WRITE = "DATA_WRITE"
       DATA_DELETE = "DATA_DELETE"
       DATA_EXPORT = "DATA_EXPORT"
       SCHEMA_CHANGE = "SCHEMA_CHANGE"
       PHI_ACCESS = "PHI_ACCESS"
       EMERGENCY_ACCESS = "EMERGENCY_ACCESS"
   
   @dataclass
   class AuditEvent:
       event_type: AuditEventType
       user_id: Optional[int]
       user_name: str
       action: str
       success: bool
       session_id: Optional[UUID] = None
       resource_type: Optional[str] = None
       resource_id: Optional[str] = None
       client_ip: Optional[str] = None
       error_message: Optional[str] = None
       details: dict = field(default_factory=dict)
       
       def sanitize_details(self) -> dict:
           """Remove any potential PHI from details"""
           # Implementation
   ```

2. **Create `src/vcf_pg_loader/audit/logger.py`**:
   - AuditLogger class with async write capability
   - Batch writes for performance (flush every N events or M seconds)
   - Guaranteed delivery (local buffer if DB unavailable)
   - Context manager for operation tracking
   
   ```python
   class AuditLogger:
       async def log_event(self, event: AuditEvent) -> None: ...
       
       @asynccontextmanager
       async def audit_operation(
           self, 
           event_type: AuditEventType,
           action: str,
           resource_type: str = None,
           resource_id: str = None
       ):
           """Context manager that logs start/end of operation"""
           ...
   ```

3. **Create `src/vcf_pg_loader/audit/context.py`**:
   - Thread-local/context-var for current user context
   - Automatic population of user info in events
   - Request context for CLI operations

4. **Integrate with loader**:
   - Modify `VCFLoader.load_vcf()` to audit:
     - Load start (DATA_WRITE)
     - Load completion/failure
     - Variant counts (in details, no PHI)
   - Audit all database queries that touch variant data

5. **Create decorators for easy auditing**:
   ```python
   @audit_operation(AuditEventType.DATA_READ, "query_variants")
   async def query_variants(self, region: str) -> list:
       ...
   ```

## Requirements
- Async-compatible for asyncpg
- Never blocks main operation on audit write
- Falls back to local file if DB unavailable
- Sanitizes all details to prevent PHI leakage
- Unit tests with mock database
```

---

## Prompt 1.3: Audit Log Immutability & Integrity

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. Audit logs must be 
tamper-proof to meet HIPAA requirements for reliable audit trails.

## Current State
- Audit tables exist
- Events are being captured
- No protection against modification

## Task
Ensure audit logs cannot be modified after creation:

1. **Create `src/vcf_pg_loader/db/schema/audit_triggers.sql`**:
   ```sql
   -- Prevent any modifications to audit records
   CREATE OR REPLACE FUNCTION prevent_audit_modification()
   RETURNS TRIGGER AS $$
   BEGIN
       RAISE EXCEPTION 'Audit log records cannot be modified or deleted. '
           'This is a HIPAA compliance requirement.';
       RETURN NULL;
   END;
   $$ LANGUAGE plpgsql SECURITY DEFINER;
   
   CREATE TRIGGER audit_immutability
   BEFORE UPDATE OR DELETE ON hipaa_audit_log
   FOR EACH ROW
   EXECUTE FUNCTION prevent_audit_modification();
   
   -- Prevent TRUNCATE
   CREATE OR REPLACE FUNCTION prevent_audit_truncate()
   RETURNS event_trigger AS $$
   BEGIN
       IF EXISTS (
           SELECT 1 FROM pg_event_trigger_dropped_objects()
           WHERE object_name = 'hipaa_audit_log'
       ) THEN
           RAISE EXCEPTION 'Cannot truncate or drop audit log tables';
       END IF;
   END;
   $$ LANGUAGE plpgsql;
   
   CREATE EVENT TRIGGER protect_audit_tables
   ON sql_drop
   EXECUTE FUNCTION prevent_audit_truncate();
   ```

2. **Create `src/vcf_pg_loader/audit/integrity.py`**:
   - Hash chain for audit entries (each entry includes hash of previous)
   - Periodic integrity verification function
   - Alert mechanism for detected tampering
   
   ```python
   class AuditIntegrity:
       def compute_entry_hash(self, entry: AuditEvent, previous_hash: str) -> str:
           """Compute SHA-256 hash including previous entry's hash"""
           ...
       
       async def verify_chain_integrity(
           self, 
           start_date: date, 
           end_date: date
       ) -> IntegrityReport:
           """Verify hash chain for date range"""
           ...
       
       async def get_last_verified_hash(self) -> str:
           """Get hash of last verified entry for chain continuation"""
           ...
   ```

3. **Add integrity verification CLI**:
   ```
   vcf-pg-loader audit verify --start-date 2024-01-01 --end-date 2024-12-31
   ```
   - Reports any chain breaks
   - Shows verification coverage
   - Exits non-zero if tampering detected

4. **Implement backup verification**:
   - Export audit logs with integrity metadata
   - Verify imported audit logs match hashes
   - Document secure backup procedures

5. **Add row-level security**:
   - Users can only query their own audit entries
   - Auditors can query all entries (special role)
   - Nobody can modify (enforced by triggers)

## Requirements
- SHA-256 for hash chain
- Verification should be efficient (index on hash column)
- Clear reporting of any integrity violations
- Integration test that attempts modification and verifies prevention
```

---

## Prompt 2.1: User Authentication System

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. HIPAA 164.312(d) requires 
unique user identification. Currently, the tool has no authentication.

## Current State
- No user accounts
- No authentication
- Operations are anonymous

## Task
Implement user authentication with unique identifiers:

1. **Create `src/vcf_pg_loader/db/schema/users_tables.sql`**:
   ```sql
   CREATE TABLE users (
       user_id SERIAL PRIMARY KEY,
       username VARCHAR(100) UNIQUE NOT NULL,
       email VARCHAR(255) UNIQUE,
       password_hash TEXT NOT NULL,
       
       -- Status
       is_active BOOLEAN DEFAULT true,
       is_locked BOOLEAN DEFAULT false,
       failed_login_attempts INTEGER DEFAULT 0,
       locked_until TIMESTAMPTZ,
       
       -- Password policy
       password_changed_at TIMESTAMPTZ DEFAULT NOW(),
       password_expires_at TIMESTAMPTZ,
       must_change_password BOOLEAN DEFAULT false,
       
       -- Audit
       created_at TIMESTAMPTZ DEFAULT NOW(),
       created_by INTEGER REFERENCES users(user_id),
       last_login_at TIMESTAMPTZ,
       
       -- MFA (optional)
       mfa_enabled BOOLEAN DEFAULT false,
       mfa_secret TEXT
   );
   
   CREATE TABLE password_history (
       id SERIAL PRIMARY KEY,
       user_id INTEGER REFERENCES users(user_id),
       password_hash TEXT NOT NULL,
       created_at TIMESTAMPTZ DEFAULT NOW()
   );
   ```

2. **Create `src/vcf_pg_loader/auth/authentication.py`**:
   ```python
   class Authenticator:
       # Password requirements (NIST 800-63B aligned)
       MIN_PASSWORD_LENGTH = 12
       REQUIRE_COMPLEXITY = False  # NIST recommends against
       CHECK_BREACHED_PASSWORDS = True  # Check against HaveIBeenPwned
       
       async def authenticate(
           self, 
           username: str, 
           password: str,
           client_ip: str
       ) -> AuthResult:
           """Authenticate user and return session token"""
           ...
       
       async def create_user(
           self, 
           username: str,
           email: str,
           password: str,
           created_by: int
       ) -> User:
           """Create new user with hashed password"""
           ...
       
       def hash_password(self, password: str) -> str:
           """Hash password using Argon2id"""
           ...
       
       def verify_password(self, password: str, hash: str) -> bool:
           """Verify password against hash"""
           ...
   ```

3. **Create `src/vcf_pg_loader/auth/users.py`**:
   - User model/dataclass
   - User CRUD operations
   - Password policy enforcement
   - Account lockout after N failed attempts

4. **Create `src/vcf_pg_loader/cli/auth_commands.py`**:
   ```
   vcf-pg-loader auth login
   vcf-pg-loader auth logout  
   vcf-pg-loader auth whoami
   vcf-pg-loader auth create-user --username <name> --email <email>
   vcf-pg-loader auth change-password
   vcf-pg-loader auth reset-password --username <name>
   vcf-pg-loader auth list-users
   vcf-pg-loader auth disable-user --username <name>
   ```

5. **Integrate with existing commands**:
   - All data commands require authentication
   - Store session token in secure location (~/.vcf-pg-loader/session)
   - Token expiration (configurable, default 8 hours)
   - Prompt for login if not authenticated

## Requirements
- Argon2id for password hashing
- No plaintext password storage anywhere
- Account lockout after 5 failed attempts
- Password history (prevent reuse of last 12)
- All auth events logged to audit system
```

---

## Prompt 2.2: Role-Based Access Control

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. HIPAA 164.312(a)(1) requires 
access controls limiting ePHI access to authorized users. Need RBAC with minimum 
necessary access principle.

## Current State
- User authentication exists (from previous chunk)
- No authorization/permission system
- All authenticated users have full access

## Task
Implement RBAC with granular permissions:

1. **Create `src/vcf_pg_loader/db/schema/rbac_tables.sql`**:
   ```sql
   -- Predefined roles
   CREATE TABLE roles (
       role_id SERIAL PRIMARY KEY,
       role_name VARCHAR(50) UNIQUE NOT NULL,
       description TEXT,
       is_system_role BOOLEAN DEFAULT false,
       created_at TIMESTAMPTZ DEFAULT NOW()
   );
   
   -- Granular permissions
   CREATE TABLE permissions (
       permission_id SERIAL PRIMARY KEY,
       permission_name VARCHAR(100) UNIQUE NOT NULL,
       resource_type VARCHAR(50) NOT NULL,  -- 'variant', 'sample', 'audit', 'user'
       action VARCHAR(20) NOT NULL,  -- 'read', 'write', 'delete', 'admin'
       description TEXT
   );
   
   -- Role-permission mapping
   CREATE TABLE role_permissions (
       role_id INTEGER REFERENCES roles(role_id),
       permission_id INTEGER REFERENCES permissions(permission_id),
       PRIMARY KEY (role_id, permission_id)
   );
   
   -- User-role mapping
   CREATE TABLE user_roles (
       user_id INTEGER REFERENCES users(user_id),
       role_id INTEGER REFERENCES roles(role_id),
       granted_by INTEGER REFERENCES users(user_id),
       granted_at TIMESTAMPTZ DEFAULT NOW(),
       expires_at TIMESTAMPTZ,
       PRIMARY KEY (user_id, role_id)
   );
   
   -- Insert default roles
   INSERT INTO roles (role_name, description, is_system_role) VALUES
       ('admin', 'Full system access', true),
       ('data_loader', 'Can load VCF files', true),
       ('data_reader', 'Can query variant data', true),
       ('auditor', 'Can view audit logs', true),
       ('user_manager', 'Can manage user accounts', true);
   
   -- Insert default permissions
   INSERT INTO permissions (permission_name, resource_type, action) VALUES
       ('variants:read', 'variant', 'read'),
       ('variants:write', 'variant', 'write'),
       ('variants:delete', 'variant', 'delete'),
       ('samples:read', 'sample', 'read'),
       ('audit:read', 'audit', 'read'),
       ('users:read', 'user', 'read'),
       ('users:write', 'user', 'write'),
       ('users:admin', 'user', 'admin');
   ```

2. **Create `src/vcf_pg_loader/auth/roles.py`**:
   ```python
   class RoleManager:
       async def assign_role(
           self, 
           user_id: int, 
           role_name: str,
           granted_by: int,
           expires_at: datetime = None
       ) -> None: ...
       
       async def revoke_role(self, user_id: int, role_name: str) -> None: ...
       
       async def get_user_roles(self, user_id: int) -> list[Role]: ...
       
       async def get_role_users(self, role_name: str) -> list[User]: ...
   ```

3. **Create `src/vcf_pg_loader/auth/permissions.py`**:
   ```python
   class PermissionChecker:
       async def has_permission(
           self, 
           user_id: int, 
           permission: str
       ) -> bool: ...
       
       async def get_user_permissions(self, user_id: int) -> set[str]: ...
       
       def require_permission(self, permission: str):
           """Decorator to enforce permission on function"""
           ...
   ```

4. **Add CLI commands**:
   ```
   vcf-pg-loader roles list
   vcf-pg-loader roles assign --user <username> --role <role>
   vcf-pg-loader roles revoke --user <username> --role <role>
   vcf-pg-loader roles show --user <username>
   vcf-pg-loader permissions list
   vcf-pg-loader permissions check --user <username> --permission <perm>
   ```

5. **Integrate with existing commands**:
   - `load` command requires `variants:write`
   - `query` commands require `variants:read`
   - `validate` requires `variants:read`
   - User management requires `users:admin`
   - Audit access requires `audit:read`

## Requirements
- Default deny (no permission = no access)
- Permission inheritance through roles
- Time-limited role assignments supported
- All permission changes logged to audit
- Caching for performance (invalidate on changes)
```

---

## Prompt 2.3: Session Management

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. HIPAA 164.312(a)(2)(iii) 
requires automatic logoff after inactivity. Need proper session management.

## Current State
- Users can authenticate
- Session token stored locally
- No timeout or session tracking

## Task
Implement comprehensive session management:

1. **Create `src/vcf_pg_loader/db/schema/sessions_tables.sql`**:
   ```sql
   CREATE TABLE user_sessions (
       session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       user_id INTEGER REFERENCES users(user_id) NOT NULL,
       
       -- Session metadata
       created_at TIMESTAMPTZ DEFAULT NOW(),
       last_activity_at TIMESTAMPTZ DEFAULT NOW(),
       expires_at TIMESTAMPTZ NOT NULL,
       
       -- Client info
       client_ip INET,
       user_agent TEXT,
       
       -- Status
       is_active BOOLEAN DEFAULT true,
       terminated_reason VARCHAR(50),  -- 'logout', 'timeout', 'admin', 'password_change'
       terminated_at TIMESTAMPTZ
   );
   
   CREATE INDEX idx_sessions_user ON user_sessions (user_id, is_active);
   CREATE INDEX idx_sessions_expires ON user_sessions (expires_at) WHERE is_active;
   ```

2. **Create `src/vcf_pg_loader/auth/sessions.py`**:
   ```python
   @dataclass
   class SessionConfig:
       # HIPAA-compliant defaults
       session_timeout_minutes: int = 30
       absolute_timeout_hours: int = 8
       max_concurrent_sessions: int = 3
       extend_on_activity: bool = True
   
   class SessionManager:
       async def create_session(
           self,
           user_id: int,
           client_ip: str,
           user_agent: str
       ) -> Session: ...
       
       async def validate_session(self, session_id: UUID) -> Session | None:
           """Validate and optionally extend session"""
           ...
       
       async def terminate_session(
           self, 
           session_id: UUID, 
           reason: str
       ) -> None: ...
       
       async def terminate_user_sessions(
           self,
           user_id: int,
           reason: str
       ) -> int:
           """Terminate all sessions for user (e.g., on password change)"""
           ...
       
       async def cleanup_expired_sessions(self) -> int:
           """Background job to clean up expired sessions"""
           ...
   ```

3. **Create session storage for CLI**:
   - Store session token in `~/.vcf-pg-loader/session`
   - File permissions: 600 (user read/write only)
   - Include expiration timestamp
   - Auto-clear on logout or expiration

4. **Add session CLI commands**:
   ```
   vcf-pg-loader session status     # Show current session info
   vcf-pg-loader session list       # List active sessions (admin)
   vcf-pg-loader session terminate  # Terminate specific session (admin)
   vcf-pg-loader session terminate-all --user <username>  # Terminate all for user
   ```

5. **Integrate with all commands**:
   - Check session validity before each operation
   - Update last_activity_at on successful operations
   - Clear local session file if server session invalid
   - Handle concurrent session limit gracefully

6. **Add background session cleanup**:
   - Optional background process/cron job
   - Clean up expired sessions
   - Log session timeouts to audit

## Requirements
- Configurable timeout (default 30 minutes)
- Extend session on activity (configurable)
- Maximum concurrent sessions per user
- Terminate all sessions on password change
- Session activity logged to audit
```

---

## Prompt 3.1: Sample ID Anonymization

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. VCF sample IDs may contain 
identifiable information (e.g., "JohnDoe_2024", hospital MRNs). Need to replace 
with anonymous identifiers while maintaining linkage capability.

## Current State
- VCF sample names stored directly in database
- No de-identification
- Sample IDs visible in queries

## Task
Implement sample ID anonymization with secure mapping:

1. **Create `src/vcf_pg_loader/db/schema/id_mapping_tables.sql`**:
   ```sql
   -- Separate schema for PHI mapping (can be in different database)
   CREATE SCHEMA IF NOT EXISTS phi_vault;
   
   -- Sample ID mapping table
   CREATE TABLE phi_vault.sample_id_mapping (
       mapping_id SERIAL PRIMARY KEY,
       anonymous_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
       original_id TEXT NOT NULL,
       
       -- Source tracking
       source_file TEXT NOT NULL,
       load_batch_id UUID NOT NULL,
       
       -- Encryption of original (for additional protection)
       original_id_encrypted BYTEA,  -- AES-256 encrypted
       
       -- Audit
       created_at TIMESTAMPTZ DEFAULT NOW(),
       created_by INTEGER,
       
       -- Access control
       UNIQUE (original_id, source_file)
   );
   
   -- Restrict access to PHI vault
   REVOKE ALL ON SCHEMA phi_vault FROM PUBLIC;
   GRANT USAGE ON SCHEMA phi_vault TO phi_admin;
   ```

2. **Create `src/vcf_pg_loader/phi/anonymizer.py`**:
   ```python
   class SampleAnonymizer:
       def __init__(self, encryption_key: bytes = None):
           self.encryption_key = encryption_key
       
       async def anonymize_sample_id(
           self,
           original_id: str,
           source_file: str,
           load_batch_id: UUID
       ) -> UUID:
           """
           Get or create anonymous ID for sample.
           Returns existing anonymous ID if sample was seen before.
           """
           ...
       
       async def bulk_anonymize(
           self,
           sample_ids: list[str],
           source_file: str,
           load_batch_id: UUID
       ) -> dict[str, UUID]:
           """Anonymize multiple sample IDs efficiently"""
           ...
       
       async def reverse_lookup(
           self,
           anonymous_id: UUID,
           requester_id: int
       ) -> str | None:
           """
           Reverse lookup - requires special permission.
           All lookups are logged to audit.
           """
           ...
   ```

3. **Create `src/vcf_pg_loader/phi/id_mapping.py`**:
   - Secure storage of mappings
   - Optional encryption of original IDs
   - Consistent anonymous ID for same sample across files
   - Export mapping for authorized users only

4. **Integrate with VCF loading**:
   - Modify parser to anonymize sample IDs during load
   - Store anonymous IDs in variants/genotypes tables
   - Keep mapping table separate (different access controls)
   - Add `--anonymize/--no-anonymize` flag (default: anonymize)

5. **Add CLI commands**:
   ```
   vcf-pg-loader phi lookup --anonymous-id <uuid>  # Reverse lookup (audited)
   vcf-pg-loader phi export-mapping --batch-id <uuid>  # Export mapping file
   vcf-pg-loader phi stats  # Show anonymization statistics
   ```

6. **Add re-identification warning**:
   - Log warning that genomic data may still be re-identifiable
   - Recommend Data Use Agreements for any data sharing
   - Reference HIPAA Expert Determination requirement

## Requirements
- Deterministic mapping (same input = same output)
- Optional encryption of original IDs (AES-256-GCM)
- All reverse lookups logged to audit
- Separate permissions for mapping access
- Integration tests with realistic sample ID patterns
```

---

## Prompt 3.2: VCF Header Sanitization

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. VCF headers often contain 
PHI in metadata fields (CommandLine paths, SAMPLE fields with demographics, 
institution names, processing dates that could identify patients).

## Current State
- VCF headers parsed but not sanitized
- SAMPLE metadata stored as-is
- CommandLine fields may contain file paths with patient info

## Task
Implement VCF header sanitization to remove PHI:

1. **Create `src/vcf_pg_loader/phi/header_sanitizer.py`**:
   ```python
   @dataclass
   class SanitizationConfig:
       # What to sanitize
       remove_commandline: bool = True
       remove_sample_metadata: bool = True
       remove_dates: bool = True
       remove_file_paths: bool = True
       remove_institution_patterns: bool = True
       
       # Custom patterns to remove
       custom_patterns: list[str] = field(default_factory=list)
       
       # What to preserve (override removals)
       preserve_fields: set[str] = field(default_factory=set)
   
   class VCFHeaderSanitizer:
       # Patterns that commonly contain PHI
       PHI_PATTERNS = [
           r'(?i)patient[_\s]?id',
           r'(?i)mrn[_\s]?',
           r'(?i)ssn[_\s]?',
           r'(?i)dob[_\s]?',
           r'(?i)birth[_\s]?date',
           r'/home/\w+/',  # Home directory paths
           r'/Users/\w+/',  # macOS home paths
           r'[A-Z]:\\Users\\\w+\\',  # Windows paths
           r'\d{3}-\d{2}-\d{4}',  # SSN pattern
           r'\d{1,2}/\d{1,2}/\d{2,4}',  # Date patterns
       ]
       
       def sanitize_header(self, vcf_header: str) -> SanitizedHeader:
           """Sanitize VCF header, returning cleaned version and removed items"""
           ...
       
       def sanitize_info_line(self, line: str) -> str:
           """Sanitize single ##INFO or ##FORMAT line"""
           ...
       
       def sanitize_sample_metadata(self, metadata: dict) -> dict:
           """Remove PHI from SAMPLE metadata"""
           ...
       
       def get_sanitization_report(self) -> SanitizationReport:
           """Report what was sanitized (for audit)"""
           ...
   ```

2. **Create pattern configuration file**:
   `config/phi_patterns.yaml`:
   ```yaml
   # Institution-specific patterns
   institutions:
     - "Mayo Clinic"
     - "Johns Hopkins"
     - "MGH"
     - "UCSF"
   
   # Path patterns
   file_paths:
     - "/data/patients/"
     - "/clinical/"
     - "/PHI/"
   
   # Field names to always remove
   remove_fields:
     - "PatientID"
     - "MRN"
     - "DOB"
     - "SSN"
   
   # Field names to preserve (even if matched)
   preserve_fields:
     - "reference"
     - "assembly"
   ```

3. **Modify `src/vcf_pg_loader/parser/vcf_parser.py`**:
   - Apply sanitization during parsing
   - Store sanitization report in audit log
   - Option to output sanitized VCF for verification

4. **Add pre-load scanning**:
   ```python
   class PHIScanner:
       def scan_vcf_for_phi(self, vcf_path: Path) -> PHIScanResult:
           """Scan VCF for potential PHI before loading"""
           ...
   ```

5. **Add CLI commands**:
   ```
   vcf-pg-loader phi scan <vcf_path>  # Scan for potential PHI
   vcf-pg-loader phi sanitize <vcf_path> --output <output_path>  # Sanitize VCF file
   vcf-pg-loader phi report --batch-id <uuid>  # Show sanitization report for load
   ```

6. **Add load-time options**:
   ```
   vcf-pg-loader load sample.vcf.gz --sanitize-headers  # Default: true
   vcf-pg-loader load sample.vcf.gz --phi-scan  # Scan before load
   vcf-pg-loader load sample.vcf.gz --fail-on-phi  # Fail if PHI detected
   ```

## Requirements
- Configurable patterns (institution-specific additions)
- Audit log of what was sanitized
- Preview mode to see what would be removed
- Does not modify original VCF file
- Integration tests with synthetic PHI examples
```

---

## Prompt 3.3: PHI Detection & Alerting

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. Need proactive PHI detection 
to alert when potential PHI is found in data being loaded.

## Current State
- Header sanitization exists
- No detection in INFO/FORMAT fields
- No alerting mechanism

## Task
Implement comprehensive PHI detection with alerting:

1. **Create `src/vcf_pg_loader/phi/patterns.py`**:
   ```python
   @dataclass
   class PHIPattern:
       name: str
       pattern: re.Pattern
       severity: str  # 'critical', 'high', 'medium', 'low'
       description: str
       false_positive_hints: list[str]
   
   class PHIPatternRegistry:
       BUILTIN_PATTERNS = [
           PHIPattern(
               name="ssn",
               pattern=re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
               severity="critical",
               description="Social Security Number pattern",
               false_positive_hints=["May be gene coordinates"]
           ),
           PHIPattern(
               name="mrn",
               pattern=re.compile(r'\b(MRN|mrn)[:\s]?\d+\b'),
               severity="critical", 
               description="Medical Record Number",
               false_positive_hints=[]
           ),
           PHIPattern(
               name="email",
               pattern=re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
               severity="high",
               description="Email address",
               false_positive_hints=["May be tool contact info"]
           ),
           # ... more patterns
       ]
       
       def load_custom_patterns(self, config_path: Path) -> None: ...
       def add_pattern(self, pattern: PHIPattern) -> None: ...
       def get_patterns_by_severity(self, severity: str) -> list[PHIPattern]: ...
   ```

2. **Create `src/vcf_pg_loader/phi/detector.py`**:
   ```python
   @dataclass
   class PHIDetection:
       pattern_name: str
       matched_value: str  # Partially masked
       location: str  # Where in VCF (header, INFO field, etc.)
       context: str  # Surrounding text for review
       severity: str
   
   class PHIDetector:
       def __init__(self, patterns: PHIPatternRegistry):
           self.patterns = patterns
           self.detections: list[PHIDetection] = []
       
       def scan_value(self, value: str, location: str) -> list[PHIDetection]: ...
       
       def scan_vcf_record(self, record) -> list[PHIDetection]:
           """Scan INFO and FORMAT fields of VCF record"""
           ...
       
       def scan_vcf_stream(
           self, 
           vcf_path: Path,
           sample_size: int = 1000
       ) -> PHIScanReport:
           """Scan sample of VCF for PHI (for large files)"""
           ...
       
       def mask_phi(self, value: str, detection: PHIDetection) -> str:
           """Mask detected PHI for logging/display"""
           ...
   ```

3. **Create alerting system**:
   ```python
   class PHIAlertHandler:
       async def handle_detection(
           self, 
           detection: PHIDetection,
           context: LoadContext
       ) -> AlertAction:
           """Handle PHI detection based on severity and config"""
           ...
   
   class AlertAction(Enum):
       LOG_ONLY = "log_only"
       WARN_AND_CONTINUE = "warn_and_continue"  
       PAUSE_FOR_REVIEW = "pause_for_review"
       ABORT_LOAD = "abort_load"
   ```

4. **Integrate with loader**:
   - Scan configurable sample of records during load
   - Configurable action on detection:
     - Log and continue (default for low severity)
     - Warn and continue (medium severity)
     - Pause for confirmation (high severity)
     - Abort load (critical severity)
   
5. **Add configuration**:
   ```toml
   [phi_detection]
   enabled = true
   sample_rate = 0.01  # Scan 1% of records
   scan_headers = true
   scan_info_fields = true
   scan_sample_ids = true
   
   [phi_detection.actions]
   critical = "abort"
   high = "pause"
   medium = "warn"
   low = "log"
   
   [phi_detection.alerts]
   email = "security@example.com"
   slack_webhook = "https://..."
   ```

6. **Add CLI commands**:
   ```
   vcf-pg-loader phi detect <vcf_path>  # Full PHI detection scan
   vcf-pg-loader phi patterns list  # List detection patterns
   vcf-pg-loader phi patterns add --name <name> --pattern <regex> --severity <level>
   vcf-pg-loader phi patterns test --pattern <regex> --input <text>  # Test pattern
   ```

## Requirements
- Configurable pattern library
- False positive documentation
- Masked output (don't log actual PHI)
- Sampling for large files
- Webhook/email alerting
- Integration with audit logging
```

---

## Prompt 4.1: Encryption at Rest

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. HIPAA 164.312(a)(2)(iv) 
addresses encryption. While addressable (not required), encryption at rest 
is strongly recommended and expected for clinical data.

## Current State  
- PostgreSQL data stored unencrypted
- Docker volumes not encrypted
- No encryption configuration

## Task
Implement encryption at rest for PostgreSQL:

1. **Document encryption options**:
   Create `docs/security/encryption-at-rest.md` covering:
   - PostgreSQL TDE (Transparent Data Encryption) - Enterprise feature
   - Filesystem-level encryption (LUKS, dm-crypt)
   - Cloud provider encryption (AWS RDS, GCP Cloud SQL, Azure)
   - Application-level encryption (column-level)

2. **Implement column-level encryption for PHI mapping**:
   ```python
   # src/vcf_pg_loader/phi/encryption.py
   from cryptography.fernet import Fernet
   from cryptography.hazmat.primitives.ciphers.aead import AESGCM
   
   class ColumnEncryption:
       def __init__(self, key: bytes):
           """Initialize with 256-bit AES key"""
           self.aesgcm = AESGCM(key)
       
       def encrypt(self, plaintext: str, associated_data: bytes = b"") -> bytes:
           """Encrypt with AES-256-GCM"""
           nonce = os.urandom(12)
           ciphertext = self.aesgcm.encrypt(nonce, plaintext.encode(), associated_data)
           return nonce + ciphertext
       
       def decrypt(self, ciphertext: bytes, associated_data: bytes = b"") -> str:
           """Decrypt AES-256-GCM"""
           nonce = ciphertext[:12]
           return self.aesgcm.decrypt(nonce, ciphertext[12:], associated_data).decode()
   ```

3. **Apply to PHI mapping table**:
   - Encrypt `original_id` in sample_id_mapping table
   - Store encrypted form, decrypt only on authorized access
   - Key rotation support

4. **Create Docker volume encryption guide**:
   ```bash
   # scripts/setup-encrypted-volume.sh
   # Create encrypted volume for PostgreSQL data
   # Using dm-crypt/LUKS
   ```

5. **Add encryption key management**:
   ```python
   class KeyManager:
       def get_encryption_key(self, key_id: str) -> bytes:
           """Get key from environment, file, or KMS"""
           ...
       
       def rotate_key(self, old_key_id: str, new_key_id: str) -> None:
           """Rotate encryption key for all encrypted data"""
           ...
   ```

6. **Cloud deployment guides**:
   - `docs/deployment/aws-rds-encryption.md`
   - `docs/deployment/gcp-cloudsql-encryption.md`  
   - `docs/deployment/azure-encryption.md`

7. **Add CLI verification**:
   ```
   vcf-pg-loader security check-encryption  # Verify encryption status
   vcf-pg-loader security rotate-key --old-key <id> --new-key <id>
   ```

## Requirements
- AES-256-GCM for column encryption
- Support for key rotation without downtime
- Document all encryption options with pros/cons
- Clear key management procedures
- Integration with cloud KMS (AWS KMS, GCP KMS, Azure Key Vault)
```

---

## Prompt 4.2: Secure Data Disposal

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. HIPAA requires proper 
disposal of PHI when no longer needed. Need secure deletion capabilities.

## Current State
- No data deletion commands
- Standard SQL DELETE (leaves data recoverable)
- No disposal documentation

## Task
Implement secure data disposal with verification:

1. **Create `src/vcf_pg_loader/data/disposal.py`**:
   ```python
   class DataDisposal:
       async def dispose_batch(
           self,
           batch_id: UUID,
           reason: str,
           authorized_by: int,
           verification_required: bool = True
       ) -> DisposalResult:
           """
           Securely dispose of all data from a load batch.
           Includes variants, genotypes, and PHI mappings.
           """
           ...
       
       async def dispose_sample(
           self,
           sample_anonymous_id: UUID,
           reason: str,
           authorized_by: int
       ) -> DisposalResult:
           """Remove all data for a specific sample (e.g., patient withdrawal)"""
           ...
       
       async def verify_disposal(self, disposal_id: UUID) -> VerificationResult:
           """Verify data was properly disposed"""
           ...
       
       async def generate_disposal_certificate(
           self, 
           disposal_id: UUID
       ) -> DisposalCertificate:
           """Generate certificate of destruction for compliance"""
           ...
   ```

2. **Create disposal tracking table**:
   ```sql
   CREATE TABLE disposal_records (
       disposal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       disposal_type VARCHAR(50) NOT NULL,  -- 'batch', 'sample', 'date_range'
       target_identifier TEXT NOT NULL,  -- batch_id, sample_id, etc.
       
       -- Counts for verification
       variants_disposed BIGINT,
       genotypes_disposed BIGINT,
       mappings_disposed BIGINT,
       
       -- Authorization
       reason TEXT NOT NULL,
       authorized_by INTEGER REFERENCES users(user_id),
       authorized_at TIMESTAMPTZ DEFAULT NOW(),
       
       -- Execution
       executed_by INTEGER REFERENCES users(user_id),
       executed_at TIMESTAMPTZ,
       
       -- Verification
       verified_by INTEGER REFERENCES users(user_id),
       verified_at TIMESTAMPTZ,
       verification_result JSONB,
       
       -- Certificate
       certificate_generated_at TIMESTAMPTZ,
       certificate_hash TEXT
   );
   ```

3. **Implement secure deletion**:
   - Use PostgreSQL VACUUM FULL after DELETE (reclaims space)
   - Clear WAL entries if possible
   - Document that backups must also be purged
   - For cloud deployments, document provider-specific procedures

4. **Add CLI commands**:
   ```
   vcf-pg-loader data dispose --batch-id <uuid> --reason "Patient withdrawal"
   vcf-pg-loader data dispose --sample-id <uuid> --reason "Data retention expired"
   vcf-pg-loader data verify-disposal --disposal-id <uuid>
   vcf-pg-loader data certificate --disposal-id <uuid> --output <path>
   vcf-pg-loader data list-disposals --start-date <date> --end-date <date>
   ```

5. **Add retention policy support**:
   ```python
   class RetentionPolicy:
       async def check_expired_data(self) -> list[ExpiredData]:
           """Find data past retention period"""
           ...
       
       async def generate_expiration_report(self) -> RetentionReport:
           """Report on data approaching expiration"""
           ...
   ```

6. **Create disposal documentation**:
   - `docs/compliance/data-retention-policy.md`
   - `docs/compliance/disposal-procedures.md`
   - Certificate of destruction template

## Requirements
- Two-person authorization for disposal (configurable)
- Verification step after disposal
- Certificate of destruction generation
- Audit trail of all disposals
- Backup purging documentation
- Integration with retention policies
```

---

## Prompt 5.1: Security Documentation

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. HIPAA 164.316 requires 
documented policies and procedures. Need comprehensive security documentation.

## Task
Create required HIPAA compliance documentation:

1. **Create `docs/compliance/security-policies.md`**:
   - Access control policy
   - Authentication requirements
   - Password policy (aligned with NIST 800-63B)
   - Encryption policy
   - Audit logging policy
   - Incident response policy reference
   - Data retention policy
   - Disposal policy

2. **Create `docs/compliance/risk-assessment-template.md`**:
   ```markdown
   # Risk Assessment Template for vcf-pg-loader Deployment
   
   ## 1. System Description
   - [ ] Document system boundaries
   - [ ] Identify ePHI data flows
   - [ ] List all system components
   
   ## 2. Threat Identification
   - [ ] Natural threats
   - [ ] Human threats (intentional/unintentional)
   - [ ] Environmental threats
   
   ## 3. Vulnerability Assessment
   - [ ] Technical vulnerabilities
   - [ ] Administrative vulnerabilities
   - [ ] Physical vulnerabilities
   
   ## 4. Risk Analysis Matrix
   | Threat | Vulnerability | Likelihood | Impact | Risk Level |
   |--------|--------------|------------|--------|------------|
   
   ## 5. Risk Mitigation Plan
   ...
   ```

3. **Create `docs/compliance/incident-response-plan.md`**:
   - Incident classification (breach vs. security event)
   - Detection and reporting procedures
   - Containment procedures
   - Four-factor risk assessment template
   - Notification requirements and timelines
   - Documentation requirements
   - Post-incident review process

4. **Create `docs/compliance/baa-template.md`**:
   - Template Business Associate Agreement provisions
   - Required clauses for HIPAA compliance
   - Security requirements for business associates
   - Breach notification requirements
   - Subcontractor requirements

5. **Create `docs/compliance/deployment-checklist.md`**:
   ```markdown
   # HIPAA-Compliant Deployment Checklist
   
   ## Pre-Deployment
   - [ ] Risk assessment completed
   - [ ] Security policies documented
   - [ ] BAA signed (if applicable)
   - [ ] Encryption keys provisioned
   - [ ] TLS certificates obtained
   
   ## Infrastructure
   - [ ] Database encryption at rest enabled
   - [ ] TLS 1.2+ enforced
   - [ ] Audit logging configured
   - [ ] Backup encryption verified
   
   ## Access Control
   - [ ] User accounts created
   - [ ] Roles assigned (minimum necessary)
   - [ ] MFA enabled (if available)
   - [ ] Emergency access procedures documented
   
   ## Validation
   - [ ] `vcf-pg-loader compliance check` passes
   - [ ] Penetration testing completed
   - [ ] Security review completed
   
   ## Ongoing
   - [ ] Audit log review schedule established
   - [ ] Patch management process defined
   - [ ] Annual risk assessment scheduled
   ```

6. **Create `docs/compliance/audit-review-guide.md`**:
   - What to review in audit logs
   - Frequency of reviews
   - Indicators of potential breaches
   - Documentation of review findings

## Requirements
- Templates should be customizable
- Clear language appropriate for non-technical compliance officers
- Cross-references to relevant HIPAA sections
- Markdown format for easy version control
- Examples and sample text where appropriate
```

---

## Prompt 5.2: Docker Security Hardening

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. Container security 
per NIST SP 800-190 is necessary for HIPAA-compliant deployments.

## Current State
- Basic Dockerfile exists
- Docker Compose for development
- No security hardening

## Task
Harden Docker configuration for HIPAA compliance:

1. **Modify `Dockerfile`**:
   ```dockerfile
   # Use specific version, not latest
   FROM python:3.12-slim-bookworm AS builder
   
   # Create non-root user
   RUN groupadd -r vcfloader && useradd -r -g vcfloader vcfloader
   
   # Install dependencies in separate layer
   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt
   
   # Production stage
   FROM python:3.12-slim-bookworm
   
   # Security: Run as non-root
   RUN groupadd -r vcfloader && useradd -r -g vcfloader vcfloader
   
   # Security: Remove unnecessary packages
   RUN apt-get update && apt-get install -y --no-install-recommends \
       && rm -rf /var/lib/apt/lists/* \
       && apt-get clean
   
   # Security: Read-only filesystem support
   VOLUME ["/tmp", "/var/log"]
   
   # Copy application
   COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
   COPY --chown=vcfloader:vcfloader src/ /app/
   
   USER vcfloader
   WORKDIR /app
   
   # Security: No new privileges
   # Applied via docker run --security-opt=no-new-privileges:true
   
   # Health check
   HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
       CMD python -c "import vcf_pg_loader; print('healthy')" || exit 1
   
   ENTRYPOINT ["python", "-m", "vcf_pg_loader"]
   ```

2. **Create `docker-compose.hipaa.yml`** (production overlay):
   ```yaml
   version: '3.8'
   
   services:
     vcf-pg-loader:
       security_opt:
         - no-new-privileges:true
       read_only: true
       tmpfs:
         - /tmp
       cap_drop:
         - ALL
       networks:
         - internal
       logging:
         driver: json-file
         options:
           max-size: "10m"
           max-file: "3"
   
     postgres:
       security_opt:
         - no-new-privileges:true
       cap_drop:
         - ALL
       cap_add:
         - CHOWN
         - SETUID
         - SETGID
       volumes:
         - type: volume
           source: pgdata
           target: /var/lib/postgresql/data
           # Use encrypted volume
       networks:
         - internal
       logging:
         driver: json-file
         options:
           max-size: "50m"
           max-file: "5"
   
   networks:
     internal:
       driver: bridge
       internal: true  # No external access
   
   volumes:
     pgdata:
       driver: local
       driver_opts:
         type: none
         o: bind
         device: /encrypted/pgdata  # Mount encrypted filesystem
   ```

3. **Create `docker/security/seccomp-profile.json`**:
   - Restrict system calls to minimum required
   - Block dangerous syscalls

4. **Create image scanning configuration**:
   `.github/workflows/security-scan.yml`:
   - Trivy vulnerability scanning
   - Fail on HIGH/CRITICAL vulnerabilities
   - SBOM generation

5. **Create `docs/deployment/container-security.md`**:
   - Container security best practices
   - Required runtime flags
   - Network isolation requirements
   - Secret injection (not in image)
   - Log aggregation setup

6. **Add container security validation**:
   ```
   vcf-pg-loader doctor --check-container-security
   ```
   - Verify running as non-root
   - Check for writable filesystem
   - Validate network isolation
   - Check capability restrictions

## Requirements
- Non-root execution
- Read-only root filesystem
- Dropped capabilities
- No new privileges
- Network isolation
- Vulnerability scanning in CI
- Documentation of all security controls
```

---

## Prompt 5.3: Compliance Validation CLI

```
## Context
I'm implementing HIPAA compliance for vcf-pg-loader. Need automated 
compliance checking to verify proper configuration.

## Task
Create CLI commands to validate HIPAA compliance:

1. **Create `src/vcf_pg_loader/compliance/checks.py`**:
   ```python
   from dataclasses import dataclass
   from enum import Enum
   
   class ComplianceStatus(Enum):
       PASS = "pass"
       FAIL = "fail"
       WARN = "warn"
       SKIP = "skip"
   
   @dataclass
   class ComplianceCheck:
       id: str
       name: str
       hipaa_reference: str
       description: str
       severity: str  # 'critical', 'high', 'medium', 'low'
   
   @dataclass
   class CheckResult:
       check: ComplianceCheck
       status: ComplianceStatus
       message: str
       remediation: str = None
   
   class ComplianceChecks:
       """All compliance checks"""
       
       CHECKS = [
           ComplianceCheck(
               id="TLS_ENABLED",
               name="TLS Encryption in Transit",
               hipaa_reference="164.312(e)(1)",
               description="Verify TLS 1.2+ is required for all database connections",
               severity="critical"
           ),
           ComplianceCheck(
               id="AUDIT_ENABLED",
               name="Audit Logging Active",
               hipaa_reference="164.312(b)",
               description="Verify comprehensive audit logging is enabled",
               severity="critical"
           ),
           ComplianceCheck(
               id="AUTH_REQUIRED",
               name="Authentication Required",
               hipaa_reference="164.312(d)",
               description="Verify user authentication is enforced",
               severity="critical"
           ),
           ComplianceCheck(
               id="RBAC_CONFIGURED",
               name="Role-Based Access Control",
               hipaa_reference="164.312(a)(1)",
               description="Verify RBAC is properly configured",
               severity="high"
           ),
           ComplianceCheck(
               id="ENCRYPTION_AT_REST",
               name="Encryption at Rest",
               hipaa_reference="164.312(a)(2)(iv)",
               description="Verify data encryption at rest",
               severity="medium"
           ),
           ComplianceCheck(
               id="SESSION_TIMEOUT",
               name="Automatic Session Timeout",
               hipaa_reference="164.312(a)(2)(iii)",
               description="Verify automatic logoff is configured",
               severity="medium"
           ),
           # ... more checks
       ]
   ```

2. **Create `src/vcf_pg_loader/compliance/validator.py`**:
   ```python
   class ComplianceValidator:
       async def run_all_checks(self) -> ComplianceReport:
           """Run all compliance checks and return report"""
           ...
       
       async def run_check(self, check_id: str) -> CheckResult:
           """Run specific check"""
           ...
       
       async def check_tls(self) -> CheckResult:
           """Verify TLS is properly configured"""
           # Check connection uses TLS
           # Check TLS version >= 1.2
           # Check certificate validity
           ...
       
       async def check_audit_logging(self) -> CheckResult:
           """Verify audit logging is working"""
           # Check pgaudit extension loaded
           # Check audit tables exist
           # Check recent audit entries exist
           # Check immutability triggers
           ...
       
       async def check_authentication(self) -> CheckResult:
           """Verify authentication is enforced"""
           # Check users table exists
           # Check anonymous access blocked
           # Check password hashing algorithm
           ...
       
       # ... implementations for all checks
       
       def generate_report(self, results: list[CheckResult]) -> ComplianceReport:
           """Generate compliance report"""
           ...
       
       def export_report(
           self, 
           report: ComplianceReport, 
           format: str = "json"
       ) -> str:
           """Export report as JSON, HTML, or PDF"""
           ...
   ```

3. **Create `src/vcf_pg_loader/cli/compliance_commands.py`**:
   ```
   vcf-pg-loader compliance check  # Run all checks
   vcf-pg-loader compliance check --id TLS_ENABLED  # Run specific check
   vcf-pg-loader compliance report --format json --output report.json
   vcf-pg-loader compliance report --format html --output report.html
   vcf-pg-loader compliance status  # Quick summary
   ```

4. **Create report templates**:
   - JSON schema for machine-readable output
   - HTML template for human review
   - Summary format for CLI display

5. **Add CI integration**:
   - GitHub Action to run compliance checks
   - Fail pipeline on critical failures
   - Generate compliance report artifact

6. **Create ongoing monitoring**:
   ```python
   class ComplianceMonitor:
       """Continuous compliance monitoring"""
       
       async def check_and_alert(self) -> None:
           """Run checks and alert on failures"""
           ...
       
       async def schedule_checks(self, interval_hours: int = 24) -> None:
           """Schedule regular compliance checks"""
           ...
   ```

## Requirements
- All HIPAA Security Rule requirements covered
- Clear pass/fail/warn status
- Remediation guidance for failures
- Machine-readable output for CI/CD
- HTML reports for compliance documentation
- Exit code reflects compliance status
```

---

## Implementation Timeline Summary

| Phase | Duration | Chunks | Key Deliverables |
|-------|----------|--------|------------------|
| 0 | Week 1 | 0.1, 0.2 | Secrets management, TLS enforcement |
| 1 | Weeks 2-3 | 1.1, 1.2, 1.3 | Complete audit logging system |
| 2 | Weeks 4-5 | 2.1, 2.2, 2.3 | Authentication, RBAC, sessions |
| 3 | Weeks 6-7 | 3.1, 3.2, 3.3 | De-identification, PHI detection |
| 4 | Week 8 | 4.1, 4.2 | Encryption at rest, secure disposal |
| 5 | Weeks 9-10 | 5.1, 5.2, 5.3 | Documentation, Docker hardening, validation |

**Total: ~10 weeks for full HIPAA compliance implementation**

---

## Usage Notes for Claude Code

1. **Execute prompts in order** - Later chunks depend on earlier ones
2. **Test after each chunk** - Run `pytest` and manual verification
3. **Review generated code** - Security code needs careful review
4. **Update documentation** - Keep README and docs in sync
5. **Run compliance check** - After each phase, run `vcf-pg-loader compliance check`

Each prompt is designed to be self-contained with:
- Clear context and current state
- Specific files to create/modify
- Code examples where helpful
- Requirements for completion
- Testing expectations
