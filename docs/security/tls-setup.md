# TLS Configuration for vcf-pg-loader

This document describes how to configure TLS/SSL encryption for PostgreSQL connections in vcf-pg-loader, implementing HIPAA 164.312(e)(1) encryption in transit requirements.

## Overview

vcf-pg-loader enforces TLS 1.2+ for all database connections by default. This ensures that ePHI (electronic Protected Health Information) is encrypted during transmission.

## Quick Start

### Using the Managed Database

The managed database (`vcf-pg-loader db start`) automatically configures TLS when certificates are available:

```bash
# Generate certificates for development
./scripts/generate-certs.sh

# Start the managed database with TLS
vcf-pg-loader db start
```

### Using Docker Compose

```bash
# Generate certificates
./scripts/generate-certs.sh

# Start with TLS enabled
docker compose up -d
```

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `VCF_PG_LOADER_REQUIRE_TLS` | `true` | Require TLS for all connections |
| `VCF_PG_LOADER_TLS_VERIFY` | `true` | Verify server certificate |
| `VCF_PG_LOADER_TLS_CA_CERT` | (none) | Path to CA certificate file |
| `VCF_PG_LOADER_TLS_CLIENT_CERT` | (none) | Path to client certificate |
| `VCF_PG_LOADER_TLS_CLIENT_KEY` | (none) | Path to client private key |

### CLI Options

```bash
# Require TLS (default)
vcf-pg-loader load sample.vcf --require-tls

# Disable TLS (not recommended)
vcf-pg-loader load sample.vcf --no-require-tls
```

## Certificate Generation

### Development Certificates

For development and testing, use the provided script:

```bash
./scripts/generate-certs.sh
```

This generates:
- `docker/certs/ca.crt` - Certificate Authority
- `docker/certs/server.crt` - Server certificate
- `docker/certs/server.key` - Server private key
- `docker/certs/client.crt` - Client certificate (optional)
- `docker/certs/client.key` - Client private key (optional)

### Production Certificates

For production, obtain certificates from a trusted Certificate Authority. Configure them via environment variables:

```bash
export VCF_PG_LOADER_TLS_CA_CERT=/path/to/ca-bundle.crt
export VCF_PG_LOADER_TLS_CLIENT_CERT=/path/to/client.crt
export VCF_PG_LOADER_TLS_CLIENT_KEY=/path/to/client.key
```

## PostgreSQL Server Configuration

### Required Settings (ssl.conf)

```ini
ssl = on
ssl_min_protocol_version = 'TLSv1.2'
ssl_cert_file = '/path/to/server.crt'
ssl_key_file = '/path/to/server.key'
ssl_ca_file = '/path/to/ca.crt'
ssl_ciphers = 'HIGH:MEDIUM:+3DES:!aNULL:!eNULL:!MD5:!RC4'
ssl_prefer_server_ciphers = on
```

### Host-Based Authentication (pg_hba.conf)

To enforce TLS-only connections:

```
# Reject non-SSL connections
host    all    all    0.0.0.0/0    reject
hostssl all    all    0.0.0.0/0    scram-sha-256
```

## Verification

### Check TLS Status

```bash
# Run system check
vcf-pg-loader doctor
```

Expected output:
```
TLS Support ✓ (TLS 1.2+ (TLS 1.3 available))
TLS Certificates ✓ (using system defaults)
```

### Verify PostgreSQL TLS

```sql
-- Check if SSL is enabled
SHOW ssl;

-- Check current connection
SELECT ssl, ssl_cipher, ssl_version FROM pg_stat_ssl WHERE pid = pg_backend_pid();
```

## Troubleshooting

### Connection Refused

If connections fail with TLS enabled:

1. Verify certificates exist and are readable:
   ```bash
   ls -la docker/certs/
   ```

2. Check certificate permissions (should be 600 for keys):
   ```bash
   chmod 600 docker/certs/*.key
   ```

3. Verify PostgreSQL SSL is enabled:
   ```bash
   docker exec vcf-pg-loader-db psql -U vcfloader -c "SHOW ssl"
   ```

### Certificate Verification Failed

If you see "certificate verify failed" errors:

1. For self-signed certificates, set the CA path:
   ```bash
   export VCF_PG_LOADER_TLS_CA_CERT=docker/certs/ca.crt
   ```

2. Or disable verification (development only):
   ```bash
   export VCF_PG_LOADER_TLS_VERIFY=false
   ```

### Disable TLS (Not Recommended)

For legacy systems or testing without TLS:

```bash
export VCF_PG_LOADER_REQUIRE_TLS=false
vcf-pg-loader load sample.vcf --no-require-tls
```

**Warning:** Disabling TLS violates HIPAA encryption requirements. Only use for development with non-sensitive data.

## HIPAA Compliance Notes

- TLS 1.2 is the minimum required version (TLS 1.3 preferred)
- Strong cipher suites are enforced (no weak or export ciphers)
- Certificate verification is enabled by default
- Connection attempts without TLS are rejected when `require_tls=true`
- All TLS-related events are logged for audit purposes
