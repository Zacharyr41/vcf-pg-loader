# Container Security for HIPAA Compliance

This document describes container security controls for HIPAA-compliant deployments of vcf-pg-loader per NIST SP 800-190.

## Required Runtime Flags

When running the vcf-pg-loader container, the following security flags are **required**:

```bash
docker run \
  --security-opt=no-new-privileges:true \
  --read-only \
  --tmpfs /tmp:mode=1777,size=100M \
  --cap-drop=ALL \
  --user 1000:1000 \
  ghcr.io/zacharyr41/vcf-pg-loader:latest
```

### Flag Descriptions

| Flag | Purpose | HIPAA Control |
|------|---------|---------------|
| `--security-opt=no-new-privileges:true` | Prevents privilege escalation | Access Control |
| `--read-only` | Immutable root filesystem | Integrity |
| `--tmpfs /tmp` | Writable temp space in memory | Minimal surface |
| `--cap-drop=ALL` | Remove all Linux capabilities | Least privilege |
| `--user 1000:1000` | Run as non-root user | Least privilege |

## Network Isolation

Production deployments must use isolated networks:

```yaml
networks:
  internal:
    driver: bridge
    internal: true  # No external access
```

The `internal: true` setting prevents containers from accessing external networks. All database connections should be within the internal network.

### Required Network Configuration

1. **No external network access** from the vcf-pg-loader container
2. **Database on internal network only**
3. **TLS required** for all database connections
4. **No published ports** except through a reverse proxy with TLS termination

## Secret Management

**Never include secrets in container images.**

### Recommended Approaches

1. **Docker Secrets** (Swarm mode):
   ```yaml
   secrets:
     postgres_password:
       file: ./secrets/postgres_password.txt
   ```

2. **Environment Variables** (from secure source):
   ```bash
   docker run \
     -e DATABASE_URL="$(vault read -field=url secret/database)" \
     ghcr.io/zacharyr41/vcf-pg-loader:latest
   ```

3. **Mounted Secret Files**:
   ```bash
   docker run \
     -v /secure/secrets:/secrets:ro \
     -e DATABASE_URL_FILE=/secrets/database_url \
     ghcr.io/zacharyr41/vcf-pg-loader:latest
   ```

### Secrets Checklist

- [ ] No secrets in Dockerfile
- [ ] No secrets in docker-compose.yml
- [ ] No secrets in environment file checked into git
- [ ] Secrets rotated regularly
- [ ] Secrets access logged

## Log Aggregation

Configure structured logging for HIPAA audit requirements:

```yaml
logging:
  driver: json-file
  options:
    max-size: "10m"
    max-file: "3"
    labels: "service,environment"
```

### Production Log Requirements

1. **Centralized logging** - Ship logs to a SIEM (Splunk, ELK, etc.)
2. **Log retention** - Minimum 6 years per HIPAA
3. **Log encryption** - Encrypt logs at rest and in transit
4. **Access logging** - All PHI access must be logged
5. **Immutable logs** - Logs cannot be modified after creation

### Example Fluentd Configuration

```yaml
logging:
  driver: fluentd
  options:
    fluentd-address: "localhost:24224"
    tag: "vcf-pg-loader.{{.Name}}"
    fluentd-async: "true"
```

## Encrypted Volume Requirements

All persistent data must use encrypted volumes:

### Linux with LUKS

```bash
# Create encrypted volume
cryptsetup luksFormat /dev/sdb1
cryptsetup open /dev/sdb1 pgdata
mkfs.ext4 /dev/mapper/pgdata
mount /dev/mapper/pgdata /encrypted/pgdata
```

### Docker Volume with Encryption

```yaml
volumes:
  pgdata:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /encrypted/pgdata
```

### AWS EBS Encryption

```yaml
volumes:
  pgdata:
    driver: rexray/ebs
    driver_opts:
      encrypted: "true"
      kmsKeyId: "alias/hipaa-key"
```

## Seccomp Profile

The included seccomp profile (`docker/security/seccomp-profile.json`) restricts system calls to the minimum required. Apply it with:

```bash
docker run \
  --security-opt seccomp=docker/security/seccomp-profile.json \
  ghcr.io/zacharyr41/vcf-pg-loader:latest
```

## Health Checks

The container includes a health check. Verify with:

```bash
docker inspect --format='{{.State.Health.Status}}' container_name
```

## Vulnerability Scanning

CI/CD pipelines include:

1. **Trivy** - Container vulnerability scanning
2. **Hadolint** - Dockerfile best practices
3. **Gitleaks** - Secret detection

### Manual Scan

```bash
trivy image --severity HIGH,CRITICAL ghcr.io/zacharyr41/vcf-pg-loader:latest
```

## Container Security Validation

Use the built-in doctor command to verify container security:

```bash
vcf-pg-loader doctor --check-container-security
```

This validates:
- Running as non-root user
- Read-only root filesystem
- Dropped capabilities
- Network isolation

## Compliance Checklist

### HIPAA Technical Safeguards

- [x] **Access Control (164.312(a)(1))**: Non-root execution, dropped capabilities
- [x] **Audit Controls (164.312(b))**: Structured logging, log aggregation
- [x] **Integrity (164.312(c)(1))**: Read-only filesystem, signed images
- [x] **Transmission Security (164.312(e)(1))**: TLS required, network isolation

### NIST SP 800-190 Controls

- [x] **4.1**: Use minimal base images (slim-bookworm)
- [x] **4.2**: Vulnerability scanning in CI
- [x] **4.3**: Content trust / image signing
- [x] **4.4**: Non-root execution
- [x] **4.5**: Read-only root filesystem
- [x] **4.6**: Resource limits
- [x] **4.7**: Network segmentation

## Quick Reference

### Development (docker-compose.yml)
```bash
docker compose up -d
```

### Production (docker-compose.hipaa.yml)
```bash
docker compose -f docker-compose.hipaa.yml up -d
```

### Verify Security Settings
```bash
docker inspect container_name | jq '.[0].HostConfig.SecurityOpt'
docker inspect container_name | jq '.[0].HostConfig.ReadonlyRootfs'
docker inspect container_name | jq '.[0].Config.User'
```
