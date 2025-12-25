# GCP Cloud SQL PostgreSQL Encryption

HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption

This guide covers encryption options for vcf-pg-loader with GCP Cloud SQL PostgreSQL.

## Encryption at Rest

### Default Encryption

Cloud SQL encrypts all data at rest by default using AES-256. For customer-managed keys (CMEK):

```bash
# Create Cloud SQL instance with CMEK
gcloud sql instances create vcf-pg-loader-db \
    --database-version=POSTGRES_16 \
    --tier=db-custom-4-16384 \
    --region=us-central1 \
    --disk-encryption-key=projects/PROJECT_ID/locations/us-central1/keyRings/RING/cryptoKeys/KEY \
    --availability-type=REGIONAL \
    --backup-start-time=03:00 \
    --enable-point-in-time-recovery
```

### Terraform Example

```hcl
resource "google_kms_key_ring" "cloudsql" {
  name     = "vcf-pg-loader-keyring"
  location = "us-central1"
}

resource "google_kms_crypto_key" "cloudsql" {
  name            = "vcf-pg-loader-key"
  key_ring        = google_kms_key_ring.cloudsql.id
  rotation_period = "7776000s"  # 90 days

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_project_service_identity" "cloudsql" {
  provider = google-beta
  project  = var.project_id
  service  = "sqladmin.googleapis.com"
}

resource "google_kms_crypto_key_iam_binding" "cloudsql" {
  crypto_key_id = google_kms_crypto_key.cloudsql.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"

  members = [
    "serviceAccount:${google_project_service_identity.cloudsql.email}",
  ]
}

resource "google_sql_database_instance" "vcf_pg_loader" {
  name                = "vcf-pg-loader"
  database_version    = "POSTGRES_16"
  region              = "us-central1"
  deletion_protection = true

  encryption_key_name = google_kms_crypto_key.cloudsql.id

  settings {
    tier              = "db-custom-4-16384"
    availability_type = "REGIONAL"

    backup_configuration {
      enabled                        = true
      start_time                     = "03:00"
      point_in_time_recovery_enabled = true
      backup_retention_settings {
        retained_backups = 7
      }
    }

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
      require_ssl     = true
    }

    database_flags {
      name  = "log_statement"
      value = "all"
    }

    insights_config {
      query_insights_enabled  = true
      query_string_length     = 1024
      record_application_tags = true
      record_client_address   = true
    }
  }
}

resource "google_sql_user" "vcf_pg_loader" {
  instance = google_sql_database_instance.vcf_pg_loader.name
  name     = "vcf_pg_loader"
  password = var.db_password
}
```

## Encryption in Transit

### SSL/TLS Configuration

```bash
# Download Cloud SQL server CA
gcloud sql ssl server-certs create --instance=vcf-pg-loader-db

# Download client certificate
gcloud sql ssl client-certs create vcf-pg-loader-client \
    --instance=vcf-pg-loader-db

# Configure vcf-pg-loader
export VCF_PG_LOADER_TLS_MODE=verify-full
export VCF_PG_LOADER_TLS_CA_CERT=/path/to/server-ca.pem
export VCF_PG_LOADER_TLS_CERT=/path/to/client-cert.pem
export VCF_PG_LOADER_TLS_KEY=/path/to/client-key.pem
```

### Cloud SQL Proxy

For private connectivity:

```bash
# Start Cloud SQL Proxy
cloud-sql-proxy \
    --private-ip \
    PROJECT_ID:REGION:vcf-pg-loader-db &

# Connect via proxy
export POSTGRES_URL="postgresql://vcf_pg_loader@127.0.0.1:5432/variants"
```

## PHI Column Encryption with Cloud KMS

### 1. Create KMS Key

```bash
gcloud kms keyrings create vcf-pg-loader \
    --location=us-central1

gcloud kms keys create phi-encryption \
    --keyring=vcf-pg-loader \
    --location=us-central1 \
    --purpose=encryption \
    --rotation-period=90d
```

### 2. Generate Data Encryption Key

```bash
# Generate 256-bit key and encrypt with KMS
KEY_RAW=$(openssl rand 32)
KEY_B64=$(echo -n "$KEY_RAW" | base64)

# Encrypt the key for storage
echo -n "$KEY_B64" | gcloud kms encrypt \
    --key=phi-encryption \
    --keyring=vcf-pg-loader \
    --location=us-central1 \
    --plaintext-file=- \
    --ciphertext-file=phi_key_encrypted.bin

# Set for vcf-pg-loader
export VCF_PG_LOADER_PHI_KEY="$KEY_B64"

# Clear from shell history
unset KEY_RAW KEY_B64
```

### 3. Key Recovery

```bash
# Decrypt stored key
gcloud kms decrypt \
    --key=phi-encryption \
    --keyring=vcf-pg-loader \
    --location=us-central1 \
    --ciphertext-file=phi_key_encrypted.bin \
    --plaintext-file=-
```

## IAM Roles

### Cloud SQL Client

```yaml
# Service account for vcf-pg-loader
apiVersion: iam.googleapis.com/v1
kind: ServiceAccount
metadata:
  name: vcf-pg-loader-sa
spec:
  displayName: vcf-pg-loader Service Account

---
# IAM binding
bindings:
  - role: roles/cloudsql.client
    members:
      - serviceAccount:vcf-pg-loader-sa@PROJECT_ID.iam.gserviceaccount.com
  - role: roles/cloudkms.cryptoKeyDecrypter
    members:
      - serviceAccount:vcf-pg-loader-sa@PROJECT_ID.iam.gserviceaccount.com
```

### Workload Identity (GKE)

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: vcf-pg-loader
  annotations:
    iam.gke.io/gcp-service-account: vcf-pg-loader-sa@PROJECT_ID.iam.gserviceaccount.com
```

## Secret Manager Integration

```bash
# Store database password
echo -n "${DB_PASSWORD}" | gcloud secrets create vcf-pg-loader-db-password \
    --data-file=-

# Access in application
gcloud secrets versions access latest \
    --secret=vcf-pg-loader-db-password
```

Use with vcf-pg-loader:

```bash
export VCF_PG_LOADER_DB_PASSWORD=$(gcloud secrets versions access latest \
    --secret=vcf-pg-loader-db-password)
```

## Audit Logging

Enable Data Access logs for Cloud SQL:

```bash
gcloud logging sinks create vcf-pg-loader-audit \
    bigquery.googleapis.com/projects/PROJECT_ID/datasets/audit_logs \
    --log-filter='protoPayload.serviceName="sqladmin.googleapis.com"'
```

## Compliance Checklist

- [ ] Cloud SQL instance created with CMEK encryption
- [ ] KMS key configured with automatic rotation
- [ ] SSL/TLS required (`require_ssl = true`)
- [ ] Private IP only (no public IP)
- [ ] Cloud SQL Proxy configured for connections
- [ ] IAM database authentication enabled (optional)
- [ ] Audit logging enabled (Data Access logs)
- [ ] Point-in-time recovery enabled
- [ ] PHI column encryption key stored in Secret Manager
- [ ] VPC Service Controls configured (optional)
