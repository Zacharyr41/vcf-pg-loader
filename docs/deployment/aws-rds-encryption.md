# AWS RDS PostgreSQL Encryption

HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption

This guide covers encryption options for vcf-pg-loader with AWS RDS PostgreSQL.

## Encryption at Rest

### RDS Native Encryption

AWS RDS provides AES-256 encryption for all storage.

```bash
# Create encrypted RDS instance via CLI
aws rds create-db-instance \
    --db-instance-identifier vcf-pg-loader-db \
    --db-instance-class db.r6g.large \
    --engine postgres \
    --engine-version 16.1 \
    --master-username postgres \
    --master-user-password "${DB_PASSWORD}" \
    --allocated-storage 100 \
    --storage-encrypted \
    --kms-key-id alias/vcf-pg-loader-key \
    --backup-retention-period 7 \
    --storage-type gp3
```

### Terraform Example

```hcl
resource "aws_kms_key" "rds" {
  description             = "KMS key for vcf-pg-loader RDS encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = {
    Purpose = "vcf-pg-loader"
    HIPAA   = "true"
  }
}

resource "aws_db_instance" "vcf_pg_loader" {
  identifier     = "vcf-pg-loader"
  engine         = "postgres"
  engine_version = "16.1"
  instance_class = "db.r6g.large"

  allocated_storage     = 100
  max_allocated_storage = 1000
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.rds.arn

  username = "postgres"
  password = var.db_password

  backup_retention_period = 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "Mon:04:00-Mon:05:00"

  multi_az               = true
  publicly_accessible    = false
  deletion_protection    = true
  skip_final_snapshot    = false
  final_snapshot_identifier = "vcf-pg-loader-final"

  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.rds.name

  performance_insights_enabled          = true
  performance_insights_kms_key_id       = aws_kms_key.rds.arn
  performance_insights_retention_period = 7

  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  tags = {
    Environment = "production"
    HIPAA       = "true"
  }
}
```

## Encryption in Transit

### SSL/TLS Configuration

```bash
# Download RDS CA bundle
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

# Connect with SSL required
export VCF_PG_LOADER_TLS_MODE=require
export VCF_PG_LOADER_TLS_CA_CERT=/path/to/global-bundle.pem
```

### Force SSL on RDS

```sql
-- In RDS parameter group
ssl = 1
rds.force_ssl = 1
```

## PHI Column Encryption with KMS

For additional column-level encryption using AWS KMS:

### 1. Create KMS Key

```bash
aws kms create-key \
    --description "vcf-pg-loader PHI encryption" \
    --key-usage ENCRYPT_DECRYPT \
    --key-spec SYMMETRIC_DEFAULT
```

### 2. Generate Data Encryption Key

```bash
# Generate a data key encrypted by KMS
aws kms generate-data-key \
    --key-id alias/vcf-pg-loader-phi \
    --key-spec AES_256 \
    --output json > data_key.json

# Extract the plaintext key for local use
jq -r '.Plaintext' data_key.json | base64 -d > phi_key.bin

# Store the encrypted key for recovery
jq -r '.CiphertextBlob' data_key.json > phi_key_encrypted.b64

# Set the key for vcf-pg-loader
export VCF_PG_LOADER_PHI_KEY=$(cat phi_key.bin | base64)

# Securely delete plaintext
rm phi_key.bin data_key.json
```

### 3. Key Rotation

```bash
# Re-encrypt the data key with a new KMS key version
aws kms re-encrypt \
    --source-key-id alias/vcf-pg-loader-phi \
    --destination-key-id alias/vcf-pg-loader-phi \
    --ciphertext-blob fileb://phi_key_encrypted.b64 \
    --output json > phi_key_reencrypted.json
```

## IAM Policies

### Minimum RDS Access

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds-db:connect"
            ],
            "Resource": [
                "arn:aws:rds-db:us-east-1:123456789012:dbuser:*/vcf_pg_loader_user"
            ]
        }
    ]
}
```

### KMS Key Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Allow vcf-pg-loader service",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789012:role/vcf-pg-loader-role"
            },
            "Action": [
                "kms:Decrypt",
                "kms:GenerateDataKey"
            ],
            "Resource": "*"
        }
    ]
}
```

## Secrets Manager Integration

Store database credentials in Secrets Manager:

```bash
aws secretsmanager create-secret \
    --name vcf-pg-loader/db-credentials \
    --secret-string '{"username":"postgres","password":"<password>"}'
```

Use with vcf-pg-loader:

```bash
export VCF_PG_LOADER_DB_PASSWORD=$(aws secretsmanager get-secret-value \
    --secret-id vcf-pg-loader/db-credentials \
    --query 'SecretString' --output text | jq -r '.password')
```

## Compliance Checklist

- [ ] RDS instance created with `storage_encrypted = true`
- [ ] KMS key configured with automatic rotation
- [ ] SSL/TLS enforced (`rds.force_ssl = 1`)
- [ ] VPC security groups restrict access
- [ ] IAM authentication configured (optional)
- [ ] CloudWatch logs enabled for audit
- [ ] Automated backups with encryption
- [ ] PHI column encryption key stored in Secrets Manager or KMS
- [ ] Key rotation policy documented
