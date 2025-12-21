# Azure Database for PostgreSQL Encryption

HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption

This guide covers encryption options for vcf-pg-loader with Azure Database for PostgreSQL Flexible Server.

## Encryption at Rest

### Default Encryption

Azure encrypts all data at rest by default. For customer-managed keys (CMK):

```bash
# Create Key Vault
az keyvault create \
    --name vcf-pg-loader-kv \
    --resource-group vcf-pg-loader-rg \
    --location eastus \
    --enable-purge-protection \
    --enable-soft-delete

# Create encryption key
az keyvault key create \
    --vault-name vcf-pg-loader-kv \
    --name vcf-pg-loader-key \
    --kty RSA \
    --size 2048

# Create PostgreSQL with CMK
az postgres flexible-server create \
    --resource-group vcf-pg-loader-rg \
    --name vcf-pg-loader-db \
    --version 16 \
    --sku-name Standard_D4s_v3 \
    --storage-size 128 \
    --key $(az keyvault key show --vault-name vcf-pg-loader-kv --name vcf-pg-loader-key --query key.kid -o tsv) \
    --identity vcf-pg-loader-identity
```

### Terraform Example

```hcl
resource "azurerm_key_vault" "vcf_pg_loader" {
  name                        = "vcf-pg-loader-kv"
  location                    = azurerm_resource_group.main.location
  resource_group_name         = azurerm_resource_group.main.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days  = 90
  purge_protection_enabled    = true
  sku_name                    = "standard"
}

resource "azurerm_key_vault_key" "postgres" {
  name         = "postgres-encryption-key"
  key_vault_id = azurerm_key_vault.vcf_pg_loader.id
  key_type     = "RSA"
  key_size     = 2048

  key_opts = [
    "decrypt",
    "encrypt",
    "sign",
    "unwrapKey",
    "verify",
    "wrapKey",
  ]

  rotation_policy {
    automatic {
      time_before_expiry = "P30D"
    }
    expire_after         = "P90D"
    notify_before_expiry = "P29D"
  }
}

resource "azurerm_user_assigned_identity" "postgres" {
  name                = "vcf-pg-loader-identity"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_key_vault_access_policy" "postgres" {
  key_vault_id = azurerm_key_vault.vcf_pg_loader.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_user_assigned_identity.postgres.principal_id

  key_permissions = [
    "Get",
    "WrapKey",
    "UnwrapKey"
  ]
}

resource "azurerm_postgresql_flexible_server" "vcf_pg_loader" {
  name                   = "vcf-pg-loader"
  resource_group_name    = azurerm_resource_group.main.name
  location               = azurerm_resource_group.main.location
  version                = "16"
  delegated_subnet_id    = azurerm_subnet.postgres.id
  private_dns_zone_id    = azurerm_private_dns_zone.postgres.id
  administrator_login    = "vcfadmin"
  administrator_password = var.db_password
  zone                   = "1"
  storage_mb             = 131072
  sku_name               = "GP_Standard_D4s_v3"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.postgres.id]
  }

  customer_managed_key {
    key_vault_key_id                  = azurerm_key_vault_key.postgres.id
    primary_user_assigned_identity_id = azurerm_user_assigned_identity.postgres.id
  }

  backup_retention_days        = 7
  geo_redundant_backup_enabled = true

  high_availability {
    mode                      = "ZoneRedundant"
    standby_availability_zone = "2"
  }
}

resource "azurerm_postgresql_flexible_server_configuration" "require_ssl" {
  name      = "require_secure_transport"
  server_id = azurerm_postgresql_flexible_server.vcf_pg_loader.id
  value     = "ON"
}
```

## Encryption in Transit

### SSL/TLS Configuration

```bash
# Download Azure CA certificate
curl -o DigiCertGlobalRootCA.crt.pem \
    https://dl.cacerts.digicert.com/DigiCertGlobalRootCA.crt.pem

# Configure vcf-pg-loader
export VCF_PG_LOADER_TLS_MODE=require
export VCF_PG_LOADER_TLS_CA_CERT=/path/to/DigiCertGlobalRootCA.crt.pem
```

### Private Link

For private connectivity:

```bash
# Create private endpoint
az network private-endpoint create \
    --name vcf-pg-loader-pe \
    --resource-group vcf-pg-loader-rg \
    --vnet-name vcf-pg-loader-vnet \
    --subnet private-endpoints \
    --private-connection-resource-id $(az postgres flexible-server show \
        --resource-group vcf-pg-loader-rg \
        --name vcf-pg-loader-db \
        --query id -o tsv) \
    --group-id postgresqlServer \
    --connection-name vcf-pg-loader-connection
```

## PHI Column Encryption with Key Vault

### 1. Create Encryption Key

```bash
# Create a key for PHI encryption
az keyvault key create \
    --vault-name vcf-pg-loader-kv \
    --name phi-encryption-key \
    --kty oct \
    --size 256
```

### 2. Generate Data Encryption Key

```bash
# Generate local key and wrap with Key Vault
KEY_RAW=$(openssl rand 32 | base64)

# Store wrapped in Key Vault as secret
az keyvault secret set \
    --vault-name vcf-pg-loader-kv \
    --name phi-encryption-key-data \
    --value "$KEY_RAW"

# Set for vcf-pg-loader
export VCF_PG_LOADER_PHI_KEY="$KEY_RAW"

# Clear from shell
unset KEY_RAW
```

### 3. Key Retrieval

```bash
export VCF_PG_LOADER_PHI_KEY=$(az keyvault secret show \
    --vault-name vcf-pg-loader-kv \
    --name phi-encryption-key-data \
    --query value -o tsv)
```

## Azure RBAC

### PostgreSQL Access

```bash
# Assign AAD admin
az postgres flexible-server ad-admin create \
    --resource-group vcf-pg-loader-rg \
    --server-name vcf-pg-loader-db \
    --display-name "vcf-pg-loader-admin" \
    --object-id $(az ad user show --id admin@example.com --query id -o tsv)
```

### Key Vault Access

```json
{
  "properties": {
    "accessPolicies": [
      {
        "tenantId": "<tenant-id>",
        "objectId": "<service-principal-id>",
        "permissions": {
          "keys": ["get", "wrapKey", "unwrapKey"],
          "secrets": ["get"]
        }
      }
    ]
  }
}
```

### Managed Identity

```bash
# Create managed identity
az identity create \
    --name vcf-pg-loader-identity \
    --resource-group vcf-pg-loader-rg

# Assign Key Vault access
az keyvault set-policy \
    --name vcf-pg-loader-kv \
    --object-id $(az identity show \
        --name vcf-pg-loader-identity \
        --resource-group vcf-pg-loader-rg \
        --query principalId -o tsv) \
    --secret-permissions get \
    --key-permissions get wrapKey unwrapKey
```

## Diagnostic Logging

```bash
# Enable diagnostic logs
az monitor diagnostic-settings create \
    --name vcf-pg-loader-diag \
    --resource $(az postgres flexible-server show \
        --resource-group vcf-pg-loader-rg \
        --name vcf-pg-loader-db \
        --query id -o tsv) \
    --logs '[{"category": "PostgreSQLLogs", "enabled": true}]' \
    --workspace $(az monitor log-analytics workspace show \
        --resource-group vcf-pg-loader-rg \
        --workspace-name vcf-pg-loader-logs \
        --query id -o tsv)
```

## Compliance Checklist

- [ ] Flexible Server created with customer-managed key (CMK)
- [ ] Key Vault configured with purge protection
- [ ] Key rotation policy configured
- [ ] SSL/TLS required (`require_secure_transport = ON`)
- [ ] Private Link or VNet integration enabled
- [ ] Azure AD authentication configured (optional)
- [ ] Diagnostic logging enabled
- [ ] Geo-redundant backup enabled
- [ ] PHI column encryption key stored in Key Vault
- [ ] Managed Identity configured for key access
- [ ] Azure Defender for PostgreSQL enabled (optional)
