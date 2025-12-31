#!/bin/bash
# Generate TLS certificates for PostgreSQL
# For development/testing only - use proper CA-signed certs in production

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CERT_DIR="${PROJECT_ROOT}/docker/certs"
VALIDITY_DAYS=365

echo "Generating TLS certificates for vcf-pg-loader..."
echo "Output directory: ${CERT_DIR}"

mkdir -p "${CERT_DIR}"

if [ -f "${CERT_DIR}/ca.crt" ]; then
    echo "Certificates already exist. Remove ${CERT_DIR} to regenerate."
    exit 0
fi

echo "Generating CA private key..."
openssl genrsa -out "${CERT_DIR}/ca.key" 4096

echo "Generating CA certificate..."
openssl req -new -x509 -days ${VALIDITY_DAYS} \
    -key "${CERT_DIR}/ca.key" \
    -out "${CERT_DIR}/ca.crt" \
    -subj "/CN=vcf-pg-loader-ca/O=vcf-pg-loader/OU=Development"

echo "Generating server private key..."
openssl genrsa -out "${CERT_DIR}/server.key" 2048

echo "Generating server certificate signing request..."
openssl req -new \
    -key "${CERT_DIR}/server.key" \
    -out "${CERT_DIR}/server.csr" \
    -subj "/CN=postgres/O=vcf-pg-loader/OU=Server"

cat > "${CERT_DIR}/server.ext" << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = postgres
DNS.3 = postgres-test
DNS.4 = vcf-pg-loader-db
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

echo "Signing server certificate..."
openssl x509 -req -days ${VALIDITY_DAYS} \
    -in "${CERT_DIR}/server.csr" \
    -CA "${CERT_DIR}/ca.crt" \
    -CAkey "${CERT_DIR}/ca.key" \
    -CAcreateserial \
    -out "${CERT_DIR}/server.crt" \
    -extfile "${CERT_DIR}/server.ext"

echo "Generating client private key..."
openssl genrsa -out "${CERT_DIR}/client.key" 2048

echo "Generating client certificate signing request..."
openssl req -new \
    -key "${CERT_DIR}/client.key" \
    -out "${CERT_DIR}/client.csr" \
    -subj "/CN=vcfloader/O=vcf-pg-loader/OU=Client"

cat > "${CERT_DIR}/client.ext" << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

echo "Signing client certificate..."
openssl x509 -req -days ${VALIDITY_DAYS} \
    -in "${CERT_DIR}/client.csr" \
    -CA "${CERT_DIR}/ca.crt" \
    -CAkey "${CERT_DIR}/ca.key" \
    -CAcreateserial \
    -out "${CERT_DIR}/client.crt" \
    -extfile "${CERT_DIR}/client.ext"

echo "Setting file permissions..."
chmod 600 "${CERT_DIR}"/*.key
chmod 644 "${CERT_DIR}"/*.crt

rm -f "${CERT_DIR}"/*.csr "${CERT_DIR}"/*.ext "${CERT_DIR}"/*.srl

echo ""
echo "Certificates generated successfully:"
echo "  CA Certificate:     ${CERT_DIR}/ca.crt"
echo "  Server Certificate: ${CERT_DIR}/server.crt"
echo "  Server Key:         ${CERT_DIR}/server.key"
echo "  Client Certificate: ${CERT_DIR}/client.crt"
echo "  Client Key:         ${CERT_DIR}/client.key"
echo ""
echo "To use these certificates, set the following environment variables:"
echo "  export VCF_PG_LOADER_TLS_CA_CERT=${CERT_DIR}/ca.crt"
echo "  export VCF_PG_LOADER_TLS_CLIENT_CERT=${CERT_DIR}/client.crt"
echo "  export VCF_PG_LOADER_TLS_CLIENT_KEY=${CERT_DIR}/client.key"
