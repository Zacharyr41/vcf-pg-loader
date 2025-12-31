#!/bin/bash
set -euo pipefail

# Setup encrypted volume for PostgreSQL data
# HIPAA Reference: 164.312(a)(2)(iv) - Encryption and Decryption
#
# This script creates a LUKS-encrypted volume for PostgreSQL data storage.
# For production use, consider using cloud provider managed encryption.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VOLUME_NAME="${1:-vcf_pg_encrypted}"
VOLUME_SIZE="${2:-10G}"
MOUNT_POINT="${3:-/var/lib/postgresql/encrypted}"

usage() {
    cat << EOF
Usage: $0 [volume_name] [size] [mount_point]

Create a LUKS-encrypted volume for PostgreSQL data.

Arguments:
  volume_name   Name of the Docker volume (default: vcf_pg_encrypted)
  size          Size of the volume (default: 10G)
  mount_point   Mount point inside container (default: /var/lib/postgresql/encrypted)

Examples:
  $0
  $0 my_encrypted_volume 50G /data/postgres

Requirements:
  - Linux with cryptsetup installed
  - Root access for LUKS operations
  - Docker for volume management

For macOS/Windows, use FileVault/BitLocker on the Docker data directory.
EOF
}

check_requirements() {
    if [[ "$(uname)" != "Linux" ]]; then
        echo "Warning: LUKS encryption is Linux-only."
        echo "For macOS: Enable FileVault on the Docker data directory"
        echo "For Windows: Enable BitLocker on the Docker data directory"
        echo ""
        echo "Alternatively, use Docker's encrypted volumes or cloud provider encryption."
        exit 0
    fi

    if ! command -v cryptsetup &> /dev/null; then
        echo "Error: cryptsetup is required. Install with:"
        echo "  apt-get install cryptsetup  # Debian/Ubuntu"
        echo "  yum install cryptsetup      # RHEL/CentOS"
        exit 1
    fi

    if [[ $EUID -ne 0 ]]; then
        echo "Error: This script requires root privileges for LUKS operations."
        echo "Run with: sudo $0 $*"
        exit 1
    fi
}

create_encrypted_volume() {
    local backing_file="/var/lib/docker/volumes/${VOLUME_NAME}_backing"
    local luks_name="${VOLUME_NAME}_crypt"

    echo "Creating encrypted volume: ${VOLUME_NAME}"
    echo "  Size: ${VOLUME_SIZE}"
    echo "  Backing file: ${backing_file}"
    echo ""

    # Create backing file
    if [[ -f "${backing_file}" ]]; then
        echo "Error: Backing file already exists: ${backing_file}"
        echo "To recreate, first remove it manually."
        exit 1
    fi

    echo "Creating backing file..."
    fallocate -l "${VOLUME_SIZE}" "${backing_file}" || \
        dd if=/dev/zero of="${backing_file}" bs=1M count=$(echo "${VOLUME_SIZE}" | sed 's/G/*1024/' | bc)

    # Setup LUKS encryption
    echo ""
    echo "Setting up LUKS encryption..."
    echo "You will be prompted to create an encryption passphrase."
    echo "IMPORTANT: Store this passphrase securely - it cannot be recovered!"
    echo ""

    cryptsetup luksFormat "${backing_file}"

    # Open the encrypted volume
    echo ""
    echo "Opening encrypted volume..."
    cryptsetup luksOpen "${backing_file}" "${luks_name}"

    # Create filesystem
    echo "Creating ext4 filesystem..."
    mkfs.ext4 "/dev/mapper/${luks_name}"

    # Create mount point
    mkdir -p "${MOUNT_POINT}"

    # Mount the volume
    mount "/dev/mapper/${luks_name}" "${MOUNT_POINT}"

    # Set permissions for PostgreSQL
    chown -R 999:999 "${MOUNT_POINT}"  # postgres user in official image

    echo ""
    echo "Encrypted volume created successfully!"
    echo ""
    echo "Volume mounted at: ${MOUNT_POINT}"
    echo ""
    echo "To use with Docker:"
    echo "  docker run -v ${MOUNT_POINT}:/var/lib/postgresql/data postgres:16"
    echo ""
    echo "To unmount and close:"
    echo "  umount ${MOUNT_POINT}"
    echo "  cryptsetup luksClose ${luks_name}"
    echo ""
    echo "To remount after reboot:"
    echo "  cryptsetup luksOpen ${backing_file} ${luks_name}"
    echo "  mount /dev/mapper/${luks_name} ${MOUNT_POINT}"
}

create_docker_compose_example() {
    cat << 'EOF'

# Example docker-compose.yml for encrypted PostgreSQL:

version: '3.8'
services:
  postgres:
    image: postgres:16
    volumes:
      # Mount the LUKS-encrypted volume
      - /var/lib/postgresql/encrypted:/var/lib/postgresql/data
    environment:
      POSTGRES_PASSWORD_FILE: /run/secrets/db_password
    secrets:
      - db_password

secrets:
  db_password:
    file: ./secrets/db_password.txt

# Note: Ensure LUKS volume is mounted before starting containers.
# Add to systemd service or startup script:
#   cryptsetup luksOpen /var/lib/docker/volumes/vcf_pg_encrypted_backing vcf_pg_encrypted_crypt
#   mount /dev/mapper/vcf_pg_encrypted_crypt /var/lib/postgresql/encrypted
EOF
}

# Parse arguments
case "${1:-}" in
    -h|--help)
        usage
        exit 0
        ;;
    --example)
        create_docker_compose_example
        exit 0
        ;;
esac

check_requirements
create_encrypted_volume

echo ""
echo "For docker-compose example, run: $0 --example"
