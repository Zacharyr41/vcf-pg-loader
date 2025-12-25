-- HIPAA-Compliant Encryption at Rest Schema
--
-- HIPAA Citation: 45 CFR 164.312(a)(2)(iv) - Encryption and Decryption
-- "Implement a mechanism to encrypt and decrypt electronic protected
-- health information."
--
-- NIST SP 800-111 Requirements:
-- - Algorithm: AES-256 (minimum AES-128)
-- - Mode: AES-GCM or other FIPS-approved modes
-- - Key storage: Separate from encrypted data
-- - FIPS 140-3 validated cryptographic modules
--
-- HHS Breach Safe Harbor (45 CFR 164.402):
-- Properly encrypted PHI is "unusable, unreadable, or indecipherable"
-- and exempt from breach notification requirements.

CREATE TABLE IF NOT EXISTS encryption_keys (
    key_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_name VARCHAR(100) NOT NULL UNIQUE,
    key_version INTEGER NOT NULL DEFAULT 1,

    -- Key material (encrypted with master key, never stored in plaintext)
    -- The actual key material is stored encrypted by an external KMS or
    -- environment-provided master key
    encrypted_key_material BYTEA NOT NULL,
    key_nonce BYTEA NOT NULL,
    key_tag BYTEA NOT NULL,

    -- Key metadata
    algorithm VARCHAR(50) NOT NULL DEFAULT 'AES-256-GCM',
    key_length_bits INTEGER NOT NULL DEFAULT 256,
    is_active BOOLEAN NOT NULL DEFAULT true,
    purpose VARCHAR(50) NOT NULL CHECK (purpose IN (
        'data_encryption',
        'phi_encryption',
        'backup_encryption',
        'transport_encryption'
    )),

    -- Rotation tracking
    -- NIST SP 800-57 recommends key rotation
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rotated_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ,

    -- Audit
    created_by INTEGER,
    last_used_at TIMESTAMPTZ,
    use_count BIGINT DEFAULT 0,

    CONSTRAINT valid_key_length CHECK (key_length_bits IN (128, 192, 256)),
    CONSTRAINT active_not_retired CHECK (NOT (is_active AND retired_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_encryption_keys_active
    ON encryption_keys (purpose, is_active)
    WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_encryption_keys_name
    ON encryption_keys (key_name, key_version);

-- Key rotation history for audit trail
CREATE TABLE IF NOT EXISTS encryption_key_rotations (
    rotation_id SERIAL PRIMARY KEY,
    key_id UUID NOT NULL REFERENCES encryption_keys(key_id),
    old_version INTEGER NOT NULL,
    new_version INTEGER NOT NULL,
    rotated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rotated_by INTEGER,
    reason TEXT NOT NULL,
    details JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_key_rotations_key ON encryption_key_rotations (key_id);
CREATE INDEX IF NOT EXISTS idx_key_rotations_time ON encryption_key_rotations (rotated_at DESC);

-- Encrypted data references (track what data uses which keys)
CREATE TABLE IF NOT EXISTS encrypted_data_registry (
    id SERIAL PRIMARY KEY,
    key_id UUID NOT NULL REFERENCES encryption_keys(key_id),
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    encryption_method VARCHAR(50) NOT NULL DEFAULT 'column_level',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (table_name, column_name)
);

CREATE INDEX IF NOT EXISTS idx_encrypted_data_key ON encrypted_data_registry (key_id);

-- Function to get active encryption key for a purpose
CREATE OR REPLACE FUNCTION get_active_encryption_key(p_purpose VARCHAR(50))
RETURNS TABLE (
    key_id UUID,
    key_name VARCHAR(100),
    key_version INTEGER,
    encrypted_key_material BYTEA,
    key_nonce BYTEA,
    key_tag BYTEA,
    algorithm VARCHAR(50)
) AS $$
BEGIN
    -- Update last_used_at and use_count
    UPDATE encryption_keys k
    SET last_used_at = NOW(),
        use_count = k.use_count + 1
    WHERE k.purpose = p_purpose
      AND k.is_active = true
      AND (k.expires_at IS NULL OR k.expires_at > NOW());

    RETURN QUERY
    SELECT
        k.key_id,
        k.key_name,
        k.key_version,
        k.encrypted_key_material,
        k.key_nonce,
        k.key_tag,
        k.algorithm
    FROM encryption_keys k
    WHERE k.purpose = p_purpose
      AND k.is_active = true
      AND (k.expires_at IS NULL OR k.expires_at > NOW())
    ORDER BY k.created_at DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function to rotate an encryption key
CREATE OR REPLACE FUNCTION rotate_encryption_key(
    p_key_name VARCHAR(100),
    p_new_encrypted_material BYTEA,
    p_new_nonce BYTEA,
    p_new_tag BYTEA,
    p_rotated_by INTEGER,
    p_reason TEXT
)
RETURNS UUID AS $$
DECLARE
    v_old_key RECORD;
    v_new_key_id UUID;
BEGIN
    -- Get current key
    SELECT * INTO v_old_key
    FROM encryption_keys
    WHERE key_name = p_key_name AND is_active = true;

    IF v_old_key IS NULL THEN
        RAISE EXCEPTION 'No active key found with name: %', p_key_name;
    END IF;

    -- Retire old key
    UPDATE encryption_keys
    SET is_active = false,
        retired_at = NOW()
    WHERE key_id = v_old_key.key_id;

    -- Create new key version
    INSERT INTO encryption_keys (
        key_name, key_version, encrypted_key_material, key_nonce, key_tag,
        algorithm, key_length_bits, purpose, created_by, rotated_at
    ) VALUES (
        p_key_name, v_old_key.key_version + 1, p_new_encrypted_material,
        p_new_nonce, p_new_tag, v_old_key.algorithm, v_old_key.key_length_bits,
        v_old_key.purpose, p_rotated_by, NOW()
    ) RETURNING key_id INTO v_new_key_id;

    -- Log rotation
    INSERT INTO encryption_key_rotations (
        key_id, old_version, new_version, rotated_by, reason
    ) VALUES (
        v_new_key_id, v_old_key.key_version, v_old_key.key_version + 1,
        p_rotated_by, p_reason
    );

    RETURN v_new_key_id;
END;
$$ LANGUAGE plpgsql;

-- Audit retention policy table
-- HIPAA Citation: 45 CFR 164.316(b)(2)(i) - 6-year retention requirement
CREATE TABLE IF NOT EXISTS audit_retention_policy (
    policy_id SERIAL PRIMARY KEY,
    retention_years INTEGER NOT NULL CHECK (retention_years >= 6),
    enforce_minimum BOOLEAN NOT NULL DEFAULT true,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER,
    notes TEXT
);

-- Create default 6-year retention policy if none exists
INSERT INTO audit_retention_policy (retention_years, enforce_minimum, notes)
SELECT 6, true, 'HIPAA minimum retention per 45 CFR 164.316(b)(2)(i)'
WHERE NOT EXISTS (SELECT 1 FROM audit_retention_policy WHERE is_active = true);

-- View: Encryption key status summary
CREATE OR REPLACE VIEW v_encryption_key_status AS
SELECT
    k.key_id,
    k.key_name,
    k.key_version,
    k.algorithm,
    k.purpose,
    k.is_active,
    k.created_at,
    k.expires_at,
    k.retired_at,
    k.use_count,
    k.last_used_at,
    CASE
        WHEN k.retired_at IS NOT NULL THEN 'retired'
        WHEN k.expires_at IS NOT NULL AND k.expires_at < NOW() THEN 'expired'
        WHEN k.is_active THEN 'active'
        ELSE 'inactive'
    END as status,
    COUNT(DISTINCT edr.id) as encrypted_columns
FROM encryption_keys k
LEFT JOIN encrypted_data_registry edr ON edr.key_id = k.key_id
GROUP BY k.key_id, k.key_name, k.key_version, k.algorithm, k.purpose,
         k.is_active, k.created_at, k.expires_at, k.retired_at,
         k.use_count, k.last_used_at;

-- Create encryption roles
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'encryption_admin') THEN
        CREATE ROLE encryption_admin;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'key_user') THEN
        CREATE ROLE key_user;
    END IF;
END$$;

GRANT SELECT, INSERT, UPDATE ON encryption_keys TO encryption_admin;
GRANT SELECT, INSERT ON encryption_key_rotations TO encryption_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON encrypted_data_registry TO encryption_admin;
GRANT SELECT ON v_encryption_key_status TO encryption_admin;
GRANT EXECUTE ON FUNCTION rotate_encryption_key TO encryption_admin;

GRANT EXECUTE ON FUNCTION get_active_encryption_key TO key_user;
GRANT SELECT ON encryption_keys TO key_user;
