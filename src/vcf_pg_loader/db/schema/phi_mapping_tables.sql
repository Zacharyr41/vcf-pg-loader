-- PHI Vault: HIPAA-Compliant Sample ID Mapping
-- HIPAA Reference: 164.514(b) - De-identification Standard
--
-- This schema stores the mapping between original (potentially PHI-containing)
-- sample IDs and anonymous UUIDs. Access should be heavily restricted.

-- Create separate schema for PHI vault (can be in different database for isolation)
CREATE SCHEMA IF NOT EXISTS phi_vault;

-- Sample ID mapping table
CREATE TABLE IF NOT EXISTS phi_vault.sample_id_mapping (
    mapping_id BIGSERIAL PRIMARY KEY,
    anonymous_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),

    -- Original identifier (may contain PHI)
    original_id TEXT NOT NULL,

    -- Source tracking for audit trail
    source_file TEXT NOT NULL,
    load_batch_id UUID NOT NULL,

    -- Optional encryption of original ID (AES-256-GCM)
    original_id_encrypted BYTEA,
    encryption_iv BYTEA,

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER,

    -- Ensure unique mapping per source file
    UNIQUE (original_id, source_file)
);

-- Index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_phi_mapping_anonymous_id
    ON phi_vault.sample_id_mapping (anonymous_id);
CREATE INDEX IF NOT EXISTS idx_phi_mapping_batch
    ON phi_vault.sample_id_mapping (load_batch_id);
CREATE INDEX IF NOT EXISTS idx_phi_mapping_source
    ON phi_vault.sample_id_mapping (source_file);

-- Reverse lookup audit table
CREATE TABLE IF NOT EXISTS phi_vault.reverse_lookup_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    mapping_id BIGINT NOT NULL REFERENCES phi_vault.sample_id_mapping(mapping_id),
    anonymous_id UUID NOT NULL,
    requester_id INTEGER NOT NULL,
    lookup_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    client_ip INET,
    reason TEXT,
    success BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_reverse_lookup_time
    ON phi_vault.reverse_lookup_audit (lookup_time DESC);
CREATE INDEX IF NOT EXISTS idx_reverse_lookup_requester
    ON phi_vault.reverse_lookup_audit (requester_id, lookup_time DESC);

-- Immutability trigger for mappings (cannot delete or modify existing mappings)
CREATE OR REPLACE FUNCTION phi_vault.prevent_mapping_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'PHI Compliance: Sample ID mappings cannot be modified or deleted. '
        'Mapping ID: %. This restriction is required for HIPAA de-identification audit trail.',
        OLD.mapping_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS mapping_immutability ON phi_vault.sample_id_mapping;
CREATE TRIGGER mapping_immutability
    BEFORE UPDATE OR DELETE ON phi_vault.sample_id_mapping
    FOR EACH ROW
    EXECUTE FUNCTION phi_vault.prevent_mapping_modification();

-- Function to get or create anonymous ID
CREATE OR REPLACE FUNCTION phi_vault.get_or_create_anonymous_id(
    p_original_id TEXT,
    p_source_file TEXT,
    p_load_batch_id UUID,
    p_created_by INTEGER DEFAULT NULL,
    p_encrypted_id BYTEA DEFAULT NULL,
    p_encryption_iv BYTEA DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_anonymous_id UUID;
BEGIN
    SELECT anonymous_id INTO v_anonymous_id
    FROM phi_vault.sample_id_mapping
    WHERE original_id = p_original_id AND source_file = p_source_file;

    IF v_anonymous_id IS NULL THEN
        INSERT INTO phi_vault.sample_id_mapping (
            original_id, source_file, load_batch_id, created_by,
            original_id_encrypted, encryption_iv
        ) VALUES (
            p_original_id, p_source_file, p_load_batch_id, p_created_by,
            p_encrypted_id, p_encryption_iv
        )
        RETURNING anonymous_id INTO v_anonymous_id;
    END IF;

    RETURN v_anonymous_id;
END;
$$ LANGUAGE plpgsql;

-- Function for audited reverse lookup
CREATE OR REPLACE FUNCTION phi_vault.reverse_lookup(
    p_anonymous_id UUID,
    p_requester_id INTEGER,
    p_client_ip INET DEFAULT NULL,
    p_reason TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_original_id TEXT;
    v_mapping_id BIGINT;
BEGIN
    SELECT original_id, mapping_id INTO v_original_id, v_mapping_id
    FROM phi_vault.sample_id_mapping
    WHERE anonymous_id = p_anonymous_id;

    INSERT INTO phi_vault.reverse_lookup_audit (
        mapping_id, anonymous_id, requester_id, client_ip, reason, success
    ) VALUES (
        COALESCE(v_mapping_id, -1), p_anonymous_id, p_requester_id,
        p_client_ip, p_reason, v_original_id IS NOT NULL
    );

    RETURN v_original_id;
END;
$$ LANGUAGE plpgsql;

-- Statistics view
CREATE OR REPLACE VIEW phi_vault.v_mapping_stats AS
SELECT
    COUNT(*) as total_mappings,
    COUNT(DISTINCT source_file) as unique_files,
    COUNT(DISTINCT load_batch_id) as total_batches,
    COUNT(*) FILTER (WHERE original_id_encrypted IS NOT NULL) as encrypted_count,
    MIN(created_at) as oldest_mapping,
    MAX(created_at) as newest_mapping
FROM phi_vault.sample_id_mapping;

-- Reverse lookup statistics
CREATE OR REPLACE VIEW phi_vault.v_lookup_stats AS
SELECT
    COUNT(*) as total_lookups,
    COUNT(DISTINCT requester_id) as unique_requesters,
    COUNT(*) FILTER (WHERE NOT success) as failed_lookups,
    MIN(lookup_time) as first_lookup,
    MAX(lookup_time) as last_lookup
FROM phi_vault.reverse_lookup_audit;

-- Row-Level Security
ALTER TABLE phi_vault.sample_id_mapping ENABLE ROW LEVEL SECURITY;
ALTER TABLE phi_vault.reverse_lookup_audit ENABLE ROW LEVEL SECURITY;

-- Create phi_admin role if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'phi_admin') THEN
        CREATE ROLE phi_admin;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'phi_viewer') THEN
        CREATE ROLE phi_viewer;
    END IF;
END$$;

-- Restrict schema access
REVOKE ALL ON SCHEMA phi_vault FROM PUBLIC;
GRANT USAGE ON SCHEMA phi_vault TO phi_admin;
GRANT USAGE ON SCHEMA phi_vault TO phi_viewer;

-- Grant permissions
GRANT SELECT, INSERT ON phi_vault.sample_id_mapping TO phi_admin;
GRANT SELECT ON phi_vault.sample_id_mapping TO phi_viewer;
GRANT SELECT, INSERT ON phi_vault.reverse_lookup_audit TO phi_admin;
GRANT SELECT ON phi_vault.reverse_lookup_audit TO phi_viewer;
GRANT SELECT ON phi_vault.v_mapping_stats TO phi_admin;
GRANT SELECT ON phi_vault.v_mapping_stats TO phi_viewer;
GRANT SELECT ON phi_vault.v_lookup_stats TO phi_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA phi_vault TO phi_admin;

-- Policies for phi_admin
DROP POLICY IF EXISTS phi_mapping_admin ON phi_vault.sample_id_mapping;
CREATE POLICY phi_mapping_admin ON phi_vault.sample_id_mapping
    FOR ALL
    USING (
        pg_has_role(current_user, 'phi_admin', 'MEMBER')
        OR current_user = 'postgres'
    );

DROP POLICY IF EXISTS phi_audit_admin ON phi_vault.reverse_lookup_audit;
CREATE POLICY phi_audit_admin ON phi_vault.reverse_lookup_audit
    FOR ALL
    USING (
        pg_has_role(current_user, 'phi_admin', 'MEMBER')
        OR current_user = 'postgres'
    );

-- Policies for phi_viewer (read-only, cannot see original_id)
DROP POLICY IF EXISTS phi_mapping_viewer ON phi_vault.sample_id_mapping;
CREATE POLICY phi_mapping_viewer ON phi_vault.sample_id_mapping
    FOR SELECT
    USING (pg_has_role(current_user, 'phi_viewer', 'MEMBER'));
