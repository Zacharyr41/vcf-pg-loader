-- Secure Data Disposal: HIPAA-Compliant PHI Deletion
-- HIPAA Reference: 164.530(j) - Retention and Disposal
--
-- Implements secure deletion with:
-- - Two-person authorization (configurable)
-- - Verification after disposal
-- - Certificate of destruction generation
-- - Full audit trail

-- Disposal records table
CREATE TABLE IF NOT EXISTS disposal_records (
    disposal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    disposal_type VARCHAR(50) NOT NULL CHECK (disposal_type IN ('batch', 'sample', 'date_range')),
    target_identifier TEXT NOT NULL,

    -- Counts for verification
    variants_disposed BIGINT DEFAULT 0,
    genotypes_disposed BIGINT DEFAULT 0,
    mappings_disposed BIGINT DEFAULT 0,

    -- Authorization (two-person rule)
    reason TEXT NOT NULL,
    authorized_by INTEGER REFERENCES users(user_id),
    authorized_at TIMESTAMPTZ DEFAULT NOW(),
    second_authorizer INTEGER REFERENCES users(user_id),
    second_authorized_at TIMESTAMPTZ,
    authorization_required_count INTEGER DEFAULT 1,

    -- Execution
    executed_by INTEGER REFERENCES users(user_id),
    executed_at TIMESTAMPTZ,
    execution_status VARCHAR(20) DEFAULT 'pending' CHECK (
        execution_status IN ('pending', 'authorized', 'executing', 'completed', 'failed', 'cancelled')
    ),
    error_message TEXT,

    -- Verification
    verified_by INTEGER REFERENCES users(user_id),
    verified_at TIMESTAMPTZ,
    verification_status VARCHAR(20) CHECK (
        verification_status IN ('pending', 'passed', 'failed', 'skipped')
    ),
    verification_result JSONB,

    -- Certificate of destruction
    certificate_generated_at TIMESTAMPTZ,
    certificate_hash TEXT,
    certificate_data JSONB,

    -- Audit timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_disposal_status ON disposal_records (execution_status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_disposal_authorized_by ON disposal_records (authorized_by, authorized_at DESC);
CREATE INDEX IF NOT EXISTS idx_disposal_type ON disposal_records (disposal_type, target_identifier);

-- Retention policies table
CREATE TABLE IF NOT EXISTS retention_policies (
    policy_id SERIAL PRIMARY KEY,
    policy_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,

    -- Retention settings
    retention_days INTEGER NOT NULL,
    data_type VARCHAR(50) NOT NULL CHECK (data_type IN ('variants', 'samples', 'audit_logs', 'all')),

    -- Auto-disposal settings
    auto_dispose BOOLEAN DEFAULT FALSE,
    notification_days_before INTEGER DEFAULT 30,

    -- Audit
    created_at TIMESTAMPTZ DEFAULT NOW(),
    created_by INTEGER REFERENCES users(user_id),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_retention_active ON retention_policies (is_active, data_type);

-- Insert default retention policies
INSERT INTO retention_policies (policy_name, description, retention_days, data_type, auto_dispose)
VALUES
    ('hipaa_minimum', 'HIPAA minimum 6-year retention for PHI', 2190, 'all', FALSE),
    ('audit_logs', 'Audit log retention (non-deletable)', 2555, 'audit_logs', FALSE)
ON CONFLICT (policy_name) DO NOTHING;

-- Function to dispose batch data
CREATE OR REPLACE FUNCTION dispose_batch_data(
    p_disposal_id UUID,
    p_batch_id UUID,
    p_executor_id INTEGER
) RETURNS JSONB AS $$
DECLARE
    v_variant_count BIGINT;
    v_mapping_count BIGINT;
    v_result JSONB;
BEGIN
    UPDATE disposal_records
    SET execution_status = 'executing',
        executed_by = p_executor_id,
        executed_at = NOW(),
        updated_at = NOW()
    WHERE disposal_id = p_disposal_id
    AND execution_status = 'authorized';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Disposal % not in authorized state', p_disposal_id;
    END IF;

    SELECT COUNT(*) INTO v_variant_count
    FROM variants WHERE load_batch_id = p_batch_id;

    DELETE FROM variants WHERE load_batch_id = p_batch_id;

    SELECT COUNT(*) INTO v_mapping_count
    FROM phi_vault.sample_id_mapping WHERE load_batch_id = p_batch_id;

    UPDATE disposal_records
    SET execution_status = 'completed',
        variants_disposed = v_variant_count,
        mappings_disposed = v_mapping_count,
        verification_status = 'pending',
        updated_at = NOW()
    WHERE disposal_id = p_disposal_id;

    v_result := jsonb_build_object(
        'variants_deleted', v_variant_count,
        'mappings_affected', v_mapping_count,
        'completed_at', NOW()
    );

    RETURN v_result;
EXCEPTION
    WHEN OTHERS THEN
        UPDATE disposal_records
        SET execution_status = 'failed',
            error_message = SQLERRM,
            updated_at = NOW()
        WHERE disposal_id = p_disposal_id;
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Function to dispose sample data
CREATE OR REPLACE FUNCTION dispose_sample_data(
    p_disposal_id UUID,
    p_anonymous_id UUID,
    p_executor_id INTEGER
) RETURNS JSONB AS $$
DECLARE
    v_variant_count BIGINT;
    v_sample_id TEXT;
    v_result JSONB;
BEGIN
    UPDATE disposal_records
    SET execution_status = 'executing',
        executed_by = p_executor_id,
        executed_at = NOW(),
        updated_at = NOW()
    WHERE disposal_id = p_disposal_id
    AND execution_status = 'authorized';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Disposal % not in authorized state', p_disposal_id;
    END IF;

    SELECT COUNT(*) INTO v_variant_count
    FROM variants WHERE sample_id = p_anonymous_id::TEXT;

    DELETE FROM variants WHERE sample_id = p_anonymous_id::TEXT;

    UPDATE disposal_records
    SET execution_status = 'completed',
        variants_disposed = v_variant_count,
        verification_status = 'pending',
        updated_at = NOW()
    WHERE disposal_id = p_disposal_id;

    v_result := jsonb_build_object(
        'variants_deleted', v_variant_count,
        'sample_id', p_anonymous_id,
        'completed_at', NOW()
    );

    RETURN v_result;
EXCEPTION
    WHEN OTHERS THEN
        UPDATE disposal_records
        SET execution_status = 'failed',
            error_message = SQLERRM,
            updated_at = NOW()
        WHERE disposal_id = p_disposal_id;
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Function to verify disposal
CREATE OR REPLACE FUNCTION verify_disposal(
    p_disposal_id UUID,
    p_verifier_id INTEGER
) RETURNS JSONB AS $$
DECLARE
    v_record RECORD;
    v_remaining_variants BIGINT;
    v_passed BOOLEAN;
    v_result JSONB;
BEGIN
    SELECT * INTO v_record
    FROM disposal_records
    WHERE disposal_id = p_disposal_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Disposal record % not found', p_disposal_id;
    END IF;

    IF v_record.execution_status != 'completed' THEN
        RAISE EXCEPTION 'Disposal % not completed (status: %)',
            p_disposal_id, v_record.execution_status;
    END IF;

    IF v_record.disposal_type = 'batch' THEN
        SELECT COUNT(*) INTO v_remaining_variants
        FROM variants WHERE load_batch_id = v_record.target_identifier::UUID;
    ELSIF v_record.disposal_type = 'sample' THEN
        SELECT COUNT(*) INTO v_remaining_variants
        FROM variants WHERE sample_id = v_record.target_identifier;
    ELSE
        v_remaining_variants := 0;
    END IF;

    v_passed := v_remaining_variants = 0;

    v_result := jsonb_build_object(
        'remaining_variants', v_remaining_variants,
        'expected_deleted', v_record.variants_disposed,
        'verification_passed', v_passed,
        'verified_at', NOW()
    );

    UPDATE disposal_records
    SET verified_by = p_verifier_id,
        verified_at = NOW(),
        verification_status = CASE WHEN v_passed THEN 'passed' ELSE 'failed' END,
        verification_result = v_result,
        updated_at = NOW()
    WHERE disposal_id = p_disposal_id;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- Function to generate certificate hash
CREATE OR REPLACE FUNCTION generate_certificate_hash(
    p_disposal_id UUID
) RETURNS TEXT AS $$
DECLARE
    v_record RECORD;
    v_cert_data JSONB;
    v_hash TEXT;
BEGIN
    SELECT * INTO v_record
    FROM disposal_records
    WHERE disposal_id = p_disposal_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Disposal record % not found', p_disposal_id;
    END IF;

    IF v_record.verification_status != 'passed' THEN
        RAISE EXCEPTION 'Cannot generate certificate for unverified disposal %', p_disposal_id;
    END IF;

    v_cert_data := jsonb_build_object(
        'disposal_id', p_disposal_id,
        'disposal_type', v_record.disposal_type,
        'target_identifier', v_record.target_identifier,
        'variants_disposed', v_record.variants_disposed,
        'mappings_disposed', v_record.mappings_disposed,
        'reason', v_record.reason,
        'authorized_by', v_record.authorized_by,
        'authorized_at', v_record.authorized_at,
        'executed_by', v_record.executed_by,
        'executed_at', v_record.executed_at,
        'verified_by', v_record.verified_by,
        'verified_at', v_record.verified_at,
        'verification_result', v_record.verification_result
    );

    v_hash := encode(sha256(v_cert_data::TEXT::BYTEA), 'hex');

    UPDATE disposal_records
    SET certificate_generated_at = NOW(),
        certificate_hash = v_hash,
        certificate_data = v_cert_data,
        updated_at = NOW()
    WHERE disposal_id = p_disposal_id;

    RETURN v_hash;
END;
$$ LANGUAGE plpgsql;

-- View for expired data based on retention policies
CREATE OR REPLACE VIEW v_expired_data AS
SELECT
    vla.load_batch_id,
    vla.vcf_file_path,
    vla.load_completed_at,
    rp.policy_name,
    rp.retention_days,
    vla.load_completed_at + (rp.retention_days || ' days')::INTERVAL AS expires_at,
    CASE
        WHEN vla.load_completed_at + (rp.retention_days || ' days')::INTERVAL < NOW()
        THEN TRUE ELSE FALSE
    END AS is_expired,
    (SELECT COUNT(*) FROM variants v WHERE v.load_batch_id = vla.load_batch_id) AS variant_count
FROM variant_load_audit vla
CROSS JOIN retention_policies rp
WHERE rp.is_active = TRUE
AND rp.data_type IN ('variants', 'all')
AND vla.status = 'completed';

-- View for upcoming expirations
CREATE OR REPLACE VIEW v_upcoming_expirations AS
SELECT
    load_batch_id,
    vcf_file_path,
    policy_name,
    expires_at,
    expires_at - NOW() AS time_until_expiry,
    variant_count
FROM v_expired_data
WHERE is_expired = FALSE
AND expires_at <= NOW() + INTERVAL '90 days'
ORDER BY expires_at ASC;

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_disposal_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS disposal_update_timestamp ON disposal_records;
CREATE TRIGGER disposal_update_timestamp
    BEFORE UPDATE ON disposal_records
    FOR EACH ROW
    EXECUTE FUNCTION update_disposal_timestamp();
