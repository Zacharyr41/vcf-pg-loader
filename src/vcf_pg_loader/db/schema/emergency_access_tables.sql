-- HIPAA-Compliant Emergency Access (Break-Glass) Schema
--
-- HIPAA Citation: 45 CFR 164.312(a)(2)(ii) - REQUIRED specification
-- "Establish (and implement as needed) procedures for obtaining necessary
-- electronic protected health information during an emergency."
--
-- This schema implements time-limited emergency access with:
-- - Mandatory justification
-- - Automatic expiration
-- - Enhanced audit logging
-- - Post-incident review requirements

CREATE TABLE IF NOT EXISTS emergency_access_tokens (
    token_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id INTEGER NOT NULL REFERENCES users(user_id),

    -- Justification is REQUIRED per HIPAA emergency access procedures
    justification TEXT NOT NULL CHECK (length(justification) >= 20),
    emergency_type VARCHAR(50) NOT NULL CHECK (emergency_type IN (
        'patient_emergency',
        'system_emergency',
        'disaster_recovery',
        'legal_requirement',
        'other'
    )),

    -- Time limits - emergency access must be time-bounded
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    revoked_by INTEGER REFERENCES users(user_id),
    revocation_reason TEXT,

    -- Approval chain
    granted_by INTEGER REFERENCES users(user_id),
    requires_review BOOLEAN NOT NULL DEFAULT true,
    reviewed_at TIMESTAMPTZ,
    reviewed_by INTEGER REFERENCES users(user_id),
    review_notes TEXT,

    -- Access scope - what resources are accessible
    access_scope JSONB NOT NULL DEFAULT '{"all_phi": false, "resources": []}',

    -- Client info for audit
    client_ip INET,
    client_hostname TEXT,

    CONSTRAINT valid_expiry CHECK (expires_at > granted_at),
    CONSTRAINT max_duration CHECK (expires_at <= granted_at + INTERVAL '24 hours')
);

CREATE INDEX IF NOT EXISTS idx_emergency_tokens_user ON emergency_access_tokens (user_id);
CREATE INDEX IF NOT EXISTS idx_emergency_tokens_active
    ON emergency_access_tokens (user_id, expires_at)
    WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_emergency_tokens_pending_review
    ON emergency_access_tokens (granted_at)
    WHERE requires_review = true AND reviewed_at IS NULL;

-- Emergency access audit log (separate from main audit for enhanced tracking)
-- 45 CFR 164.312(b): "record and examine activity in information systems"
CREATE TABLE IF NOT EXISTS emergency_access_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    token_id UUID NOT NULL REFERENCES emergency_access_tokens(token_id),
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type VARCHAR(50) NOT NULL CHECK (event_type IN (
        'token_granted',
        'token_used',
        'token_expired',
        'token_revoked',
        'access_denied',
        'review_completed'
    )),
    user_id INTEGER REFERENCES users(user_id),
    resource_type TEXT,
    resource_id TEXT,
    action TEXT,
    success BOOLEAN NOT NULL DEFAULT true,
    details JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_emergency_audit_token ON emergency_access_audit (token_id);
CREATE INDEX IF NOT EXISTS idx_emergency_audit_time ON emergency_access_audit (event_time DESC);

-- Function to grant emergency access
-- 45 CFR 164.312(a)(2)(ii): Procedure for obtaining ePHI during emergency
CREATE OR REPLACE FUNCTION grant_emergency_access(
    p_user_id INTEGER,
    p_justification TEXT,
    p_emergency_type VARCHAR(50),
    p_duration_minutes INTEGER DEFAULT 60,
    p_granted_by INTEGER DEFAULT NULL,
    p_access_scope JSONB DEFAULT '{"all_phi": false, "resources": []}',
    p_client_ip INET DEFAULT NULL,
    p_client_hostname TEXT DEFAULT NULL
)
RETURNS UUID AS $$
DECLARE
    v_token_id UUID;
    v_expires_at TIMESTAMPTZ;
    v_max_minutes INTEGER := 1440; -- 24 hours max
BEGIN
    -- Validate duration
    IF p_duration_minutes > v_max_minutes THEN
        RAISE EXCEPTION 'Emergency access duration cannot exceed 24 hours (requested: % minutes)',
            p_duration_minutes;
    END IF;

    IF p_duration_minutes < 1 THEN
        RAISE EXCEPTION 'Emergency access duration must be at least 1 minute';
    END IF;

    -- Calculate expiration
    v_expires_at := NOW() + (p_duration_minutes || ' minutes')::INTERVAL;

    -- Create the token
    INSERT INTO emergency_access_tokens (
        user_id, justification, emergency_type, expires_at,
        granted_by, access_scope, client_ip, client_hostname
    ) VALUES (
        p_user_id, p_justification, p_emergency_type, v_expires_at,
        p_granted_by, p_access_scope, p_client_ip, p_client_hostname
    ) RETURNING token_id INTO v_token_id;

    -- Log the grant event
    INSERT INTO emergency_access_audit (
        token_id, event_type, user_id, details
    ) VALUES (
        v_token_id, 'token_granted', p_user_id,
        jsonb_build_object(
            'justification', p_justification,
            'emergency_type', p_emergency_type,
            'duration_minutes', p_duration_minutes,
            'granted_by', p_granted_by,
            'access_scope', p_access_scope
        )
    );

    RETURN v_token_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to validate emergency access token
CREATE OR REPLACE FUNCTION validate_emergency_access(
    p_token_id UUID,
    p_resource_type TEXT DEFAULT NULL,
    p_resource_id TEXT DEFAULT NULL
)
RETURNS TABLE (
    is_valid BOOLEAN,
    user_id INTEGER,
    access_scope JSONB,
    expires_at TIMESTAMPTZ,
    message TEXT
) AS $$
DECLARE
    v_token RECORD;
BEGIN
    SELECT t.* INTO v_token
    FROM emergency_access_tokens t
    WHERE t.token_id = p_token_id;

    IF v_token IS NULL THEN
        RETURN QUERY SELECT false, NULL::INTEGER, NULL::JSONB, NULL::TIMESTAMPTZ,
            'Token not found'::TEXT;
        RETURN;
    END IF;

    IF v_token.revoked_at IS NOT NULL THEN
        -- Log denied access attempt
        INSERT INTO emergency_access_audit (
            token_id, event_type, user_id, resource_type, resource_id,
            success, details
        ) VALUES (
            p_token_id, 'access_denied', v_token.user_id, p_resource_type, p_resource_id,
            false, '{"reason": "token_revoked"}'::jsonb
        );

        RETURN QUERY SELECT false, v_token.user_id, NULL::JSONB, NULL::TIMESTAMPTZ,
            'Token has been revoked'::TEXT;
        RETURN;
    END IF;

    IF NOW() > v_token.expires_at THEN
        -- Log denied access attempt
        INSERT INTO emergency_access_audit (
            token_id, event_type, user_id, resource_type, resource_id,
            success, details
        ) VALUES (
            p_token_id, 'access_denied', v_token.user_id, p_resource_type, p_resource_id,
            false, '{"reason": "token_expired"}'::jsonb
        );

        RETURN QUERY SELECT false, v_token.user_id, NULL::JSONB, v_token.expires_at,
            'Token has expired'::TEXT;
        RETURN;
    END IF;

    -- Log successful token usage
    INSERT INTO emergency_access_audit (
        token_id, event_type, user_id, resource_type, resource_id, success
    ) VALUES (
        p_token_id, 'token_used', v_token.user_id, p_resource_type, p_resource_id, true
    );

    RETURN QUERY SELECT true, v_token.user_id, v_token.access_scope, v_token.expires_at,
        'Access granted'::TEXT;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to revoke emergency access
CREATE OR REPLACE FUNCTION revoke_emergency_access(
    p_token_id UUID,
    p_revoked_by INTEGER,
    p_reason TEXT
)
RETURNS BOOLEAN AS $$
DECLARE
    v_updated BOOLEAN;
BEGIN
    UPDATE emergency_access_tokens
    SET revoked_at = NOW(),
        revoked_by = p_revoked_by,
        revocation_reason = p_reason
    WHERE token_id = p_token_id
      AND revoked_at IS NULL;

    GET DIAGNOSTICS v_updated = ROW_COUNT;

    IF v_updated THEN
        INSERT INTO emergency_access_audit (
            token_id, event_type, user_id, details
        ) VALUES (
            p_token_id, 'token_revoked', p_revoked_by,
            jsonb_build_object('reason', p_reason)
        );
    END IF;

    RETURN v_updated > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to complete post-incident review (HIPAA requires review of emergency access)
CREATE OR REPLACE FUNCTION complete_emergency_review(
    p_token_id UUID,
    p_reviewed_by INTEGER,
    p_review_notes TEXT
)
RETURNS BOOLEAN AS $$
DECLARE
    v_updated BOOLEAN;
BEGIN
    UPDATE emergency_access_tokens
    SET reviewed_at = NOW(),
        reviewed_by = p_reviewed_by,
        review_notes = p_review_notes
    WHERE token_id = p_token_id
      AND requires_review = true
      AND reviewed_at IS NULL;

    GET DIAGNOSTICS v_updated = ROW_COUNT;

    IF v_updated THEN
        INSERT INTO emergency_access_audit (
            token_id, event_type, user_id, details
        ) VALUES (
            p_token_id, 'review_completed', p_reviewed_by,
            jsonb_build_object('notes', p_review_notes)
        );
    END IF;

    RETURN v_updated > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- View: Active emergency access tokens
CREATE OR REPLACE VIEW v_active_emergency_access AS
SELECT
    t.token_id,
    t.user_id,
    u.username,
    t.justification,
    t.emergency_type,
    t.granted_at,
    t.expires_at,
    t.access_scope,
    EXTRACT(EPOCH FROM (t.expires_at - NOW())) / 60 as minutes_remaining
FROM emergency_access_tokens t
JOIN users u ON u.user_id = t.user_id
WHERE t.revoked_at IS NULL
  AND t.expires_at > NOW();

-- View: Pending emergency access reviews
CREATE OR REPLACE VIEW v_pending_emergency_reviews AS
SELECT
    t.token_id,
    t.user_id,
    u.username,
    t.justification,
    t.emergency_type,
    t.granted_at,
    t.expires_at,
    COALESCE(t.revoked_at, t.expires_at) as ended_at,
    gu.username as granted_by_username,
    COUNT(a.audit_id) FILTER (WHERE a.event_type = 'token_used') as access_count
FROM emergency_access_tokens t
JOIN users u ON u.user_id = t.user_id
LEFT JOIN users gu ON gu.user_id = t.granted_by
LEFT JOIN emergency_access_audit a ON a.token_id = t.token_id
WHERE t.requires_review = true
  AND t.reviewed_at IS NULL
  AND (t.revoked_at IS NOT NULL OR t.expires_at < NOW())
GROUP BY t.token_id, t.user_id, u.username, t.justification, t.emergency_type,
         t.granted_at, t.expires_at, t.revoked_at, gu.username;

-- Create roles for emergency access management
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'emergency_grantor') THEN
        CREATE ROLE emergency_grantor;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'emergency_reviewer') THEN
        CREATE ROLE emergency_reviewer;
    END IF;
END$$;

GRANT SELECT ON emergency_access_tokens TO emergency_reviewer;
GRANT SELECT ON emergency_access_audit TO emergency_reviewer;
GRANT SELECT ON v_active_emergency_access TO emergency_reviewer;
GRANT SELECT ON v_pending_emergency_reviews TO emergency_reviewer;
GRANT EXECUTE ON FUNCTION complete_emergency_review TO emergency_reviewer;

GRANT EXECUTE ON FUNCTION grant_emergency_access TO emergency_grantor;
GRANT EXECUTE ON FUNCTION revoke_emergency_access TO emergency_grantor;
GRANT SELECT ON v_active_emergency_access TO emergency_grantor;
