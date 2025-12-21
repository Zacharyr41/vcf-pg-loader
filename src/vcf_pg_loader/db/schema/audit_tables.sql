-- HIPAA-Compliant Audit Logging Schema
-- HIPAA Reference: 164.312(b) - Audit Controls
-- 6-year minimum retention requirement

-- Audit event types enum
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'audit_event_type') THEN
        CREATE TYPE audit_event_type AS ENUM (
            'AUTH_LOGIN',
            'AUTH_LOGOUT',
            'AUTH_FAILED',
            'DATA_READ',
            'DATA_WRITE',
            'DATA_DELETE',
            'DATA_EXPORT',
            'SCHEMA_CHANGE',
            'CONFIG_CHANGE',
            'PERMISSION_CHANGE',
            'PHI_ACCESS',
            'EMERGENCY_ACCESS'
        );
    END IF;
END$$;

-- Main audit log table (partitioned by date for retention management)
CREATE TABLE IF NOT EXISTS hipaa_audit_log (
    audit_id BIGSERIAL,
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type audit_event_type NOT NULL,

    -- WHO (user_id FK added when users table exists)
    user_id INTEGER,
    user_name TEXT NOT NULL,
    session_id UUID,

    -- WHAT
    action TEXT NOT NULL,
    resource_type TEXT,
    resource_id TEXT,

    -- WHERE
    client_ip INET,
    client_hostname TEXT,
    application_name TEXT,

    -- OUTCOME
    success BOOLEAN NOT NULL,
    error_message TEXT,

    -- DETAILS (NEVER include PHI in this field)
    details JSONB DEFAULT '{}',

    -- Hash chain for integrity verification
    previous_hash TEXT,
    entry_hash TEXT,

    -- Partition key (set by trigger on insert)
    created_date DATE NOT NULL DEFAULT CURRENT_DATE,

    PRIMARY KEY (created_date, audit_id)
) PARTITION BY RANGE (created_date);

-- Trigger to ensure created_date matches event_time
CREATE OR REPLACE FUNCTION set_audit_created_date()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_date := (NEW.event_time AT TIME ZONE 'UTC')::date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_set_created_date ON hipaa_audit_log;
CREATE TRIGGER audit_set_created_date
    BEFORE INSERT ON hipaa_audit_log
    FOR EACH ROW
    EXECUTE FUNCTION set_audit_created_date();

-- Create index on audit_id within partitions for uniqueness
CREATE INDEX IF NOT EXISTS idx_audit_log_audit_id ON hipaa_audit_log (audit_id);

-- Indexes for common compliance queries
CREATE INDEX IF NOT EXISTS idx_audit_user_time
    ON hipaa_audit_log (user_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event_type
    ON hipaa_audit_log (event_type, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_audit_resource
    ON hipaa_audit_log (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_audit_session
    ON hipaa_audit_log (session_id) WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_failed_auth
    ON hipaa_audit_log (event_time DESC)
    WHERE event_type = 'AUTH_FAILED';

-- Immutability trigger: prevent UPDATE/DELETE on audit records
CREATE OR REPLACE FUNCTION prevent_audit_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'HIPAA Compliance: Audit log records cannot be modified or deleted. '
        'Audit ID: %, Event Time: %. '
        'This restriction is required by HIPAA 164.312(b).',
        OLD.audit_id, OLD.event_time;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS audit_immutability ON hipaa_audit_log;
CREATE TRIGGER audit_immutability
    BEFORE UPDATE OR DELETE ON hipaa_audit_log
    FOR EACH ROW
    EXECUTE FUNCTION prevent_audit_modification();

-- Function to create monthly partition
CREATE OR REPLACE FUNCTION create_audit_partition(partition_date DATE)
RETURNS TEXT AS $$
DECLARE
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    start_date := date_trunc('month', partition_date)::date;
    end_date := (date_trunc('month', partition_date) + interval '1 month')::date;
    partition_name := 'hipaa_audit_log_' || to_char(start_date, 'YYYY_MM');

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = partition_name AND n.nspname = 'public'
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF hipaa_audit_log
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
        RETURN partition_name;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to create partitions for a date range
CREATE OR REPLACE FUNCTION create_audit_partitions_range(
    start_date DATE,
    months_ahead INTEGER DEFAULT 12
)
RETURNS TABLE(partition_name TEXT, created BOOLEAN) AS $$
DECLARE
    current_month DATE;
    result_name TEXT;
BEGIN
    current_month := date_trunc('month', start_date)::date;

    FOR i IN 0..months_ahead LOOP
        result_name := create_audit_partition(current_month);
        partition_name := 'hipaa_audit_log_' || to_char(current_month, 'YYYY_MM');
        created := result_name IS NOT NULL;
        RETURN NEXT;
        current_month := current_month + interval '1 month';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to archive old partitions (detach, not delete - HIPAA 6-year retention)
CREATE OR REPLACE FUNCTION archive_audit_partition(partition_date DATE)
RETURNS TEXT AS $$
DECLARE
    partition_name TEXT;
    archive_name TEXT;
BEGIN
    partition_name := 'hipaa_audit_log_' || to_char(partition_date, 'YYYY_MM');
    archive_name := partition_name || '_archived';

    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = partition_name AND n.nspname = 'public'
    ) THEN
        EXECUTE format(
            'ALTER TABLE hipaa_audit_log DETACH PARTITION %I',
            partition_name
        );
        EXECUTE format(
            'ALTER TABLE %I RENAME TO %I',
            partition_name, archive_name
        );
        RETURN archive_name;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- View: Activity summary by user
CREATE OR REPLACE VIEW v_audit_summary_by_user AS
SELECT
    user_id,
    user_name,
    COUNT(*) as total_events,
    COUNT(*) FILTER (WHERE event_type = 'AUTH_LOGIN') as logins,
    COUNT(*) FILTER (WHERE event_type = 'AUTH_FAILED') as failed_logins,
    COUNT(*) FILTER (WHERE event_type = 'DATA_READ') as data_reads,
    COUNT(*) FILTER (WHERE event_type = 'DATA_WRITE') as data_writes,
    COUNT(*) FILTER (WHERE event_type = 'DATA_DELETE') as data_deletes,
    COUNT(*) FILTER (WHERE event_type = 'PHI_ACCESS') as phi_accesses,
    MIN(event_time) as first_activity,
    MAX(event_time) as last_activity
FROM hipaa_audit_log
GROUP BY user_id, user_name;

-- View: All PHI access events (for compliance review)
CREATE OR REPLACE VIEW v_audit_phi_access AS
SELECT
    audit_id,
    event_time,
    user_id,
    user_name,
    session_id,
    action,
    resource_type,
    resource_id,
    client_ip,
    success,
    details
FROM hipaa_audit_log
WHERE event_type = 'PHI_ACCESS'
ORDER BY event_time DESC;

-- View: Failed authentication attempts (security monitoring)
CREATE OR REPLACE VIEW v_audit_failed_auth AS
SELECT
    audit_id,
    event_time,
    user_name,
    client_ip,
    client_hostname,
    application_name,
    error_message,
    details
FROM hipaa_audit_log
WHERE event_type = 'AUTH_FAILED'
ORDER BY event_time DESC;

-- View: Recent security events for dashboard
CREATE OR REPLACE VIEW v_audit_security_events AS
SELECT
    audit_id,
    event_time,
    event_type,
    user_name,
    action,
    client_ip,
    success,
    error_message
FROM hipaa_audit_log
WHERE event_type IN (
    'AUTH_LOGIN', 'AUTH_LOGOUT', 'AUTH_FAILED',
    'PERMISSION_CHANGE', 'EMERGENCY_ACCESS'
)
ORDER BY event_time DESC;
