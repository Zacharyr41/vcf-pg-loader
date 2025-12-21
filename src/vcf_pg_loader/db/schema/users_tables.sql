-- HIPAA-Compliant User Authentication Schema
-- HIPAA Reference: 164.312(d) - Person or Entity Authentication
-- Unique user identification for all PHI access

-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE,
    password_hash TEXT NOT NULL,

    -- Status
    is_active BOOLEAN DEFAULT true,
    is_locked BOOLEAN DEFAULT false,
    failed_login_attempts INTEGER DEFAULT 0,
    locked_until TIMESTAMPTZ,

    -- Password policy
    password_changed_at TIMESTAMPTZ DEFAULT NOW(),
    password_expires_at TIMESTAMPTZ,
    must_change_password BOOLEAN DEFAULT false,

    -- Audit
    created_at TIMESTAMPTZ DEFAULT NOW(),
    created_by INTEGER REFERENCES users(user_id),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_login_at TIMESTAMPTZ,

    -- MFA (optional)
    mfa_enabled BOOLEAN DEFAULT false,
    mfa_secret TEXT
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users (username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users (email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_active ON users (is_active) WHERE is_active = true;

-- Password history for reuse prevention
CREATE TABLE IF NOT EXISTS password_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_password_history_user ON password_history (user_id, created_at DESC);

-- Active sessions table
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id UUID PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL,

    -- Session metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_activity_at TIMESTAMPTZ DEFAULT NOW(),

    -- Client info for audit
    client_ip INET,
    client_hostname TEXT,
    application_name TEXT DEFAULT 'vcf-pg-loader'
);

CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON user_sessions (expires_at);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON user_sessions (token_hash);

-- Trigger to update updated_at on users table
CREATE OR REPLACE FUNCTION update_users_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS users_updated_at ON users;
CREATE TRIGGER users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_users_updated_at();

-- Function to clean expired sessions
CREATE OR REPLACE FUNCTION cleanup_expired_sessions()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM user_sessions WHERE expires_at < NOW();
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Create auth roles if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'auth_admin') THEN
        CREATE ROLE auth_admin;
    END IF;
END$$;

GRANT SELECT, INSERT, UPDATE ON users TO auth_admin;
GRANT SELECT, INSERT ON password_history TO auth_admin;
GRANT SELECT, INSERT, DELETE ON user_sessions TO auth_admin;
GRANT USAGE, SELECT ON SEQUENCE users_user_id_seq TO auth_admin;
GRANT USAGE, SELECT ON SEQUENCE password_history_id_seq TO auth_admin;
