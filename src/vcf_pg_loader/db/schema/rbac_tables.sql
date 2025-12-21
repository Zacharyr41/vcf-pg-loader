-- HIPAA-Compliant Role-Based Access Control Schema
-- HIPAA Reference: 164.312(a)(1) - Access Controls
-- Implements minimum necessary access principle

-- Predefined roles
CREATE TABLE IF NOT EXISTS roles (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    is_system_role BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_roles_name ON roles (role_name);

-- Granular permissions
CREATE TABLE IF NOT EXISTS permissions (
    permission_id SERIAL PRIMARY KEY,
    permission_name VARCHAR(100) UNIQUE NOT NULL,
    resource_type VARCHAR(50) NOT NULL,
    action VARCHAR(20) NOT NULL,
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_permissions_name ON permissions (permission_name);
CREATE INDEX IF NOT EXISTS idx_permissions_resource ON permissions (resource_type);

-- Role-permission mapping
CREATE TABLE IF NOT EXISTS role_permissions (
    role_id INTEGER REFERENCES roles(role_id) ON DELETE CASCADE,
    permission_id INTEGER REFERENCES permissions(permission_id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

CREATE INDEX IF NOT EXISTS idx_role_permissions_role ON role_permissions (role_id);

-- User-role mapping with expiry support
CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    role_id INTEGER REFERENCES roles(role_id) ON DELETE CASCADE,
    granted_by INTEGER REFERENCES users(user_id),
    granted_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (user_id, role_id)
);

CREATE INDEX IF NOT EXISTS idx_user_roles_user ON user_roles (user_id);
CREATE INDEX IF NOT EXISTS idx_user_roles_expires ON user_roles (expires_at) WHERE expires_at IS NOT NULL;

-- Audit table for role changes (HIPAA compliance)
CREATE TABLE IF NOT EXISTS role_audit (
    audit_id SERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ DEFAULT NOW(),
    event_type VARCHAR(20) NOT NULL,
    target_user_id INTEGER REFERENCES users(user_id),
    role_id INTEGER REFERENCES roles(role_id),
    performed_by INTEGER REFERENCES users(user_id),
    details JSONB
);

CREATE INDEX IF NOT EXISTS idx_role_audit_time ON role_audit (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_role_audit_user ON role_audit (target_user_id);

-- Insert default roles
INSERT INTO roles (role_name, description, is_system_role) VALUES
    ('admin', 'Full system access', true),
    ('data_loader', 'Can load VCF files', true),
    ('data_reader', 'Can query variant data', true),
    ('auditor', 'Can view audit logs', true),
    ('user_manager', 'Can manage user accounts', true),
    ('phi_admin', 'PHI vault access - sample ID re-identification', true)
ON CONFLICT (role_name) DO NOTHING;

-- Insert default permissions
INSERT INTO permissions (permission_name, resource_type, action, description) VALUES
    ('variants:read', 'variant', 'read', 'Read variant data'),
    ('variants:write', 'variant', 'write', 'Write/load variant data'),
    ('variants:delete', 'variant', 'delete', 'Delete variant data'),
    ('samples:read', 'sample', 'read', 'Read sample data'),
    ('audit:read', 'audit', 'read', 'Read audit logs'),
    ('users:read', 'user', 'read', 'View user accounts'),
    ('users:write', 'user', 'write', 'Modify user accounts'),
    ('users:admin', 'user', 'admin', 'Full user administration'),
    ('phi:lookup', 'phi', 'read', 'Reverse lookup sample IDs from anonymous IDs'),
    ('phi:export', 'phi', 'read', 'Export PHI mappings'),
    ('phi:stats', 'phi', 'read', 'View PHI anonymization statistics')
ON CONFLICT (permission_name) DO NOTHING;

-- Map permissions to roles
-- Admin gets all permissions
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM roles r, permissions p
WHERE r.role_name = 'admin'
ON CONFLICT DO NOTHING;

-- Data loader gets variants:write and variants:read
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM roles r, permissions p
WHERE r.role_name = 'data_loader' AND p.permission_name IN ('variants:write', 'variants:read', 'samples:read')
ON CONFLICT DO NOTHING;

-- Data reader gets variants:read
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM roles r, permissions p
WHERE r.role_name = 'data_reader' AND p.permission_name IN ('variants:read', 'samples:read')
ON CONFLICT DO NOTHING;

-- Auditor gets audit:read
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM roles r, permissions p
WHERE r.role_name = 'auditor' AND p.permission_name = 'audit:read'
ON CONFLICT DO NOTHING;

-- User manager gets user permissions
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM roles r, permissions p
WHERE r.role_name = 'user_manager' AND p.permission_name IN ('users:read', 'users:write')
ON CONFLICT DO NOTHING;

-- PHI admin gets phi permissions
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM roles r, permissions p
WHERE r.role_name = 'phi_admin' AND p.permission_name IN ('phi:lookup', 'phi:export', 'phi:stats')
ON CONFLICT DO NOTHING;

-- Grant permissions to auth_admin role
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'auth_admin') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON roles TO auth_admin;
        GRANT SELECT, INSERT, UPDATE, DELETE ON permissions TO auth_admin;
        GRANT SELECT, INSERT, DELETE ON role_permissions TO auth_admin;
        GRANT SELECT, INSERT, DELETE ON user_roles TO auth_admin;
        GRANT SELECT, INSERT ON role_audit TO auth_admin;
        GRANT USAGE, SELECT ON SEQUENCE roles_role_id_seq TO auth_admin;
        GRANT USAGE, SELECT ON SEQUENCE permissions_permission_id_seq TO auth_admin;
        GRANT USAGE, SELECT ON SEQUENCE role_audit_audit_id_seq TO auth_admin;
    END IF;
END$$;

-- Function to clean up expired role assignments
CREATE OR REPLACE FUNCTION cleanup_expired_roles()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM user_roles WHERE expires_at IS NOT NULL AND expires_at < NOW();
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
