"""HIPAA-compliant authentication models.

HIPAA Reference: 164.312(d) - Person or Entity Authentication
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from uuid import UUID


class AuthStatus(Enum):
    SUCCESS = "success"
    INVALID_CREDENTIALS = "invalid_credentials"
    ACCOUNT_LOCKED = "account_locked"
    ACCOUNT_DISABLED = "account_disabled"
    PASSWORD_EXPIRED = "password_expired"
    MFA_REQUIRED = "mfa_required"
    SESSION_EXPIRED = "session_expired"


@dataclass
class User:
    user_id: int
    username: str
    email: str | None = None
    is_active: bool = True
    is_locked: bool = False
    failed_login_attempts: int = 0
    locked_until: datetime | None = None
    password_changed_at: datetime | None = None
    password_expires_at: datetime | None = None
    must_change_password: bool = False
    created_at: datetime | None = None
    created_by: int | None = None
    last_login_at: datetime | None = None
    mfa_enabled: bool = False

    @classmethod
    def from_db_row(cls, row: dict) -> "User":
        return cls(
            user_id=row["user_id"],
            username=row["username"],
            email=row.get("email"),
            is_active=row.get("is_active", True),
            is_locked=row.get("is_locked", False),
            failed_login_attempts=row.get("failed_login_attempts", 0),
            locked_until=row.get("locked_until"),
            password_changed_at=row.get("password_changed_at"),
            password_expires_at=row.get("password_expires_at"),
            must_change_password=row.get("must_change_password", False),
            created_at=row.get("created_at"),
            created_by=row.get("created_by"),
            last_login_at=row.get("last_login_at"),
            mfa_enabled=row.get("mfa_enabled", False),
        )


@dataclass
class Session:
    session_id: UUID
    user_id: int
    username: str
    created_at: datetime
    expires_at: datetime
    last_activity_at: datetime | None = None
    client_ip: str | None = None
    client_hostname: str | None = None

    def is_expired(self) -> bool:
        from datetime import UTC

        return datetime.now(UTC) > self.expires_at


@dataclass
class AuthResult:
    status: AuthStatus
    user: User | None = None
    session: Session | None = None
    token: str | None = None
    message: str | None = None


@dataclass
class PasswordPolicy:
    min_length: int = 12
    require_uppercase: bool = False
    require_lowercase: bool = False
    require_digit: bool = False
    require_special: bool = False
    history_count: int = 12
    max_age_days: int | None = None
    lockout_threshold: int = 5
    lockout_duration_minutes: int = 30

    def validate(self, password: str) -> list[str]:
        errors = []

        if len(password) < self.min_length:
            errors.append(f"Password must be at least {self.min_length} characters")

        if self.require_uppercase and not any(c.isupper() for c in password):
            errors.append("Password must contain at least one uppercase letter")

        if self.require_lowercase and not any(c.islower() for c in password):
            errors.append("Password must contain at least one lowercase letter")

        if self.require_digit and not any(c.isdigit() for c in password):
            errors.append("Password must contain at least one digit")

        if self.require_special:
            special = set("!@#$%^&*()_+-=[]{}|;:,.<>?")
            if not any(c in special for c in password):
                errors.append("Password must contain at least one special character")

        return errors


@dataclass
class TokenPayload:
    session_id: str
    user_id: int
    username: str
    exp: int
    iat: int
    issued_at: datetime = field(default_factory=datetime.now)


@dataclass
class Role:
    role_id: int
    role_name: str
    description: str | None = None
    is_system_role: bool = False
    created_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict) -> "Role":
        return cls(
            role_id=row["role_id"],
            role_name=row["role_name"],
            description=row.get("description"),
            is_system_role=row.get("is_system_role", False),
            created_at=row.get("created_at"),
        )


@dataclass
class Permission:
    permission_id: int
    permission_name: str
    resource_type: str
    action: str
    description: str | None = None

    @classmethod
    def from_db_row(cls, row: dict) -> "Permission":
        return cls(
            permission_id=row["permission_id"],
            permission_name=row["permission_name"],
            resource_type=row["resource_type"],
            action=row["action"],
            description=row.get("description"),
        )


@dataclass
class UserRole:
    user_id: int
    role_id: int
    role_name: str
    granted_by: int | None = None
    granted_at: datetime | None = None
    expires_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict) -> "UserRole":
        return cls(
            user_id=row["user_id"],
            role_id=row["role_id"],
            role_name=row["role_name"],
            granted_by=row.get("granted_by"),
            granted_at=row.get("granted_at"),
            expires_at=row.get("expires_at"),
        )

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        from datetime import UTC

        return datetime.now(UTC) > self.expires_at
