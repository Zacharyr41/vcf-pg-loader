"""HIPAA-compliant user authentication and authorization.

HIPAA Reference: 164.312(d) - Person or Entity Authentication
HIPAA Reference: 164.312(a)(1) - Access Controls
HIPAA Reference: 164.312(a)(2)(iii) - Automatic logoff
"""

from .authentication import Authenticator
from .models import (
    AuthResult,
    AuthStatus,
    PasswordPolicy,
    Permission,
    Role,
    Session,
    User,
    UserRole,
)
from .permissions import PermissionChecker, PermissionError
from .roles import RoleManager
from .schema import AuthSchemaManager
from .session import SessionStorage
from .session_manager import SessionConfig, SessionManager
from .users import UserManager

__all__ = [
    "Authenticator",
    "AuthResult",
    "AuthSchemaManager",
    "AuthStatus",
    "PasswordPolicy",
    "Permission",
    "PermissionChecker",
    "PermissionError",
    "Role",
    "RoleManager",
    "Session",
    "SessionConfig",
    "SessionManager",
    "SessionStorage",
    "User",
    "UserManager",
    "UserRole",
]
