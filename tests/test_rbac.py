"""Tests for Role-Based Access Control (RBAC).

HIPAA Reference: 164.312(a)(1) - Access Controls
"""

import time
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from vcf_pg_loader.auth.models import Permission, Role, UserRole
from vcf_pg_loader.auth.permissions import PermissionChecker, PermissionError
from vcf_pg_loader.auth.roles import RoleManager


class TestRoleModel:
    def test_role_from_db_row(self):
        row = {
            "role_id": 1,
            "role_name": "admin",
            "description": "Full access",
            "is_system_role": True,
            "created_at": datetime.now(UTC),
        }
        role = Role.from_db_row(row)
        assert role.role_id == 1
        assert role.role_name == "admin"
        assert role.description == "Full access"
        assert role.is_system_role is True

    def test_role_from_db_row_minimal(self):
        row = {
            "role_id": 2,
            "role_name": "custom",
        }
        role = Role.from_db_row(row)
        assert role.role_id == 2
        assert role.role_name == "custom"
        assert role.description is None
        assert role.is_system_role is False


class TestPermissionModel:
    def test_permission_from_db_row(self):
        row = {
            "permission_id": 1,
            "permission_name": "variants:read",
            "resource_type": "variant",
            "action": "read",
            "description": "Read variants",
        }
        perm = Permission.from_db_row(row)
        assert perm.permission_id == 1
        assert perm.permission_name == "variants:read"
        assert perm.resource_type == "variant"
        assert perm.action == "read"


class TestUserRoleModel:
    def test_user_role_from_db_row(self):
        row = {
            "user_id": 1,
            "role_id": 2,
            "role_name": "admin",
            "granted_by": 1,
            "granted_at": datetime.now(UTC),
            "expires_at": None,
        }
        ur = UserRole.from_db_row(row)
        assert ur.user_id == 1
        assert ur.role_id == 2
        assert ur.role_name == "admin"
        assert ur.is_expired() is False

    def test_user_role_expired(self):
        past = datetime.now(UTC) - timedelta(hours=1)
        row = {
            "user_id": 1,
            "role_id": 2,
            "role_name": "admin",
            "expires_at": past,
        }
        ur = UserRole.from_db_row(row)
        assert ur.is_expired() is True

    def test_user_role_not_expired(self):
        future = datetime.now(UTC) + timedelta(hours=1)
        row = {
            "user_id": 1,
            "role_id": 2,
            "role_name": "admin",
            "expires_at": future,
        }
        ur = UserRole.from_db_row(row)
        assert ur.is_expired() is False


class TestRoleManager:
    @pytest.fixture
    def manager(self):
        return RoleManager()

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_list_roles(self, manager, mock_conn):
        mock_conn.fetch.return_value = [
            {
                "role_id": 1,
                "role_name": "admin",
                "description": "Full access",
                "is_system_role": True,
                "created_at": datetime.now(UTC),
            },
            {
                "role_id": 2,
                "role_name": "reader",
                "description": "Read only",
                "is_system_role": True,
                "created_at": datetime.now(UTC),
            },
        ]

        roles = await manager.list_roles(mock_conn)
        assert len(roles) == 2
        assert roles[0].role_name == "admin"
        assert roles[1].role_name == "reader"

    @pytest.mark.asyncio
    async def test_get_role_found(self, manager, mock_conn):
        mock_conn.fetchrow.return_value = {
            "role_id": 1,
            "role_name": "admin",
            "description": "Full access",
            "is_system_role": True,
            "created_at": datetime.now(UTC),
        }

        role = await manager.get_role(mock_conn, "admin")
        assert role is not None
        assert role.role_name == "admin"

    @pytest.mark.asyncio
    async def test_get_role_not_found(self, manager, mock_conn):
        mock_conn.fetchrow.return_value = None
        role = await manager.get_role(mock_conn, "nonexistent")
        assert role is None

    @pytest.mark.asyncio
    async def test_assign_role_success(self, manager, mock_conn):
        mock_conn.fetchrow.side_effect = [
            {
                "role_id": 1,
                "role_name": "admin",
                "description": None,
                "is_system_role": True,
                "created_at": None,
            },
            None,
        ]
        mock_conn.execute.return_value = "INSERT 0 1"

        success, message = await manager.assign_role(mock_conn, 1, "admin", 2)
        assert success is True
        assert "assigned" in message.lower()

    @pytest.mark.asyncio
    async def test_assign_role_not_found(self, manager, mock_conn):
        mock_conn.fetchrow.return_value = None

        success, message = await manager.assign_role(mock_conn, 1, "nonexistent", 2)
        assert success is False
        assert "not found" in message.lower()

    @pytest.mark.asyncio
    async def test_assign_role_already_assigned(self, manager, mock_conn):
        mock_conn.fetchrow.side_effect = [
            {
                "role_id": 1,
                "role_name": "admin",
                "description": None,
                "is_system_role": True,
                "created_at": None,
            },
            {"exists": True},
        ]

        success, message = await manager.assign_role(mock_conn, 1, "admin", 2)
        assert success is False
        assert "already has role" in message.lower()

    @pytest.mark.asyncio
    async def test_revoke_role_success(self, manager, mock_conn):
        mock_conn.fetchrow.return_value = {
            "role_id": 1,
            "role_name": "admin",
            "description": None,
            "is_system_role": True,
            "created_at": None,
        }
        mock_conn.execute.return_value = "DELETE 1"

        success, message = await manager.revoke_role(mock_conn, 1, "admin", 2)
        assert success is True
        assert "revoked" in message.lower()

    @pytest.mark.asyncio
    async def test_revoke_role_not_assigned(self, manager, mock_conn):
        mock_conn.fetchrow.return_value = {
            "role_id": 1,
            "role_name": "admin",
            "description": None,
            "is_system_role": True,
            "created_at": None,
        }
        mock_conn.execute.return_value = "DELETE 0"

        success, message = await manager.revoke_role(mock_conn, 1, "admin", 2)
        assert success is False
        assert "does not have role" in message.lower()

    @pytest.mark.asyncio
    async def test_get_user_roles(self, manager, mock_conn):
        mock_conn.fetch.return_value = [
            {
                "user_id": 1,
                "role_id": 1,
                "role_name": "admin",
                "granted_by": 2,
                "granted_at": datetime.now(UTC),
                "expires_at": None,
            },
            {
                "user_id": 1,
                "role_id": 2,
                "role_name": "auditor",
                "granted_by": 2,
                "granted_at": datetime.now(UTC),
                "expires_at": None,
            },
        ]

        roles = await manager.get_user_roles(mock_conn, 1)
        assert len(roles) == 2
        assert roles[0].role_name == "admin"
        assert roles[1].role_name == "auditor"


class TestPermissionChecker:
    @pytest.fixture
    def checker(self):
        return PermissionChecker(cache_ttl=1.0)

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_list_permissions(self, checker, mock_conn):
        mock_conn.fetch.return_value = [
            {
                "permission_id": 1,
                "permission_name": "variants:read",
                "resource_type": "variant",
                "action": "read",
                "description": "Read variants",
            },
            {
                "permission_id": 2,
                "permission_name": "variants:write",
                "resource_type": "variant",
                "action": "write",
                "description": "Write variants",
            },
        ]

        perms = await checker.list_permissions(mock_conn)
        assert len(perms) == 2
        assert perms[0].permission_name == "variants:read"

    @pytest.mark.asyncio
    async def test_has_permission_true(self, checker, mock_conn):
        mock_conn.fetch.return_value = [
            {"permission_name": "variants:read"},
            {"permission_name": "variants:write"},
        ]

        has_perm = await checker.has_permission(mock_conn, 1, "variants:read")
        assert has_perm is True

    @pytest.mark.asyncio
    async def test_has_permission_false(self, checker, mock_conn):
        mock_conn.fetch.return_value = [
            {"permission_name": "variants:read"},
        ]

        has_perm = await checker.has_permission(mock_conn, 1, "variants:write")
        assert has_perm is False

    @pytest.mark.asyncio
    async def test_get_user_permissions(self, checker, mock_conn):
        mock_conn.fetch.return_value = [
            {"permission_name": "variants:read"},
            {"permission_name": "samples:read"},
        ]

        perms = await checker.get_user_permissions(mock_conn, 1)
        assert "variants:read" in perms
        assert "samples:read" in perms
        assert len(perms) == 2

    @pytest.mark.asyncio
    async def test_permission_caching(self, checker, mock_conn):
        mock_conn.fetch.return_value = [
            {"permission_name": "variants:read"},
        ]

        await checker.get_user_permissions(mock_conn, 1)
        await checker.get_user_permissions(mock_conn, 1)
        await checker.get_user_permissions(mock_conn, 1)

        assert mock_conn.fetch.call_count == 1

    @pytest.mark.asyncio
    async def test_cache_invalidation(self, checker, mock_conn):
        mock_conn.fetch.return_value = [
            {"permission_name": "variants:read"},
        ]

        await checker.get_user_permissions(mock_conn, 1)
        await checker.invalidate_cache(1)
        await checker.get_user_permissions(mock_conn, 1)

        assert mock_conn.fetch.call_count == 2

    @pytest.mark.asyncio
    async def test_cache_expiry(self, checker, mock_conn):
        mock_conn.fetch.return_value = [
            {"permission_name": "variants:read"},
        ]

        await checker.get_user_permissions(mock_conn, 1)
        time.sleep(1.1)
        await checker.get_user_permissions(mock_conn, 1)

        assert mock_conn.fetch.call_count == 2

    @pytest.mark.asyncio
    async def test_check_permission_granted(self, checker, mock_conn):
        mock_conn.fetch.return_value = [
            {"permission_name": "variants:read"},
        ]

        granted, message = await checker.check_permission(mock_conn, 1, "variants:read")
        assert granted is True
        assert "granted" in message.lower()

    @pytest.mark.asyncio
    async def test_check_permission_denied(self, checker, mock_conn):
        mock_conn.fetch.return_value = []

        granted, message = await checker.check_permission(mock_conn, 1, "variants:read")
        assert granted is False
        assert "denied" in message.lower()


class TestPermissionDecorator:
    @pytest.mark.asyncio
    async def test_require_permission_granted(self):
        checker = PermissionChecker()
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [{"permission_name": "variants:read"}]

        class MockService:
            _conn = mock_conn
            _user_id = 1

            @checker.require_permission("variants:read")
            async def do_something(self):
                return "success"

        service = MockService()
        result = await service.do_something()
        assert result == "success"

    @pytest.mark.asyncio
    async def test_require_permission_denied(self):
        checker = PermissionChecker()
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []

        class MockService:
            _conn = mock_conn
            _user_id = 1

            @checker.require_permission("variants:delete")
            async def do_something(self):
                return "success"

        service = MockService()
        with pytest.raises(PermissionError) as exc_info:
            await service.do_something()
        assert "variants:delete" in str(exc_info.value)
