"""Tests for HIPAA-compliant session management.

HIPAA Reference: 164.312(a)(2)(iii) - Automatic logoff
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from vcf_pg_loader.auth.models import Session
from vcf_pg_loader.auth.session_manager import SessionConfig, SessionManager


class TestSessionConfig:
    def test_default_config(self):
        config = SessionConfig()
        assert config.inactivity_timeout_minutes == 30
        assert config.absolute_timeout_hours == 8
        assert config.max_concurrent_sessions == 3
        assert config.extend_on_activity is True

    def test_custom_config(self):
        config = SessionConfig(
            inactivity_timeout_minutes=15,
            absolute_timeout_hours=4,
            max_concurrent_sessions=1,
            extend_on_activity=False,
        )
        assert config.inactivity_timeout_minutes == 15
        assert config.absolute_timeout_hours == 4
        assert config.max_concurrent_sessions == 1
        assert config.extend_on_activity is False


class TestSessionManager:
    @pytest.fixture
    def config(self):
        return SessionConfig(
            inactivity_timeout_minutes=30,
            absolute_timeout_hours=8,
            max_concurrent_sessions=3,
        )

    @pytest.fixture
    def manager(self, config):
        return SessionManager(config)

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    @pytest.fixture
    def valid_session_row(self):
        now = datetime.now(UTC)
        return {
            "session_id": uuid4(),
            "user_id": 1,
            "username": "testuser",
            "created_at": now - timedelta(hours=1),
            "expires_at": now + timedelta(hours=7),
            "last_activity_at": now - timedelta(minutes=5),
            "client_ip": "192.168.1.1",
            "client_hostname": "test-host",
            "is_active": True,
        }

    @pytest.mark.asyncio
    async def test_validate_session_valid(self, manager, mock_conn, valid_session_row):
        mock_conn.fetchrow.return_value = valid_session_row
        mock_conn.execute.return_value = "UPDATE 1"

        session = await manager.validate_session(mock_conn, valid_session_row["session_id"])

        assert session is not None
        assert session.session_id == valid_session_row["session_id"]
        assert session.username == "testuser"
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_session_expired(self, manager, mock_conn):
        now = datetime.now(UTC)
        expired_row = {
            "session_id": uuid4(),
            "user_id": 1,
            "username": "testuser",
            "created_at": now - timedelta(hours=10),
            "expires_at": now - timedelta(hours=1),
            "last_activity_at": now - timedelta(hours=2),
            "client_ip": None,
            "client_hostname": None,
            "is_active": True,
        }
        mock_conn.fetchrow.side_effect = [expired_row, {"user_id": 1, "username": "testuser"}]
        mock_conn.execute.return_value = "UPDATE 1"

        session = await manager.validate_session(mock_conn, expired_row["session_id"])

        assert session is None

    @pytest.mark.asyncio
    async def test_validate_session_inactive(self, manager, mock_conn):
        now = datetime.now(UTC)
        inactive_row = {
            "session_id": uuid4(),
            "user_id": 1,
            "username": "testuser",
            "created_at": now - timedelta(hours=2),
            "expires_at": now + timedelta(hours=6),
            "last_activity_at": now - timedelta(minutes=45),
            "client_ip": None,
            "client_hostname": None,
            "is_active": True,
        }
        mock_conn.fetchrow.side_effect = [inactive_row, {"user_id": 1, "username": "testuser"}]
        mock_conn.execute.return_value = "UPDATE 1"

        session = await manager.validate_session(mock_conn, inactive_row["session_id"])

        assert session is None

    @pytest.mark.asyncio
    async def test_validate_session_not_found(self, manager, mock_conn):
        mock_conn.fetchrow.return_value = None

        session = await manager.validate_session(mock_conn, uuid4())

        assert session is None

    @pytest.mark.asyncio
    async def test_validate_session_no_activity_update(self, manager, mock_conn, valid_session_row):
        mock_conn.fetchrow.return_value = valid_session_row

        session = await manager.validate_session(
            mock_conn, valid_session_row["session_id"], update_activity=False
        )

        assert session is not None
        mock_conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_terminate_session_success(self, manager, mock_conn):
        session_id = uuid4()
        mock_conn.fetchrow.return_value = {"user_id": 1, "username": "testuser"}

        success = await manager.terminate_session(mock_conn, session_id, "logout")

        assert success is True

    @pytest.mark.asyncio
    async def test_terminate_session_not_found(self, manager, mock_conn):
        session_id = uuid4()
        mock_conn.fetchrow.return_value = None

        success = await manager.terminate_session(mock_conn, session_id, "logout")

        assert success is False

    @pytest.mark.asyncio
    async def test_terminate_user_sessions(self, manager, mock_conn):
        mock_conn.execute.return_value = "UPDATE 3"
        mock_conn.fetchval.return_value = "testuser"

        count = await manager.terminate_user_sessions(mock_conn, 1, "password_change")

        assert count == 3

    @pytest.mark.asyncio
    async def test_terminate_user_sessions_with_exclude(self, manager, mock_conn):
        exclude_id = uuid4()
        mock_conn.execute.return_value = "UPDATE 2"
        mock_conn.fetchval.return_value = "testuser"

        count = await manager.terminate_user_sessions(
            mock_conn, 1, "admin", exclude_session_id=exclude_id
        )

        assert count == 2

    @pytest.mark.asyncio
    async def test_get_active_session_count(self, manager, mock_conn):
        mock_conn.fetchval.return_value = 2

        count = await manager.get_active_session_count(mock_conn, 1)

        assert count == 2

    @pytest.mark.asyncio
    async def test_get_active_session_count_none(self, manager, mock_conn):
        mock_conn.fetchval.return_value = None

        count = await manager.get_active_session_count(mock_conn, 1)

        assert count == 0

    @pytest.mark.asyncio
    async def test_enforce_concurrent_limit_under_limit(self, manager, mock_conn):
        mock_conn.fetchval.return_value = 2

        terminated = await manager.enforce_concurrent_limit(mock_conn, 1)

        assert terminated == 0

    @pytest.mark.asyncio
    async def test_enforce_concurrent_limit_at_limit(self, manager, mock_conn):
        mock_conn.fetchval.return_value = 3
        mock_conn.fetch.return_value = [{"session_id": uuid4()}]
        mock_conn.fetchrow.return_value = {"user_id": 1, "username": "testuser"}

        terminated = await manager.enforce_concurrent_limit(mock_conn, 1)

        assert terminated == 1

    @pytest.mark.asyncio
    async def test_enforce_concurrent_limit_over_limit(self, manager, mock_conn):
        mock_conn.fetchval.return_value = 5
        mock_conn.fetch.return_value = [
            {"session_id": uuid4()},
            {"session_id": uuid4()},
            {"session_id": uuid4()},
        ]
        mock_conn.fetchrow.return_value = {"user_id": 1, "username": "testuser"}

        terminated = await manager.enforce_concurrent_limit(mock_conn, 1)

        assert terminated == 3

    @pytest.mark.asyncio
    async def test_list_active_sessions(self, manager, mock_conn):
        now = datetime.now(UTC)
        mock_conn.fetch.return_value = [
            {
                "session_id": uuid4(),
                "user_id": 1,
                "username": "user1",
                "created_at": now,
                "expires_at": now + timedelta(hours=8),
                "last_activity_at": now,
                "client_ip": "192.168.1.1",
                "client_hostname": "host1",
                "application_name": "vcf-pg-loader",
            },
            {
                "session_id": uuid4(),
                "user_id": 2,
                "username": "user2",
                "created_at": now,
                "expires_at": now + timedelta(hours=8),
                "last_activity_at": now,
                "client_ip": None,
                "client_hostname": None,
                "application_name": "vcf-pg-loader",
            },
        ]

        sessions = await manager.list_active_sessions(mock_conn)

        assert len(sessions) == 2
        assert sessions[0]["username"] == "user1"
        assert sessions[1]["username"] == "user2"

    @pytest.mark.asyncio
    async def test_list_active_sessions_filtered_by_user(self, manager, mock_conn):
        now = datetime.now(UTC)
        mock_conn.fetch.return_value = [
            {
                "session_id": uuid4(),
                "user_id": 1,
                "username": "user1",
                "created_at": now,
                "expires_at": now + timedelta(hours=8),
                "last_activity_at": now,
                "client_ip": None,
                "client_hostname": None,
                "application_name": "vcf-pg-loader",
            },
        ]

        sessions = await manager.list_active_sessions(mock_conn, user_id=1)

        assert len(sessions) == 1
        assert sessions[0]["user_id"] == 1

    @pytest.mark.asyncio
    async def test_get_session_history(self, manager, mock_conn):
        now = datetime.now(UTC)
        mock_conn.fetch.return_value = [
            {
                "session_id": uuid4(),
                "created_at": now,
                "expires_at": now + timedelta(hours=8),
                "last_activity_at": now,
                "client_ip": "192.168.1.1",
                "client_hostname": "host1",
                "is_active": True,
                "terminated_reason": None,
                "terminated_at": None,
            },
            {
                "session_id": uuid4(),
                "created_at": now - timedelta(days=1),
                "expires_at": now - timedelta(hours=16),
                "last_activity_at": now - timedelta(hours=17),
                "client_ip": None,
                "client_hostname": None,
                "is_active": False,
                "terminated_reason": "timeout",
                "terminated_at": now - timedelta(hours=16),
            },
        ]

        history = await manager.get_session_history(mock_conn, 1)

        assert len(history) == 2
        assert history[0]["is_active"] is True
        assert history[1]["is_active"] is False
        assert history[1]["terminated_reason"] == "timeout"

    @pytest.mark.asyncio
    async def test_cleanup_expired_sessions(self, manager, mock_conn):
        mock_conn.fetch.return_value = [
            {"session_id": uuid4(), "user_id": 1, "username": "user1"},
            {"session_id": uuid4(), "user_id": 2, "username": "user2"},
        ]
        mock_conn.execute.return_value = "UPDATE 2"

        count = await manager.cleanup_expired_sessions(mock_conn)

        assert count == 2


class TestSessionManagerWithAudit:
    @pytest.fixture
    def audit_logger(self):
        return AsyncMock()

    @pytest.fixture
    def manager(self, audit_logger):
        config = SessionConfig()
        return SessionManager(config, audit_logger)

    @pytest.fixture
    def mock_conn(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_terminate_session_logs_audit_event(self, manager, mock_conn, audit_logger):
        session_id = uuid4()
        mock_conn.fetchrow.return_value = {"user_id": 1, "username": "testuser"}

        await manager.terminate_session(mock_conn, session_id, "logout")

        audit_logger.log_event.assert_called_once()
        event = audit_logger.log_event.call_args[0][0]
        assert event.action == "session_logout"
        assert event.user_id == 1

    @pytest.mark.asyncio
    async def test_terminate_session_timeout_logs_timeout_event(
        self, manager, mock_conn, audit_logger
    ):
        session_id = uuid4()
        mock_conn.fetchrow.return_value = {"user_id": 1, "username": "testuser"}

        await manager.terminate_session(mock_conn, session_id, "timeout")

        audit_logger.log_event.assert_called_once()
        event = audit_logger.log_event.call_args[0][0]
        assert event.action == "session_timeout"

    @pytest.mark.asyncio
    async def test_terminate_user_sessions_logs_audit(self, manager, mock_conn, audit_logger):
        mock_conn.execute.return_value = "UPDATE 2"
        mock_conn.fetchval.return_value = "testuser"

        await manager.terminate_user_sessions(mock_conn, 1, "password_change")

        audit_logger.log_event.assert_called_once()
        event = audit_logger.log_event.call_args[0][0]
        assert event.details["sessions_terminated"] == 2
        assert event.details["reason"] == "password_change"


class TestSessionModel:
    def test_session_is_expired_true(self):
        now = datetime.now(UTC)
        session = Session(
            session_id=uuid4(),
            user_id=1,
            username="test",
            created_at=now - timedelta(hours=10),
            expires_at=now - timedelta(hours=1),
        )
        assert session.is_expired() is True

    def test_session_is_expired_false(self):
        now = datetime.now(UTC)
        session = Session(
            session_id=uuid4(),
            user_id=1,
            username="test",
            created_at=now - timedelta(hours=1),
            expires_at=now + timedelta(hours=7),
        )
        assert session.is_expired() is False
