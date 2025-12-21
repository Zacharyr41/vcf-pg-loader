"""Integration tests for HIPAA audit log immutability and integrity."""

from datetime import UTC, date, datetime, timedelta

import pytest

from vcf_pg_loader.audit import (
    AuditEvent,
    AuditEventType,
    AuditIntegrity,
    AuditLogger,
    AuditSchemaManager,
)


@pytest.mark.integration
class TestAuditImmutability:
    @pytest.fixture
    async def audit_db(self, test_db):
        schema_manager = AuditSchemaManager()
        await schema_manager.create_audit_schema(test_db)
        await schema_manager.create_initial_partitions(test_db, months_ahead=1)
        yield test_db

    async def test_update_blocked_by_trigger(self, audit_db):
        """Verify that UPDATE on audit records is blocked."""
        conn = audit_db

        await conn.execute(
            """
            INSERT INTO hipaa_audit_log (
                event_type, user_name, action, success
            ) VALUES (
                'DATA_READ'::audit_event_type, 'test_user', 'test_action', true
            )
            """
        )

        with pytest.raises(Exception) as exc_info:
            await conn.execute(
                """
                UPDATE hipaa_audit_log SET action = 'modified_action'
                WHERE user_name = 'test_user'
                """
            )

        assert "Audit log records cannot be modified" in str(exc_info.value)
        assert "HIPAA" in str(exc_info.value)

    async def test_delete_blocked_by_trigger(self, audit_db):
        """Verify that DELETE on audit records is blocked."""
        conn = audit_db

        await conn.execute(
            """
            INSERT INTO hipaa_audit_log (
                event_type, user_name, action, success
            ) VALUES (
                'DATA_READ'::audit_event_type, 'delete_test', 'to_be_deleted', true
            )
            """
        )

        with pytest.raises(Exception) as exc_info:
            await conn.execute(
                """
                DELETE FROM hipaa_audit_log WHERE user_name = 'delete_test'
                """
            )

        assert "Audit log records cannot be modified" in str(exc_info.value)

    async def test_insert_allowed(self, audit_db):
        """Verify that INSERT is still allowed."""
        conn = audit_db

        await conn.execute(
            """
            INSERT INTO hipaa_audit_log (
                event_type, user_name, action, success
            ) VALUES (
                'DATA_WRITE'::audit_event_type, 'insert_test', 'allowed_insert', true
            )
            """
        )

        count = await conn.fetchval(
            "SELECT COUNT(*) FROM hipaa_audit_log WHERE user_name = 'insert_test'"
        )
        assert count == 1

    async def test_immutability_trigger_exists(self, audit_db):
        """Verify immutability trigger is installed."""
        conn = audit_db
        schema_manager = AuditSchemaManager()
        assert await schema_manager.verify_immutability(conn) is True


@pytest.mark.integration
class TestHashChainIntegrity:
    @pytest.fixture
    async def clean_audit_db(self, postgres_container):
        import asyncpg

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        conn = await asyncpg.connect(url)

        schema_manager = AuditSchemaManager()
        await schema_manager.create_audit_schema(conn)
        await schema_manager.create_initial_partitions(conn, months_ahead=1)

        yield conn, url

        await conn.close()

    async def test_hash_chain_valid(self, clean_audit_db):
        """Verify that valid entries pass integrity check."""
        from asyncpg import create_pool

        conn, url = clean_audit_db
        pool = await create_pool(url, min_size=1, max_size=2)

        logger = AuditLogger(pool=pool, batch_size=1)

        for i in range(5):
            await logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.DATA_READ,
                    action=f"test_action_{i}",
                    success=True,
                    user_name="hash_chain_test",
                    details={"index": i},
                )
            )

        await logger.flush()
        await pool.close()

        count = await conn.fetchval(
            "SELECT COUNT(*) FROM hipaa_audit_log WHERE user_name = 'hash_chain_test'"
        )
        assert count == 5

        entries = await conn.fetch(
            """
            SELECT audit_id, previous_hash, entry_hash
            FROM hipaa_audit_log
            WHERE user_name = 'hash_chain_test'
            ORDER BY created_date, audit_id
            """
        )

        for entry in entries:
            assert entry["entry_hash"] is not None, f"Entry {entry['audit_id']} has no hash"
            assert (
                entry["previous_hash"] is not None
            ), f"Entry {entry['audit_id']} has no previous_hash"

        for i in range(1, len(entries)):
            assert entries[i]["previous_hash"] == entries[i - 1]["entry_hash"], (
                f"Chain broken at entry {entries[i]['audit_id']}: "
                f"expected previous_hash={entries[i-1]['entry_hash'][:16]}..., "
                f"got {entries[i]['previous_hash'][:16] if entries[i]['previous_hash'] else 'NULL'}..."
            )

    async def test_compute_entry_hash_deterministic(self):
        """Verify hash computation is deterministic."""
        integrity = AuditIntegrity()

        hash1 = integrity.compute_entry_hash(
            event_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            event_type="DATA_READ",
            user_name="test_user",
            action="test_action",
            success=True,
            details={"key": "value"},
            previous_hash="abc123",
        )

        hash2 = integrity.compute_entry_hash(
            event_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            event_type="DATA_READ",
            user_name="test_user",
            action="test_action",
            success=True,
            details={"key": "value"},
            previous_hash="abc123",
        )

        assert hash1 == hash2
        assert len(hash1) == 64

    async def test_hash_changes_with_data(self):
        """Verify that changing any field changes the hash."""
        integrity = AuditIntegrity()
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        base_hash = integrity.compute_entry_hash(
            event_time=base_time,
            event_type="DATA_READ",
            user_name="test_user",
            action="test_action",
            success=True,
            details={},
            previous_hash="abc",
        )

        action_changed = integrity.compute_entry_hash(
            event_time=base_time,
            event_type="DATA_READ",
            user_name="test_user",
            action="modified_action",
            success=True,
            details={},
            previous_hash="abc",
        )

        previous_changed = integrity.compute_entry_hash(
            event_time=base_time,
            event_type="DATA_READ",
            user_name="test_user",
            action="test_action",
            success=True,
            details={},
            previous_hash="xyz",
        )

        assert base_hash != action_changed
        assert base_hash != previous_changed
        assert action_changed != previous_changed


@pytest.mark.integration
class TestBackupVerification:
    @pytest.fixture
    async def clean_backup_db(self, postgres_container):
        import asyncpg
        from asyncpg import create_pool

        url = postgres_container.get_connection_url()
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql://")

        conn = await asyncpg.connect(url)

        schema_manager = AuditSchemaManager()
        await schema_manager.create_audit_schema(conn)
        await schema_manager.create_initial_partitions(conn, months_ahead=1)

        pool = await create_pool(url, min_size=1, max_size=2)
        logger = AuditLogger(pool=pool, batch_size=1)

        for i in range(3):
            await logger.log_event(
                AuditEvent(
                    event_type=AuditEventType.DATA_READ,
                    action=f"action_{i}",
                    success=True,
                    user_name="backup_test",
                    details={"i": i},
                )
            )

        await logger.flush()
        await pool.close()

        yield conn

        await conn.close()

    async def test_export_and_verify_backup(self, clean_backup_db):
        """Test export and verification of audit backup."""
        conn = clean_backup_db
        integrity = AuditIntegrity()

        today = date.today()
        entries, metadata = await integrity.export_with_integrity(
            conn,
            start_date=today - timedelta(days=1),
            end_date=today + timedelta(days=1),
        )

        assert metadata.entry_count >= 3
        assert metadata.checksum is not None
        assert len(metadata.checksum) == 64

        is_valid, message = integrity.verify_backup(entries, metadata)
        assert is_valid
        assert "passed" in message.lower()

    async def test_backup_detects_modification(self, clean_backup_db):
        """Verify backup verification detects tampering."""
        conn = clean_backup_db
        integrity = AuditIntegrity()

        today = date.today()
        entries, metadata = await integrity.export_with_integrity(
            conn,
            start_date=today - timedelta(days=1),
            end_date=today + timedelta(days=1),
        )

        if entries:
            entries[0]["action"] = "tampered_action"

        is_valid, message = integrity.verify_backup(entries, metadata)
        assert not is_valid
        assert "checksum" in message.lower() or "mismatch" in message.lower()

    async def test_backup_detects_count_mismatch(self, clean_backup_db):
        """Verify backup verification detects missing entries."""
        conn = clean_backup_db
        integrity = AuditIntegrity()

        today = date.today()
        entries, metadata = await integrity.export_with_integrity(
            conn,
            start_date=today - timedelta(days=1),
            end_date=today + timedelta(days=1),
        )

        entries.pop()

        is_valid, message = integrity.verify_backup(entries, metadata)
        assert not is_valid
        assert "count" in message.lower()


@pytest.mark.integration
class TestRowLevelSecurity:
    @pytest.fixture
    async def rls_db(self, test_db):
        schema_manager = AuditSchemaManager()
        await schema_manager.create_audit_schema(test_db)
        await schema_manager.create_initial_partitions(test_db, months_ahead=1)
        yield test_db

    async def test_rls_policies_created(self, rls_db):
        """Verify RLS policies are created."""
        conn = rls_db

        policies = await conn.fetch(
            """
            SELECT polname FROM pg_policy
            WHERE polrelid = 'hipaa_audit_log'::regclass
            """
        )

        policy_names = {p["polname"] for p in policies}
        assert "audit_user_isolation" in policy_names
        assert "audit_insert_only" in policy_names

    async def test_audit_roles_created(self, rls_db):
        """Verify audit roles are created."""
        conn = rls_db

        roles = await conn.fetch(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN ('audit_viewer', 'audit_admin')
            """
        )

        role_names = {r["rolname"] for r in roles}
        assert "audit_viewer" in role_names
        assert "audit_admin" in role_names
