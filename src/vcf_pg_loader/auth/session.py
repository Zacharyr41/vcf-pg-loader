"""Local session token storage.

Securely stores authentication tokens on the local filesystem.
"""

import json
import logging
import os
import stat
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_SESSION_DIR = Path.home() / ".vcf-pg-loader"
DEFAULT_SESSION_FILE = DEFAULT_SESSION_DIR / "session"


class SessionStorage:
    def __init__(self, session_file: Path | None = None):
        self._session_file = session_file or DEFAULT_SESSION_FILE

    def _ensure_directory(self) -> None:
        self._session_file.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(self._session_file.parent, stat.S_IRWXU)

    def save_token(self, token: str, username: str, expires_at: datetime) -> None:
        self._ensure_directory()

        data = {
            "token": token,
            "username": username,
            "expires_at": expires_at.isoformat(),
            "saved_at": datetime.now().isoformat(),
        }

        with open(self._session_file, "w") as f:
            json.dump(data, f)

        os.chmod(self._session_file, stat.S_IRUSR | stat.S_IWUSR)
        logger.debug("Saved session token for user: %s", username)

    def load_token(self) -> tuple[str | None, str | None]:
        if not self._session_file.exists():
            return None, None

        try:
            with open(self._session_file) as f:
                data = json.load(f)

            return data.get("token"), data.get("username")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load session: %s", e)
            return None, None

    def clear_token(self) -> bool:
        if self._session_file.exists():
            self._session_file.unlink()
            logger.debug("Cleared session token")
            return True
        return False

    def get_session_info(self) -> dict | None:
        if not self._session_file.exists():
            return None

        try:
            with open(self._session_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
