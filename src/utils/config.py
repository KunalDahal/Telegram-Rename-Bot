import os
import shutil
import sys
from typing import List
from dotenv import load_dotenv


def _find_binary(name: str, local_dir: str) -> str:
    found = shutil.which(name)
    if found:
        return found
    exe_suffix = ".exe" if sys.platform == "win32" else ""
    local_path = os.path.join(local_dir, f"{name}{exe_suffix}")
    if os.path.isfile(local_path):
        return local_path
    return name


class Paths:

    def __init__(self, base_dir: str):
        self.base       = base_dir
        self.bin        = os.path.join(base_dir, "bin")
        self.tmp        = os.path.join(base_dir, "bin", "tmp")
        self.logs       = os.path.join(base_dir, "bin", "logs")
        self.thumbnails = os.path.join(base_dir, "bin", "thumbnails")
        self.users      = os.path.join(base_dir, "bin", "users")
        self.fonts      = os.path.join(base_dir, "bin", "fonts")
        self.templates  = os.path.join(base_dir, "templates")
        self.ffmpeg     = _find_binary("ffmpeg", self.bin)

        self.start_image   = "https://i.ibb.co/N6Fc2mbZ/start.png"
        self.help_banner   = os.path.join(base_dir, "templates", "help.jpg")
        self.default_thumb = os.path.join(base_dir, "bin", "default.jpg")

    def makedirs(self):
        for d in (self.tmp, self.logs, self.thumbnails, self.users, self.fonts):
            os.makedirs(d, exist_ok=True)


class Config:
    _SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def __init__(self):
        load_dotenv()

        self.bot_token: str = os.getenv("BOT_TOKEN", "")
        self.api_id:    int = int(os.getenv("API_ID", "0"))
        self.api_hash:  str = os.getenv("API_HASH", "")

        self.allowed_group_ids: List[int] = self._parse_int_list(
            os.getenv("ALLOWED_GROUP_IDS", "0")
        )
        self.owner_ids: List[int] = self._parse_int_list(
            os.getenv("OWNER_IDS", "")
        )
        self.mongo_uri: str = os.getenv("MONGO_URI", "").strip()
        # A separate database lets several bot tokens use one MongoDB cluster
        # without sharing administrators, task checkpoints, or saved sessions.
        self.mongo_db_name: str = os.getenv("MONGO_DB_NAME", "renamer_bot").strip()
        self.session_string: str = os.getenv("SESSION_STRING", "").strip()

        # Telegram dump/staging chats. Keep these as integers so Pyrogram
        # receives Telegram peer IDs as numeric IDs, never strings.
        self.dump_chat_id: int | None = self._optional_chat_id_env("DUMP_CHAT_ID")
        self.bot_dump_chat_id: int | None = self._optional_chat_id_env("BOT_DUMP_CHAT_ID")
        # Global lifecycle pool and per-stage concurrency limits.
        # GL_LIMIT is the master cap on jobs admitted to the pipeline.
        # DL_LIMIT / UL_LIMIT / WM_LIMIT only restrict their respective stages.
        self.gl_limit = self._positive_int_env("GL_LIMIT", default=4)
        self.dl_limit = self._positive_int_env("DL_LIMIT", default=4)
        self.ul_limit = self._positive_int_env("UL_LIMIT", default=1)
        self.wm_limit = self._positive_int_env("WM_LIMIT", default=1)

        # Backward compatibility for older code/handlers.
        self.max_rename_at_once = self.gl_limit
        self.command_postfix = self._command_postfix_env()

        self.paths = Paths(self._SRC_DIR)

        self._validate()

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_int_list(raw: str) -> List[int]:
        if not raw:
            return []
        result = []
        for part in raw.split(","):
            part = part.strip()
            if part:
                try:
                    result.append(int(part))
                except ValueError:
                    pass
        return result

    @staticmethod
    def _optional_chat_id_env(name: str) -> int | None:
        raw = os.getenv(name, "").strip()
        if not raw:
            return None

        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(
                f"{name} must be a numeric Telegram chat ID "
                f"(for example -1001234567890), not {raw!r}"
            ) from exc

    @staticmethod
    def _positive_int_env(name: str, default: int) -> int:
        """Read a positive concurrency value without bad env input crashing the bot."""
        try:
            value = int(os.getenv(name, str(default)))
        except ValueError:
            return default
        return value if value > 0 else default

    @staticmethod
    def _command_postfix_env() -> str:
        """Use a digits-only command postfix; 0 and empty both mean no postfix."""
        value = os.getenv("COMMAND_POSTFIX", "0").strip()
        if not value or value == "0":
            return ""
        if not value.isdigit():
            raise ValueError("COMMAND_POSTFIX must be a non-negative integer")
        return value

    def _validate(self):
        if not self.bot_token:
            raise ValueError("BOT_TOKEN is required")
        if not self.api_id:
            raise ValueError("API_ID is required")
        if not self.api_hash:
            raise ValueError("API_HASH is required")
        if not self.allowed_group_ids or self.allowed_group_ids[0] == 0:
            raise ValueError("ALLOWED_GROUP_IDS is required")
        if not self.owner_ids:
            raise ValueError("OWNER_IDS is required")
        if not self.mongo_uri:
            raise ValueError("MONGO_URI is required")
        if not self.mongo_db_name:
            raise ValueError("MONGO_DB_NAME cannot be empty")

        if self.bot_dump_chat_id is None:
            raise ValueError("BOT_DUMP_CHAT_ID is required")
