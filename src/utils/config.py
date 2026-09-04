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
        self.base = base_dir
        self.bin = os.path.join(base_dir, "bin")
        self.tmp = os.path.join(base_dir, "bin", "tmp")
        self.logs = os.path.join(base_dir, "bin", "logs")
        self.thumbnails = os.path.join(base_dir, "bin", "thumbnails")
        self.users = os.path.join(base_dir, "bin", "users")
        self.fonts = os.path.join(base_dir, "bin", "fonts")
        self.templates = os.path.join(base_dir, "templates")
        self.ffmpeg = _find_binary("ffmpeg", self.bin)

        self.start_image = "https://i.ibb.co/N6Fc2mbZ/start.png"
        self.help_banner = os.path.join(base_dir, "templates", "help.jpg")
        self.default_thumb = os.path.join(base_dir, "bin", "default.jpg")

    def makedirs(self):
        for d in (
            self.tmp,
            self.logs,
            self.thumbnails,
            self.users,
            self.fonts,
        ):
            os.makedirs(d, exist_ok=True)


class Config:
    _SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def __init__(self):
        load_dotenv()

        self.bot_token: str = os.getenv("BOT_TOKEN", "").strip()

        try:
            self.api_id: int = int(os.getenv("API_ID", "0"))
        except ValueError as exc:
            raise ValueError("API_ID must be an integer") from exc

        self.api_hash: str = os.getenv("API_HASH", "").strip()

        self.allowed_group_ids: List[int] = self._parse_int_list(
            os.getenv("ALLOWED_GROUP_IDS", "0")
        )
        self.owner_ids: List[int] = self._parse_int_list(
            os.getenv("OWNER_IDS", "")
        )

        self.mongo_uri: str = os.getenv("MONGO_URI", "").strip()

        self.mongo_db_name: str = os.getenv(
            "MONGO_DB_NAME",
            "renamer_bot",
        ).strip()

        self.bot_session_string: str = os.getenv(
            "BOT_SESSION_STRING",
            "",
        ).strip()

        self.session_string: str = os.getenv(
            "SESSION_STRING",
            "",
        ).strip()

        # Telegram targets stay as strings because:
        # - BOT_DUMP_CHAT_ID may be a numeric -100... ID, public @username,
        #   or a private invite URL for the bot-side dump.
        # - DUMP_CHAT_ID may be a private invite URL or public @username for
        #   the optional Premium SESSION_STRING.
        self.dump_chat_id: str | None = self._optional_chat_target_env(
            "DUMP_CHAT_ID"
        )
        self.bot_dump_chat_id: str | None = self._optional_chat_target_env(
            "BOT_DUMP_CHAT_ID"
        )

        self.gl_limit = self._positive_int_env("GL_LIMIT", default=4)
        self.dl_limit = self._positive_int_env("DL_LIMIT", default=4)
        # Premium-session download concurrency. Independent from DL_LIMIT.
        self.pm_dl_limit = self._positive_int_env("PM_DL_LIMIT", default=4)
        self.pm_ul_limit = self._positive_int_env("PM_UL_LIMIT", default=1)
        self.ul_limit = self._positive_int_env("UL_LIMIT", default=1)
        self.wm_limit = self._positive_int_env("WM_LIMIT", default=1)

        self.max_rename_at_once = self.gl_limit
        self.command_postfix = self._command_postfix_env()

        self.paths = Paths(self._SRC_DIR)

        self._validate()

    @staticmethod
    def _parse_int_list(raw: str) -> List[int]:
        if not raw:
            return []

        result: List[int] = []

        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue

            try:
                result.append(int(part))
            except ValueError:
                raise ValueError(
                    f"Expected comma-separated integer IDs, got {part!r}"
                )

        return result

    @staticmethod
    def _optional_chat_target_env(name: str) -> str | None:
        raw = os.getenv(name, "").strip()
        return raw or None

    @staticmethod
    def _positive_int_env(name: str, default: int) -> int:
        try:
            value = int(os.getenv(name, str(default)))
        except ValueError:
            return default

        return value if value > 0 else default

    @staticmethod
    def _command_postfix_env() -> str:
        value = os.getenv("COMMAND_POSTFIX", "0").strip()

        if not value or value == "0":
            return ""

        if not value.isdigit():
            raise ValueError(
                "COMMAND_POSTFIX must be a non-negative integer"
            )

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

        # Bot-side dump is independent from the optional Premium session.
        if self.bot_dump_chat_id is None:
            raise ValueError(
                "BOT_DUMP_CHAT_ID is required for the bot session. "
                "Use a numeric -100... ID, @username, or invite URL for "
                "the bot-side dump channel."
            )

        # Premium-side dump is only required when a Premium SESSION_STRING
        # has actually been supplied.
        if self.session_string and self.dump_chat_id is None:
            raise ValueError(
                "DUMP_CHAT_ID is required when SESSION_STRING is configured. "
                "Use the Premium account's private invite link or a public "
                "username."
            )
