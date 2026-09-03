import logging

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from src.core.access_control import AccessControl, WorkerStoreError
from src.utils.commands import command_filter


logger = logging.getLogger(__name__)


def _admin_id_from_command(message: Message) -> int | None:
    if len(message.command) != 2:
        return None
    try:
        admin_id = int(message.command[1])
    except ValueError:
        return None
    return admin_id if admin_id > 0 else None


async def _require_owner(message: Message, access_control: AccessControl) -> bool:
    user = message.from_user
    if not user or not access_control.is_owner(user.id):
        return False
    return True


def setup_worker_handlers(app: Client, config, access_control: AccessControl) -> None:
    """Register MongoDB administrator commands; legacy worker aliases remain valid."""
    allowed_filter = filters.private | filters.chat(config.allowed_group_ids)

    @app.on_message(command_filter(config, ["add_admin", "add_workers"]) & allowed_filter)
    async def add_admin_command(client: Client, message: Message):
        if not await _require_owner(message, access_control):
            return
        admin_id = _admin_id_from_command(message)
        if admin_id is None:
            await message.reply_text("Usage: <code>/add_admin &lt;user_id&gt;</code>", parse_mode=ParseMode.HTML)
            return
        if access_control.is_owner(admin_id):
            await message.reply_text("That ID is already a bootstrap owner.", parse_mode=ParseMode.HTML)
            return
        try:
            added = await access_control.add_admin(admin_id, message.from_user.id)
        except WorkerStoreError as exc:
            logger.exception("Unable to add administrator")
            await message.reply_text(f"Error: {exc}")
            return
        label = "added" if added else "already exists"
        await message.reply_text(f"Admin <code>{admin_id}</code> {label}.", parse_mode=ParseMode.HTML)

    @app.on_message(command_filter(config, ["remove_admin", "remove_workers"]) & allowed_filter)
    async def remove_admin_command(client: Client, message: Message):
        if not await _require_owner(message, access_control):
            return
        admin_id = _admin_id_from_command(message)
        if admin_id is None:
            await message.reply_text("Usage: <code>/remove_admin &lt;user_id&gt;</code>", parse_mode=ParseMode.HTML)
            return
        if access_control.is_owner(admin_id):
            await message.reply_text("Bootstrap owners in <code>OWNER_IDS</code> cannot be removed.", parse_mode=ParseMode.HTML)
            return
        try:
            removed = await access_control.remove_admin(admin_id)
        except WorkerStoreError as exc:
            logger.exception("Unable to remove administrator")
            await message.reply_text(f"Error: {exc}")
            return
        label = "removed" if removed else "was not found"
        await message.reply_text(f"Admin <code>{admin_id}</code> {label}.", parse_mode=ParseMode.HTML)

    @app.on_message(command_filter(config, ["list_admin", "view_workers"]) & allowed_filter)
    async def list_admin_command(client: Client, message: Message):
        if not await _require_owner(message, access_control):
            return
        try:
            admin_ids = await access_control.list_admins()
        except WorkerStoreError as exc:
            logger.exception("Unable to list administrators")
            await message.reply_text(f"Error: {exc}")
            return
        if not admin_ids:
            await message.reply_text("No administrators have been added.")
            return
        lines = ["<b>Administrators</b>", ""]
        lines.extend(f"- <code>{admin_id}</code>" for admin_id in admin_ids)
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
