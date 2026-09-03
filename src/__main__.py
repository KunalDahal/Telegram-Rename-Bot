import asyncio
import sys
import os
import logging

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

_original_get_event_loop = asyncio.get_event_loop

def _patched_get_event_loop():
    try:
        return _original_get_event_loop()
    except RuntimeError:
        return _loop

asyncio.get_event_loop = _patched_get_event_loop

from pyrogram import Client, idle, filters
from pyrogram.types import Message

from src import Config, TaskQueue, UserSettings
from src.core import AccessControl, UserSettingsStore
from src.services import Worker
from src.handlers.settings import setup_settings_handlers
from src.handlers.status import setup_status_handlers
from src.handlers.start import setup_start_handler
from src.handlers.cancel import setup_cancel_handlers, set_worker_instance
from src.handlers.restart import setup_restart_handler
from src.handlers.rename import setup_rename_handler
from src.handlers.set import setup_set_handlers
from src.utils.workers import setup_worker_handlers, sync_bot_command_scopes
from src.handlers.mi import setup_mediainfo_handlers

logging.basicConfig(level=logging.INFO)


async def main():
    config = Config()

    config.paths.makedirs()

    app = Client(
        "encode_bot_session",
        api_id=config.api_id,
        api_hash=config.api_hash,
        bot_token=config.bot_token,
        workdir=config.paths.logs,
        workers=32,
        # The queue manager never has more than this many files in flight.
        max_concurrent_transmissions=config.gl_limit,
    )

    task_queue = TaskQueue()
    access_control = AccessControl(config.owner_ids, config.mongo_uri, config.mongo_db_name)
    user_settings_store = UserSettingsStore(config.mongo_uri, config.mongo_db_name)
    _user_settings_cache: dict[int, UserSettings] = {}

    def get_user_settings(user_id: int) -> UserSettings:
        if user_id not in _user_settings_cache:
            _user_settings_cache[user_id] = UserSettings(
                user_id, config.paths, user_settings_store
            )
        return _user_settings_cache[user_id]

    await app.start()
    try:
        await access_control.initialize()
        user_settings_store.initialize()
    except Exception:
        user_settings_store.close()
        access_control.close()
        await app.stop()
        raise

    task_queue.set_task_store(access_control.task_store)
    try:
        saved_tasks = await access_control.task_store.load_active()
    except Exception:
        access_control.close()
        await app.stop()
        raise
    recovered_count = sum(task_queue.restore_task(task) for task in saved_tasks)
    if recovered_count:
        logging.info("Recovered %d unfinished task(s) from MongoDB.", recovered_count)

    # ── Register handlers ─────────────────────────────────────────────────────
    worker = Worker(task_queue, get_user_settings, app, config)
    set_worker_instance(worker)

    if config.session_string:
        try:
            await worker.configure_premium_download_session(config.session_string)
            logging.info("Premium download session enabled from SESSION_STRING.")
        except Exception:
            logging.exception("SESSION_STRING could not be used; using normal bot downloads.")

    setup_set_handlers(app=app, user_settings=get_user_settings, config=config, access_control=access_control)
    setup_rename_handler(app, task_queue, get_user_settings, config, access_control)
    setup_cancel_handlers(app, task_queue, config, access_control)
    setup_restart_handler(app, task_queue, config, access_control)
    setup_status_handlers(app=app, task_queue=task_queue, config=config, access_control=access_control)
    setup_start_handler(app, config, access_control)
    setup_settings_handlers(
        app=app,
        user_settings=get_user_settings,
        config=config,
        access_control=access_control,
        premium_session_checker=lambda: worker.has_premium_download_session,
    )
    setup_worker_handlers(app=app, config=config, access_control=access_control)
    setup_mediainfo_handlers(app=app, config=config, access_control=access_control)

    # Telegram command menus are scoped separately from the bot's MongoDB
    # authorization layer. Set the default menu first, then add private-chat
    # scopes for current admins/owners. Admin add/remove handlers keep those
    # per-user scopes synchronized at runtime.
    await sync_bot_command_scopes(app, config, access_control)
    

    # ── Worker ────────────────────────────────────────────────────────────────
    asyncio.create_task(worker.start())

    # ── Ready ─────────────────────────────────────────────────────────────────
    me = await app.get_me()
    print(f"""
    ╔══════════════════════════════════╗
    ║  @{me.username:<31}║
    ╚══════════════════════════════════╝
    """)

    try:
        await idle()
    finally:
        await worker.stop()
        await task_queue.flush_checkpoints()
        user_settings_store.close()
        access_control.close()
        await app.stop()


if __name__ == "__main__":
    try:
        _loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        _loop.close()
