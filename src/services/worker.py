import asyncio
import os
import shutil
import time
import logging
from copy import deepcopy

from pyrogram import Client
from pyrogram.errors import FloodWait

from src.services.downloader import Downloader
from src.services.media_processor import MediaProcessor
from src.services.uploader import Uploader


logger = logging.getLogger(__name__)

BOT_DOWNLOAD_LIMIT = 2 * 1024 ** 3
PREMIUM_DOWNLOAD_LIMIT = 4 * 1024 ** 3
MAX_PIPELINE_SLOTS = 4


class Worker:
    def __init__(self, task_queue, user_settings_getter, client, config):
        self.task_queue = task_queue
        self.user_settings_getter = user_settings_getter
        self.client = client
        self.download_client = client
        self._premium_download_client: Client | None = None
        self._retired_download_clients: list[Client] = []
        self._dump_chat_id: int | None = None
        self.config = config
        self.media_processor = MediaProcessor(config.paths.ffmpeg)
        self.temp_base = config.paths.tmp
        self.thumbnails_dir = config.paths.thumbnails
        self.running = False

        # GL_LIMIT is the number of COMPLETE job lifecycles admitted to the
        # global pool. DL/UL/WM only constrain their respective stages; they never
        # create additional jobs outside the global pool.
        configured_pool = getattr(config, "gl_limit", getattr(config, "max_rename_at_once", MAX_PIPELINE_SLOTS))
        self.pool_size = max(1, int(configured_pool or MAX_PIPELINE_SLOTS))
        self.max_rename_at_once = self.pool_size
        self.download_limit = max(1, min(int(getattr(config, "dl_limit", self.pool_size)), self.pool_size))
        self.upload_limit = max(1, min(int(getattr(config, "ul_limit", 1)), self.pool_size))
        self.watermark_limit = max(1, min(int(getattr(config, "wm_limit", 1)), self.pool_size))

        self._download_slot = asyncio.Semaphore(self.download_limit)
        self._watermark_processing_slot = asyncio.Semaphore(self.watermark_limit)
        self._upload_slot = asyncio.Semaphore(self.upload_limit)
        logger.info(
            "[Worker] Concurrency limits: global=%d download=%d upload=%d watermark=%d",
            self.pool_size, self.download_limit, self.upload_limit, self.watermark_limit,
        )
        self._active_tasks: dict[str, asyncio.Task] = {}
        self._pool_worker_tasks: list[asyncio.Task] = []
        self._uploading_task_ids: set[str] = set()

        self._ensure_runtime_directories()

    async def start(self):
        self.running = True
        self._startup_cleanup()
        await self._initialize_dump_chat()
        self._pool_worker_tasks = [
            asyncio.create_task(self._pool_worker_loop(index + 1))
            for index in range(self.pool_size)
        ]
        try:
            await self._worker_loop()
        finally:
            await self.stop()

    async def stop(self):
        self.running = False
        tasks = list(self._active_tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        for worker_task in self._pool_worker_tasks:
            if not worker_task.done():
                worker_task.cancel()
        await asyncio.gather(*tasks, *self._pool_worker_tasks, return_exceptions=True)
        self._pool_worker_tasks.clear()
        self._uploading_task_ids.clear()
        await self._stop_download_clients()

    @property
    def has_premium_download_session(self) -> bool:
        return self._premium_download_client is not None

    async def configure_premium_download_session(self, session_string: str):
        """Start and select a Premium user client for downloads only.

        The bot client remains responsible for commands, status messages, and
        delivering renamed files.  A separate user session is required because
        bot sessions cannot download Telegram files larger than 2 GiB.
        """
        candidate = Client(
            "premium_download_session",
            api_id=self.config.api_id,
            api_hash=self.config.api_hash,
            session_string=session_string,
            in_memory=True,
            no_updates=True,
        )
        try:
            await candidate.start()
            account = await candidate.get_me()
            if account.is_bot:
                raise ValueError("This is a bot session, not a user session.")
            if not getattr(account, "is_premium", False):
                raise ValueError("The user account is not Telegram Premium.")
        except Exception:
            try:
                await candidate.stop()
            except Exception:
                pass
            raise

        # Validate/prepare the dump channel with the Premium account before
        # making the session available to the worker.  This is deliberately
        # done only after the session has been verified as a real Premium user.
        if DUMP_CHAT_ID:
            self._dump_chat_id = await self._prepare_dump_chat(candidate, DUMP_CHAT_ID)
            logger.info("[Worker] Premium dump channel ready: %s", self._dump_chat_id)
        else:
            self._dump_chat_id = None
            logger.warning("[Worker] DUMP_CHAT_ID is not configured.")

        previous = self._premium_download_client
        self._premium_download_client = candidate
        self.download_client = candidate
        if previous:
            # Existing tasks may still be using it; stop it when the worker
            # stops instead of interrupting a download in progress.
            self._retired_download_clients.append(previous)
        return account

    async def _prepare_dump_chat(self, client: Client, configured_chat: str) -> int:
        """Resolve the dump chat and let the Premium account join when possible.

        Numeric private chat IDs cannot be joined from an ID alone; the account
        must already be a member. Public usernames and Telegram invite links
        can be joined automatically.
        """
        target = (configured_chat or "").strip()
        if not target:
            raise ValueError("DUMP_CHAT_ID is empty.")

        # Keep Telegram invite URLs intact; Pyrogram accepts usernames,
        # invite links, and already-resolved numeric chat IDs.
        join_target = target

        try:
            chat = await client.get_chat(target)
            return int(chat.id)
        except Exception as lookup_error:
            logger.info(
                "[Worker] Premium account is not currently a member of dump "
                "chat %r; attempting to join it.",
                target,
            )

        # Telegram public usernames and invite links can be joined. A numeric
        # -100... ID does not contain enough information to join a private chat.
        if target.startswith("-100") and target.lstrip("-").isdigit():
            raise ValueError(
                "Premium account is not a member of DUMP_CHAT_ID=%s. "
                "A numeric private channel ID cannot be joined automatically; "
                "add the Premium account to the channel once or use a Telegram "
                "invite link in DUMP_CHAT_ID." % target
            )
        else:
            # Bare public usernames are accepted by join_chat as well.
            join_target = target

        try:
            chat = await client.join_chat(join_target)
        except Exception as join_error:
            raise ValueError(
                f"Premium account could not join DUMP_CHAT_ID={target!r}: {join_error}"
            ) from join_error

        if not chat or not getattr(chat, "id", None):
            raise ValueError(f"Premium dump chat could not be resolved: {target!r}")
        return int(chat.id)

    async def _stop_download_clients(self) -> None:
        clients = [self._premium_download_client, *self._retired_download_clients]
        self._premium_download_client = None
        self._retired_download_clients.clear()
        self.download_client = self.client
        for client in clients:
            if not client:
                continue
            try:
                await client.stop()
            except Exception:
                logger.warning("Could not stop Premium download session cleanly.")

    async def clear_for_restart(self) -> None:
        """Cancel all work and remove only task scratch data before a clean restart."""
        await self.stop()
        if os.path.isdir(self.temp_base):
            for name in os.listdir(self.temp_base):
                folder = os.path.join(self.temp_base, name)
                if os.path.isdir(folder):
                    shutil.rmtree(folder, ignore_errors=True)
        self.task_queue.clear()

    def _startup_cleanup(self):
        self.task_queue.purge_stale_tasks()
        if os.path.exists(self.temp_base):
            active_ids = set(self.task_queue.queue)
            for name in os.listdir(self.temp_base):
                folder = os.path.join(self.temp_base, name)
                if os.path.isdir(folder) and name not in active_ids:
                    shutil.rmtree(folder, ignore_errors=True)

    async def _worker_loop(self):
        # Pool workers block on the same pending queue. Each worker owns one
        # complete task until its upload and delivery finish.
        while self.running:
            await asyncio.sleep(0.25)

    async def _pool_worker_loop(self, worker_number: int):
        while self.running:
            task = self.task_queue.pop_next_queued_task()
            if not task:
                await asyncio.sleep(0.05)
                continue

            task_id = task["task_id"]
            if task_id in self._active_tasks:
                continue

            task["_worker_number"] = worker_number
            task["_start_time"] = task.get("_start_time") or time.time()
            self.task_queue.update_status(task_id, "starting", 0)
            active = asyncio.create_task(self._run_pipeline_task(task))
            self._active_tasks[task_id] = active
            self._refresh_current_task()
            try:
                await active
            except asyncio.CancelledError:
                if not active.done():
                    active.cancel()
                raise
            except Exception:
                # _run_pipeline_task handles task failure and user notification.
                pass

    # ── Bounded transfer pipeline ────────────────────────────────────────────

    def _refresh_current_task(self):
        self.task_queue.current_task = next(iter(self._active_tasks), None)

    @staticmethod
    def _has_watermark(task: dict) -> bool:
        watermark = task.get("watermark", {})
        return bool(watermark.get("enabled") and str(watermark.get("text", "")).strip())

    def _uses_premium_download(self, task: dict) -> bool:
        return task.get("file_size", 0) > BOT_DOWNLOAD_LIMIT

    def _uses_premium_dump(self, task: dict) -> bool:
        return self._uses_premium_download(task)

    async def _resolve_bot_dump_chat(self, configured_chat: str):
        """Resolve the bot-side dump chat robustly.

        For private chats/channels, a numeric -100... ID is only useful if the
        current Pyrogram session already knows the peer. Enumerating dialogs
        gives the bot session an opportunity to learn/cash the peer before the
        final lookup. Public @usernames can be resolved directly.

        Invite links are intentionally rejected for the bot session because a
        bot cannot join a private channel using a t.me/+ invite URL.
        """
        target = (configured_chat or "").strip()

        if not target:
            raise RuntimeError(
                "BOT_DUMP_CHAT_ID must be configured."
            )

        if "+" in target and (
            "t.me/" in target or "telegram.me/" in target
        ):
            raise RuntimeError(
                "BOT_DUMP_CHAT_ID cannot be a private invite URL. "
                "Add the bot to the dump chat and use the numeric -100... "
                "chat ID or a public @username."
            )

        # Public usernames are directly resolvable.
        if target.startswith("@"):
            try:
                return await self.client.get_chat(target)
            except Exception as exc:
                raise RuntimeError(
                    f"Bot cannot resolve BOT_DUMP_CHAT_ID={target!r}. "
                    "Verify that the username is correct and that the bot "
                    "has access to the chat."
                ) from exc

        try:
            target_id = int(target)
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid BOT_DUMP_CHAT_ID={target!r}. "
                "Use a numeric -100... Telegram chat ID or a public @username."
            ) from exc

        # First inspect dialogs. This matters for a cold Pyrogram session:
        # get_chat(-100...) may raise PEER_ID_INVALID if the peer is not yet
        # present in the local peer database.
        try:
            async for dialog in self.client.get_dialogs():
                chat = getattr(dialog, "chat", None)
                if chat and getattr(chat, "id", None) == target_id:
                    return chat
        except Exception:
            logger.warning(
                "[Worker] Failed while enumerating bot dialogs during dump "
                "chat resolution.",
                exc_info=True,
            )

        # Fall back to direct resolution in case the peer is already cached
        # or is otherwise resolvable without dialog enumeration.
        try:
            return await self.client.get_chat(target_id)
        except Exception as exc:
            raise RuntimeError(
                f"Bot cannot resolve BOT_DUMP_CHAT_ID={target_id}. "
                "The exact bot represented by BOT_TOKEN must be a member of "
                "the dump chat. Verify the numeric -100... ID and bot "
                "membership/permissions."
            ) from exc

    async def _initialize_dump_chat(self) -> None:
        # The bot-side archive/copy path uses BOT_DUMP_CHAT_ID.
        bot_target = getattr(self.config, "bot_dump_chat_id", None)

        # Backward-compatible fallback if an older Config implementation is
        # used, while still preferring the explicit bot dump setting.
        if not bot_target:
            bot_target = getattr(self.config, "dump_chat_id", None)

        if not bot_target:
            raise RuntimeError(
                "BOT_DUMP_CHAT_ID (or DUMP_CHAT_ID) must be configured. "
                "Use a numeric -100... chat ID or a public @username "
                "for the bot-side dump archive."
            )

        chat = await self._resolve_bot_dump_chat(str(bot_target))

        if not chat or not getattr(chat, "id", None):
            raise RuntimeError(
                f"BOT_DUMP_CHAT_ID={bot_target!r} could not be resolved "
                "for the bot session."
            )

        self._dump_chat_id = int(chat.id)

        # Make startup diagnostics explicit: if Telegram returns a member
        # status, log it so permission issues are distinguishable from peer
        # resolution issues.
        try:
            me = await self.client.get_me()
            member = await self.client.get_chat_member(
                self._dump_chat_id,
                me.id,
            )
            status = getattr(member, "status", "unknown")
            logger.info(
                "[Worker] Bot dump chat ready: %s (%s); bot status=%s",
                getattr(chat, "title", None) or getattr(chat, "username", None) or self._dump_chat_id,
                self._dump_chat_id,
                status,
            )
        except Exception:
            # Resolution succeeded; inability to inspect membership should not
            # turn a known-valid peer into a false PEER_ID_INVALID diagnosis.
            logger.info(
                "[Worker] Bot dump chat resolved: %s",
                self._dump_chat_id,
            )

    async def _stage_source_to_dump(self, task: dict) -> None:
        if not self._dump_chat_id:
            raise RuntimeError("Dump chat is not initialized.")

        existing_chat_id = task.get("download_source_chat_id")
        existing_message_id = task.get("download_source_message_id")
        if existing_chat_id and existing_message_id:
            return

        source_chat_id = task.get("source_chat_id")
        source_message_id = task.get("source_message_id")
        if not source_chat_id or not source_message_id:
            raise Exception("Source chat/message ID is missing for dump staging.")

        forwarded = await self.client.forward_messages(
            chat_id=self._dump_chat_id,
            from_chat_id=source_chat_id,
            message_ids=source_message_id,
            disable_notification=True,
        )
        if isinstance(forwarded, list):
            forwarded = forwarded[0] if forwarded else None
        if not forwarded or not getattr(forwarded, "id", None):
            raise Exception("Failed to archive the received file in the dump channel.")

        task["download_source_chat_id"] = self._dump_chat_id
        task["download_source_message_id"] = forwarded.id
        task["dump_received_message_id"] = forwarded.id
        self.task_queue.checkpoint(task["task_id"])

    async def _download_with_slot(self, task: dict) -> str:
        task_id = task["task_id"]
        saved_path = task.get("downloaded_path", "")
        if task.get("download_completed") and saved_path and os.path.isfile(saved_path):
            if os.path.getsize(saved_path) > 0:
                logger.info("[Worker] Reusing completed download for %s", task_id[:8])
                return saved_path

        file_size = int(task.get("file_size", 0) or 0)
        if file_size > PREMIUM_DOWNLOAD_LIMIT:
            raise Exception("Telegram files larger than 4 GiB cannot be downloaded.")
        if file_size > BOT_DOWNLOAD_LIMIT and not self.has_premium_download_session:
            raise Exception("Files above 2 GiB require a valid SESSION_STRING for a Telegram Premium account.")

        # Archive/stage and the actual media download are separate operations.
        # Only the actual download consumes the download-stage permit, so
        # "waiting_for_download" accurately means waiting for a downloader slot.
        await self._stage_source_to_dump(task)

        self.task_queue.update_status(task_id, "waiting_for_download", 0)
        async with self._download_slot:
            self.task_queue.update_status(task_id, "downloading", 0)
            use_premium = file_size > BOT_DOWNLOAD_LIMIT
            download_client = self._premium_download_client if use_premium else self.client
            downloader = Downloader(self.temp_base, self.task_queue, task_id)
            path = await downloader.download(
                client=download_client,
                task_data=task,
            )
        if not path or not os.path.exists(path):
            raise Exception("Download failed or file missing after download")
        task["downloaded_path"] = path
        task["download_completed"] = True
        self.task_queue.checkpoint(task_id)
        return path

    async def _run_pipeline_task(self, task: dict):
        task_id = task["task_id"]
        try:
            self._ensure_runtime_directories()
            await self._snapshot_thumbnail(task)
            task["user_is_premium"] = await self._get_user_premium(task["user_id"])

            downloaded_path = await self._download_with_slot(task)
            job = self._build_job(task)
            task["output_filename"] = job["output_filename"]
            has_watermark = self._has_watermark(task)

            prepared_path = task.get("prepared_upload_path", "")
            if task.get("prepared_completed") and prepared_path and os.path.isfile(prepared_path):
                upload_path = prepared_path
                logger.info("[Worker] Reusing prepared output for %s.", task_id[:8])
            elif has_watermark:
                self.task_queue.update_status(task_id, "waiting_for_processing", 0)
                async with self._watermark_processing_slot:
                    upload_path = await self._prepare_upload_file(task, downloaded_path, job)
            else:
                upload_path = await self._prepare_upload_file(task, downloaded_path, job)

            task["prepared_upload_path"] = upload_path
            task["prepared_completed"] = True
            self.task_queue.checkpoint(task_id)

            self.task_queue.update_status(task_id, "waiting_for_upload", 0)
            async with self._upload_slot:
                self._uploading_task_ids.add(task_id)
                try:
                    await self._upload_with_flood_retry(task, upload_path, job)
                finally:
                    self._uploading_task_ids.discard(task_id)

            self.task_queue.remove_task(task_id, final_status="completed")
            self._cleanup_task_folder(task_id)
            return
        except asyncio.CancelledError:
            await self._notify_user(task["user_id"], f"⚠️ Task `{task_id[:8]}` was cancelled.")
            self.task_queue.remove_task(task_id, final_status="cancelled")
            self._cleanup_task_folder(task_id)
        except Exception as exc:
            logger.exception("[Worker] Task %s failed", task_id[:8])
            await self._notify_user(task["user_id"], f"❌ Task `{task_id[:8]}` failed.\n{str(exc)[:200]}")
            self.task_queue.remove_task(task_id, final_status="failed", error=str(exc))
            self._cleanup_task_folder(task_id)
        finally:
            self._uploading_task_ids.discard(task_id)
            self._active_tasks.pop(task_id, None)
            self._refresh_current_task()

    async def _upload_with_flood_retry(self, task: dict, file_path: str, job: dict):
        # FloodWait must keep the pool slot occupied. The task simply sleeps for
        # Telegram's requested delay and retries the same upload operation.
        # Other pool slots remain independent.
        max_retries = 5
        attempt = 0
        while True:
            try:
                return await self._upload(task, file_path, job)
            except FloodWait as exc:
                attempt += 1
                if attempt > max_retries:
                    raise
                delay = max(int(getattr(exc, "value", 0) or 0), 1)
                logger.warning(
                    "[Worker] FloodWait %ss for task %s during upload; retry %d/%d.",
                    delay, task["task_id"][:8], attempt, max_retries,
                )
                await asyncio.sleep(delay)

    async def _prepare_upload_file(self, task: dict, downloaded_path: str, job: dict) -> str:
        metadata     = job.get("metadata") or task.get("metadata", {})
        watermark    = task.get("watermark", {})
        has_metadata = any(str(v).strip() for v in metadata.values())
        has_watermark = bool(watermark.get("enabled") and str(watermark.get("text", "")).strip())
        if not has_metadata and not has_watermark:
            return downloaded_path

        task_id     = task["task_id"]
        task_folder = os.path.join(self.temp_base, task_id)
        _, ext = os.path.splitext(job["output_filename"])
        if not ext:
            ext = os.path.splitext(downloaded_path)[1] or ".mkv"
        output_path = os.path.join(task_folder, f"processed_{task_id}{ext}")

        self.task_queue.update_status(task_id, "processing", 0)
        if has_watermark:
            logger.info(
                "[Worker] %s starting watermark processing (mode=%s, output=%s).",
                task_id[:8],
                watermark.get("timing_mode", "range"),
                os.path.basename(output_path),
            )
        try:
            processed_path = await self.media_processor.process(
                input_path=downloaded_path,
                output_path=output_path,
                metadata=metadata,
                watermark=watermark,
            )
        except Exception:
            logger.exception("[Worker] %s media processing failed", task_id[:8])
            raise

        logger.info("[Worker] %s media processing completed.", task_id[:8])
        return processed_path

    async def _upload(self, task: dict, file_path: str, job: dict):
        task_id = task["task_id"]
        self._ensure_runtime_directories()
        self.task_queue.update_status(task_id, "uploading", 0)
        task["upload_progress"] = {"percentage": 0.0}

        if not self._dump_chat_id:
            raise Exception("DUMP_CHAT_ID is required for archived uploads.")

        # The final renamed file is always uploaded to the dump first.
        # Sub-2 GiB files use the bot; larger files use the configured Premium
        # user session. In both cases the user receives a copy from the dump.
        use_premium_dump = self._uses_premium_dump(task)
        if use_premium_dump and not self.has_premium_download_session:
            raise Exception("Files above 2 GiB require a valid SESSION_STRING for the Telegram Premium account.")

        upload_client = self._premium_download_client if use_premium_dump else self.client
        upload_data = {
            **task,
            "upload_file_path": file_path,
            "output_filename": job["output_filename"],
            "send_type": job.get("send_type", "media"),
            "thumbnail_path": await self._resolve_thumbnail(task, job),
            "upload_chat_id": self._dump_chat_id,
        }

        uploader = Uploader(
            upload_client,
            upload_data,
            self.task_queue,
            tmp_dir=self.temp_base,
            # The upload client, not the end-user account, determines the
            # Telegram upload-size limit. Only the Premium upload client gets
            # the larger part size.
            user_is_premium=use_premium_dump,
        )
        results = await uploader.upload()

        if not results:
            raise Exception("Upload completed without returning a dump message.")

        task["dump_renamed_message_ids"] = [
            result.id for result in results if result and getattr(result, "id", None)
        ]
        if len(task["dump_renamed_message_ids"]) != len(results):
            raise Exception("Upload returned an invalid dump message.")
        self.task_queue.checkpoint(task_id)

        for result in results:
            await self.client.copy_message(
                chat_id=task["user_id"],
                from_chat_id=self._dump_chat_id,
                message_id=result.id,
            )

    # ── Completion message ────────────────────────────────────────────────────

    async def _send_completion_to_group(self, task: dict, job: dict, file_path: str):
        source_chat_id = task.get("source_chat_id")
        if not source_chat_id:
            return
        text = ""
        try:
            file_size_bytes = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            if file_size_bytes >= 1024 ** 3:
                size_str = f"{file_size_bytes / (1024**3):.2f} GB"
            elif file_size_bytes >= 1024 ** 2:
                size_str = f"{file_size_bytes / (1024**2):.2f} MB"
            else:
                size_str = f"{file_size_bytes / 1024:.2f} KB"

            elapsed = int(time.time() - task.get("_start_time", time.time()))
            if elapsed < 60:
                elapsed_str = f"{elapsed}s"
            elif elapsed < 3600:
                elapsed_str = f"{elapsed // 60}m {elapsed % 60}s"
            else:
                h, r = divmod(elapsed, 3600)
                elapsed_str = f"{h}h {r // 60}m {r % 60}s"

            text = (
                f"`{job['output_filename']}`\n"
                f"┠ **Elapsed:** {elapsed_str}\n"
                f"➲ File has been Sent to Bot PM (Private)"
            )

            await self.client.send_message(chat_id=source_chat_id, text=text, disable_web_page_preview=True)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await self.client.send_message(chat_id=source_chat_id, text=text, disable_web_page_preview=True)
            except Exception as ex:
                print(f"[Worker] Completion retry failed: {ex}")
        except Exception as e:
            print(f"[Worker] Completion message failed: {e}")

    # ── Thumbnail helpers ─────────────────────────────────────────────────────

    async def _snapshot_thumbnail(self, task: dict):
        src = task.get("thumbnail_path", "")
        if not src or not os.path.exists(src):
            return
        task_folder = os.path.join(self.temp_base, task["task_id"])
        frozen_path = os.path.join(task_folder, f"thumbnail_{task['task_id']}.jpg")
        if os.path.abspath(src) == os.path.abspath(frozen_path):
            return
        os.makedirs(task_folder, exist_ok=True)
        try:
            shutil.copy2(src, frozen_path)
            task["thumbnail_path"] = frozen_path
        except Exception as e:
            print(f"[Worker] Thumbnail snapshot failed: {e}")

    async def _resolve_thumbnail(self, task: dict, job: dict) -> str | None:
        auto_detect = bool(task.get("auto_detect_thumb", False))
        user_thumb  = task.get("thumbnail_path") or job.get("thumbnail_path") or ""
        task_folder = os.path.join(self.temp_base, task["task_id"])

        if not auto_detect:
            return user_thumb if user_thumb and os.path.exists(user_thumb) else None

        source_thumb_id = task.get("source_thumbnail_file_id", "")
        if source_thumb_id:
            os.makedirs(task_folder, exist_ok=True)
            dest = os.path.join(task_folder, "_source_thumb.jpg")
            try:
                downloaded = await self.client.download_media(source_thumb_id, file_name=dest)
                if downloaded and os.path.exists(downloaded):
                    return os.path.abspath(downloaded)
            except Exception:
                pass

        return user_thumb if user_thumb and os.path.exists(user_thumb) else None

    # ── Misc helpers ──────────────────────────────────────────────────────────

    def _cleanup_task_folder(self, task_id: str):
        folder = os.path.join(self.temp_base, task_id)
        if os.path.exists(folder):
            shutil.rmtree(folder, ignore_errors=True)

    def _ensure_runtime_directories(self) -> None:
        os.makedirs(self.temp_base, exist_ok=True)
        os.makedirs(self.thumbnails_dir, exist_ok=True)

    async def _get_user_premium(self, user_id: int) -> bool:
        try:
            user = await self.client.get_users(user_id)
            return bool(getattr(user, "is_premium", False))
        except Exception:
            return False

    def _build_job(self, task: dict) -> dict:
        # Always build from the task's immutable enqueue-time snapshot. Never
        # consult the live per-user settings here: another task from the same
        # user may have changed those settings while this task was queued.
        settings_snapshot = task.get("settings_snapshot") or {}
        jobs = task.get("jobs") or []
        job_snapshot = jobs[0] if jobs else {}
        return {
            "output_filename": task["output_filename"],
            "metadata":        deepcopy(settings_snapshot.get("metadata", job_snapshot.get("metadata", {}))),
            "thumbnail_path":  task.get("thumbnail_path") or job_snapshot.get("thumbnail_path", ""),
            "send_type":       task.get("send_type") or job_snapshot.get("send_type", settings_snapshot.get("send_type", "media")),
        }

    async def _notify_user(self, user_id: int, text: str):
        try:
            await self.client.send_message(user_id, text)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await self.client.send_message(user_id, text)
            except Exception as ex:
                print(f"[Worker] Notify failed after FloodWait: {ex}")
        except Exception as e:
            print(f"[Worker] Notify failed: {e}")

    # ── Cancel ────────────────────────────────────────────────────────────────

    async def cancel_task(self, task_id: str):
        task = self.task_queue.get_task(task_id)

        # Upload runs inside the task's pool slot, so cancelling the pipeline
        # task directly also cancels the active Telegram upload.
        active_task = self._active_tasks.get(task_id)
        if active_task and not active_task.done():
            active_task.cancel()
            await asyncio.gather(active_task, return_exceptions=True)
            return

        task = self.task_queue.get_task(task_id)
        if task:
            self.task_queue.remove_task(task_id, final_status="cancelled")
            self._cleanup_task_folder(task_id)
            await self._notify_user(task["user_id"], f"⚠️ Task `{task_id[:8]}` cancelled.")
