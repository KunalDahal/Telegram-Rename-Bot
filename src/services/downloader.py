import os
import asyncio
import time


MAX_DOWNLOAD_ATTEMPTS = 3


class Downloader:
    def __init__(self, temp_base: str, task_queue=None, task_id=None):
        self.temp_base = temp_base
        self.task_queue = task_queue
        self.task_id = task_id
        self._start_time = None
        self._last_cb_time = None
        self._last_cb_bytes = 0
        self._declared_size = 0

        os.makedirs(self.temp_base, exist_ok=True)

        self.download_progress = {
            "total_size": 0,
            "downloaded": 0,
            "percentage": 0,
            "speed": 0,
            "eta": 0,
            "elapsed": 0,
            "status": "idle",
        }

    async def download(self, client, task_data: dict) -> str:
        task_id = task_data["task_id"]
        file_id = task_data.get("file_id", "")
        source_chat_id = task_data.get("download_source_chat_id")
        source_message_id = task_data.get("download_source_message_id")
        original_file_name = task_data.get("original_file_name") or f"video_{task_id}.mkv"
        self.task_id = task_id
        self._declared_size = task_data.get("file_size") or 0

        task_folder = os.path.join(self.temp_base, task_id)
        self._ensure_task_folder(task_folder)

        desired_path = os.path.join(task_folder, original_file_name)
        desired_abs = os.path.abspath(desired_path)

        self._reset_progress()
        self.download_progress["status"] = "downloading"
        self._start_time = time.time()

        try:
            # Keep incomplete downloads separate from the final file.  A failed
            # or interrupted Pyrogram transfer can leave behind a zero-byte
            # destination; it must never be treated as a completed rename.
            for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
                staged_path = os.path.join(task_folder, f".download-{attempt}.part")
                # Pyrogram creates its own ``.temp`` file beside file_name.
                # Recreate both directories here in case a container cleanup or
                # restart removed the scratch directory between attempts.
                self._ensure_task_folder(task_folder)
                self._remove_file(staged_path)
                self._reset_progress()
                self.download_progress["status"] = "downloading"
                self._start_time = time.time()
                actual_path = None

                try:
                    media_source = file_id
                    if source_chat_id and source_message_id:
                        media_source = await client.get_messages(
                            chat_id=source_chat_id,
                            message_ids=source_message_id,
                        )
                        if not media_source or getattr(media_source, "empty", False):
                            raise Exception(
                                f"Source message not found: {source_chat_id}/{source_message_id}"
                            )

                    actual_path = await client.download_media(
                        media_source,
                        file_name=staged_path,
                        progress=self._progress_callback,
                    )
                    actual_path = self._validate_download(actual_path)
                    os.replace(actual_path, desired_abs)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._remove_file(staged_path)
                    # Pyrogram normally returns staged_path, but clean up any
                    # alternate failed path it may have returned as well.
                    if actual_path:
                        self._remove_file(actual_path)
                    if attempt == MAX_DOWNLOAD_ATTEMPTS:
                        raise Exception(
                            f"Download did not complete after {MAX_DOWNLOAD_ATTEMPTS} attempts: {error}"
                        ) from error

            if not os.path.isfile(desired_abs):
                raise Exception(f"File not found after download: {desired_abs}")

            actual_path = desired_abs
            completed_size = os.path.getsize(actual_path)
            self.download_progress["total_size"] = completed_size
            self.download_progress["downloaded"] = completed_size

            self.download_progress["status"] = "completed"
            if self.task_queue and self.task_id:
                self.task_queue.update_status(self.task_id, "downloading", 100)
                task = self.task_queue.tasks.get(self.task_id)
                if task is not None:
                    task["progress_details"] = {
                        "total_size": self.download_progress["total_size"],
                        "downloaded": self.download_progress["total_size"],
                        "percentage": 100,
                        "speed": 0,
                        "eta": 0,
                    }
                    task["progress"] = 100.0

            return actual_path

        except asyncio.CancelledError:
            self.download_progress["status"] = "cancelled"
            raise
        except Exception as e:
            self.download_progress["status"] = "failed"
            raise Exception(f"Download failed: {e}")

    def _validate_download(self, actual_path) -> str:
        if not actual_path:
            raise Exception("download_media returned None")

        actual_path = os.path.abspath(actual_path)
        if not os.path.isfile(actual_path):
            raise Exception(f"File not found after download: {actual_path}")

        actual_size = os.path.getsize(actual_path)
        if actual_size == 0:
            raise Exception(f"Downloaded file is empty: {actual_path}")

        if self._declared_size and actual_size != self._declared_size:
            raise Exception(
                f"Downloaded file size mismatch: expected {self._declared_size} bytes, "
                f"got {actual_size} bytes"
            )

        return actual_path

    @staticmethod
    def _remove_file(path) -> None:
        try:
            if path and os.path.isfile(path):
                os.remove(path)
        except OSError:
            pass

    def _ensure_task_folder(self, task_folder: str) -> None:
        os.makedirs(self.temp_base, exist_ok=True)
        os.makedirs(task_folder, exist_ok=True)

    async def _progress_callback(self, current: int, total: int):
        now = time.time()

        if not total:
            total = getattr(self, "_declared_size", 0)

        if self.download_progress["total_size"] == 0 and total:
            self.download_progress["total_size"] = total

        total_elapsed = int(now - self._start_time) if self._start_time else 1

        if self._last_cb_time is None:
            self._last_cb_time = now
            self._last_cb_bytes = current
            speed = 0.0
        else:
            interval = now - self._last_cb_time
            if interval >= 0.5:
                speed = max(0.0, (current - self._last_cb_bytes) / interval)
                self._last_cb_time = now
                self._last_cb_bytes = current
            else:
                speed = self.download_progress.get("speed", 0.0)

        percentage = (current / total * 100) if total > 0 else 0
        eta = int((total - current) / speed) if speed > 0 and total > current else 0

        self.download_progress.update({
            "downloaded": current,
            "percentage": round(percentage, 2),
            "speed": round(speed, 2),
            "eta": eta,
            "elapsed": total_elapsed,
            "status": "downloading",
        })

        if self.task_queue and self.task_id:
            self.task_queue.update_status(self.task_id, "downloading", round(percentage, 2))
            task = self.task_queue.tasks.get(self.task_id)
            if task is not None:
                task["progress_details"] = {
                    "total_size": total or self.download_progress["total_size"],
                    "downloaded": current,
                    "percentage": round(percentage, 2),
                    "speed": round(speed, 2),
                    "eta": eta,
                }
                task["progress"] = round(percentage, 2)

    def _reset_progress(self):
        self.download_progress = {
            "total_size": 0,
            "downloaded": 0,
            "percentage": 0,
            "speed": 0,
            "eta": 0,
            "elapsed": 0,
            "status": "idle",
        }
        self._start_time = None
        self._last_cb_time = None
        self._last_cb_bytes = 0
