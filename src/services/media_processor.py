import asyncio
import json
import logging
import os
import re
import tempfile
import shutil
import time


logger = logging.getLogger(__name__)


class MediaProcessor:
    def __init__(self, ffmpeg_path: str = "ffmpeg", ffprobe_path: str = "ffprobe"):
        self.ffmpeg_path  = ffmpeg_path
        self.ffprobe_path = ffprobe_path

    # ─────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ─────────────────────────────────────────────────────────────────────────

    async def process(
        self,
        input_path: str,
        output_path: str,
        metadata: dict | None = None,
        watermark: dict | None = None,
    ) -> str:
        metadata  = metadata  or {}
        watermark = watermark or {}

        has_metadata = self._has_metadata(metadata)
        has_watermark = self._watermark_enabled(watermark)
        logger.info(
            "[Media] Processing %s (metadata=%s, watermark=%s)",
            os.path.basename(input_path),
            has_metadata,
            has_watermark,
        )

        if not has_metadata and not has_watermark:
            logger.info("[Media] No processing requested; using the downloaded file.")
            return input_path

        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

        if has_watermark:
            return await self._process_with_watermark(input_path, output_path, metadata, watermark)

        # metadata-only → plain stream copy, no re-encode needed
        cmd = self._build_copy_command(input_path, output_path, metadata)
        await self._run(cmd, "Metadata stream copy")
        self._assert_output(output_path)
        return output_path

    # ─────────────────────────────────────────────────────────────────────────
    # Watermark routing  (three modes)
    # ─────────────────────────────────────────────────────────────────────────

    async def _process_with_watermark(
        self, input_path: str, output_path: str, metadata: dict, watermark: dict
    ) -> str:
        mode = str(watermark.get("timing_mode", "range"))
        logger.info("[Media] Watermark pipeline started (mode=%s).", mode)

        if mode == "full":
            logger.info("[Media] Encoding the full video with the watermark.")
            cmd = await self._build_full_watermark_command(input_path, output_path, metadata, watermark)
            await self._run(cmd, "Full watermark encode")

        elif mode == "range":
            start = self._clamp_int(watermark.get("start", 0), 0, 86400)
            end   = self._clamp_int(watermark.get("end",   0), 0, 86400)
            if end > start:
                logger.info("[Media] Encoding watermark range %.1fs to %.1fs.", start, end)
                await self._process_segment_watermark(
                    input_path, output_path, metadata, watermark,
                    segments=[(start, end)],
                )
            else:
                # Invalid range → fall back to full encode
                logger.warning("[Media] Invalid watermark range; encoding the full video instead.")
                cmd = await self._build_full_watermark_command(input_path, output_path, metadata, watermark)
                await self._run(cmd, "Fallback full watermark encode")

        elif mode == "random_duration":
            duration     = self._clamp_int(watermark.get("duration",     30), 1, 3600)
            repeat_count = self._clamp_int(watermark.get("repeat_count",  1), 1,   20)
            total        = await self._probe_duration(input_path)
            segments     = self._random_segments(total, duration, repeat_count)
            if segments:
                logger.info("[Media] Encoding %d random watermark window(s): %s", len(segments), segments)
                await self._process_segment_watermark(
                    input_path, output_path, metadata, watermark, segments=segments
                )
            else:
                logger.warning("[Media] No valid random watermark windows; encoding the full video instead.")
                cmd = await self._build_full_watermark_command(input_path, output_path, metadata, watermark)
                await self._run(cmd, "Fallback full watermark encode")
        else:
            logger.warning("[Media] Unknown watermark mode '%s'; encoding the full video.", mode)
            cmd = await self._build_full_watermark_command(input_path, output_path, metadata, watermark)
            await self._run(cmd, "Fallback full watermark encode")

        self._assert_output(output_path)
        logger.info("[Media] Watermark pipeline completed: %s", os.path.basename(output_path))
        return output_path

    # ─────────────────────────────────────────────────────────────────────────
    # Segment-split encode  (range + random modes)
    #
    # Strategy
    # --------
    # 1. Stream-copy the segments BEFORE each watermark window  →  no CPU cost
    # 2. Re-encode ONLY the watermark window with drawtext
    # 3. Stream-copy the segment AFTER the last window          →  no CPU cost
    # 4. Concat all pieces with the concat demuxer              →  no re-encode
    #
    # Intermediate segments are written as MKV so every stream type (video,
    # audio, subtitles, attachments) can be carried without remuxing errors.
    # The final concat step produces the target container (mp4/mkv/etc.).
    # ─────────────────────────────────────────────────────────────────────────

    async def _process_segment_watermark(
        self,
        input_path: str,
        output_path: str,
        metadata: dict,
        watermark: dict,
        segments: list[tuple[int, int]],
    ) -> None:
        tmp_dir = tempfile.mkdtemp(prefix="wm_")
        logger.info("[Media] Preparing %d watermark segment(s).", len(segments))
        try:
            parts: list[str] = []
            cursor = 0.0

            for idx, (seg_start, seg_end) in enumerate(segments):
                # ── gap before this window (stream copy) ─────────────────────
                if seg_start > cursor:
                    gap_path = os.path.join(tmp_dir, f"gap_{idx}.mkv")
                    logger.info("[Media] Copying unchanged segment %.1fs to %.1fs.", cursor, seg_start)
                    await self._stream_copy_segment(input_path, gap_path, cursor, seg_start)
                    parts.append(gap_path)

                # ── watermark window (re-encode video only) ───────────────────
                wm_path = os.path.join(tmp_dir, f"wm_{idx}.mkv")
                logger.info("[Media] Encoding watermark segment %d/%d (%.1fs to %.1fs).", idx + 1, len(segments), seg_start, seg_end)
                await self._encode_watermark_segment(
                    input_path, wm_path, watermark, seg_start, seg_end
                )
                parts.append(wm_path)
                cursor = seg_end

            # ── tail after last window (stream copy) ─────────────────────────
            total = await self._probe_duration(input_path)
            if cursor < total - 0.1:
                tail_path = os.path.join(tmp_dir, "tail.mkv")
                logger.info("[Media] Copying unchanged tail from %.1fs to %.1fs.", cursor, total)
                await self._stream_copy_segment(input_path, tail_path, cursor, None)
                parts.append(tail_path)

            # ── concat all pieces ─────────────────────────────────────────────
            if len(parts) == 1:
                logger.info("[Media] One processed segment; moving it to the output.")
                shutil.move(parts[0], output_path)
            else:
                logger.info("[Media] Concatenating %d processed segment(s).", len(parts))
                list_file = os.path.join(tmp_dir, "concat.txt")
                with open(list_file, "w") as f:
                    for p in parts:
                        f.write(f"file '{p}'\n")
                await self._concat_segments(list_file, output_path, metadata)

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            logger.info("[Media] Watermark temporary files cleaned up.")

    # ─────────────────────────────────────────────────────────────────────────
    # FFmpeg helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _stream_copy_segment(
        self,
        input_path: str,
        output_path: str,
        start: float,
        end: float | None,
    ) -> None:
        """Cut a segment from input using stream copy — zero re-encode cost."""
        cmd = [
            self.ffmpeg_path, "-hide_banner", "-y",
            "-ss", str(start),
            "-i", input_path,
        ]
        if end is not None:
            cmd += ["-t", str(end - start)]
        cmd += [
            "-map", "0",
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            output_path,
        ]
        await self._run(cmd, f"Stream-copy segment {start:.1f}s to {end if end is not None else 'end'}")

    async def _encode_watermark_segment(
        self,
        input_path: str,
        output_path: str,
        watermark: dict,
        start: float,
        end: float,
    ) -> None:
        """
        Re-encode ONLY the watermark window, preserving the source video codec
        and its native parameters.  All non-video streams are stream-copied so
        audio, subtitles, and attachments pass through untouched.

        The output is always MKV so every stream type is supported without
        remuxing errors (important for files with subtitle or attachment streams).
        """
        video_info = await self._probe_video_stream(input_path)
        codec_name = video_info.get("codec_name", "libx264")

        # Map codec name reported by ffprobe → encoder name for ffmpeg
        encoder = self._codec_to_encoder(codec_name)
        logger.info("[Media] Source video codec=%s; selected encoder=%s.", codec_name, encoder)

        drawtext = self._drawtext_filter(watermark, time_offset=start)
        cmd = [
            self.ffmpeg_path, "-hide_banner", "-y",
            "-ss", str(start),
            "-i", input_path,
            "-t",  str(end - start),
            "-map", "0",
            "-vf", drawtext,
            "-c:v", encoder,
        ]

        # Carry forward any quality/bitrate settings from the source stream
        # so we don't silently downgrade quality.
        cmd += self._source_quality_flags(video_info, encoder)

        cmd += [
            "-c:a", "copy",
            "-c:s", "copy",
            "-c:d", "copy",   # data streams (e.g. tmcd)
            "-avoid_negative_ts", "make_zero",
            output_path,      # always MKV — set by caller via .mkv extension
        ]
        await self._run(cmd, f"Watermark segment encode {start:.1f}s to {end:.1f}s")

    async def _concat_segments(
        self, list_file: str, output_path: str, metadata: dict
    ) -> None:
        """
        Concat-demuxer join — no re-encode, just muxes the pieces together.
        Metadata is applied here so it only needs one pass.
        """
        cmd = [
            self.ffmpeg_path, "-hide_banner", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", list_file,
            "-map", "0",
        ]
        self._append_metadata(cmd, metadata)
        cmd += ["-c", "copy", "-movflags", "+faststart", output_path]
        await self._run(cmd, "Concatenate watermark segments")

    async def _build_full_watermark_command(
        self, input_path: str, output_path: str, metadata: dict, watermark: dict
    ) -> list[str]:
        """
        Full-file watermark encode.  Probes the source codec so we re-encode
        with the same encoder and preserve quality instead of defaulting to
        hardcoded libx264 / crf 23.
        """
        video_info = await self._probe_video_stream(input_path)
        codec_name = video_info.get("codec_name", "libx264")
        encoder    = self._codec_to_encoder(codec_name)
        logger.info("[Media] Source video codec=%s; selected encoder=%s.", codec_name, encoder)

        cmd = [
            self.ffmpeg_path, "-hide_banner", "-y",
            "-i", input_path,
            "-map", "0",
        ]
        self._append_metadata(cmd, metadata)
        cmd += [
            "-vf", self._drawtext_filter(watermark),
            "-c:v", encoder,
        ]
        cmd += self._source_quality_flags(video_info, encoder)
        cmd += [
            "-c:a", "copy",
            "-c:s", "copy",
            "-c:d", "copy",
            "-movflags", "+faststart",
            output_path,
        ]
        return cmd

    def _build_copy_command(
        self, input_path: str, output_path: str, metadata: dict
    ) -> list[str]:
        cmd = [self.ffmpeg_path, "-hide_banner", "-y", "-i", input_path, "-map", "0"]
        self._append_metadata(cmd, metadata)
        cmd += ["-c", "copy", output_path]
        return cmd

    # ─────────────────────────────────────────────────────────────────────────
    # Codec / quality helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _codec_to_encoder(codec_name: str) -> str:
        """
        Map ffprobe codec_name → ffmpeg encoder name.
        Falls back to libx264 for anything unrecognised.
        """
        _map = {
            "h264":       "libx264",
            "hevc":       "libx265",
            "vp8":        "libvpx",
            "vp9":        "libvpx-vp9",
            "av1":        "libaom-av1",
            "mpeg2video": "mpeg2video",
            "mpeg4":      "mpeg4",
            "mjpeg":      "mjpeg",
            "prores":     "prores_ks",
        }
        return _map.get(codec_name.lower(), "libx264")

    @staticmethod
    def _source_quality_flags(video_info: dict, encoder: str) -> list[str]:
        """
        Return encoder flags that preserve source quality as closely as
        possible without hardcoding any values.

        Priority order:
          1. If the source has a bit_rate, target that bitrate (-b:v).
          2. Otherwise leave quality to the encoder's default (no flags).

        We deliberately do NOT forward CRF from the source because CRF is an
        encode-time setting not stored in the stream and is not recoverable via
        ffprobe.  Using the source bitrate as a target is the safest proxy.
        """
        flags: list[str] = []

        bit_rate = video_info.get("bit_rate")
        if bit_rate:
            try:
                br = int(bit_rate)
                if br > 0:
                    flags += ["-b:v", str(br)]
                    return flags
            except (TypeError, ValueError):
                pass

        # No bitrate available — use encoder defaults.
        # For libx264/libx265 the default CRF (23/28) is reasonable; for
        # others the encoder picks its own default.  This is intentional:
        # we never silently downgrade the source quality with a hardcoded CRF.
        return flags

    # ─────────────────────────────────────────────────────────────────────────
    # ffprobe helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _probe_duration(self, input_path: str) -> float:
        """Return video duration in seconds via ffprobe."""
        logger.info("[Media] Probing duration with %s.", self.ffprobe_path)
        cmd = [
            self.ffprobe_path,
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            input_path,
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            logger.error("[Media] ffprobe executable was not found: %s", self.ffprobe_path)
            raise RuntimeError("ffprobe is not installed or is not on PATH.") from exc
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning("[Media] Duration probe failed: %s", stderr.decode(errors="ignore")[-500:].strip())
            return 0.0
        try:
            duration = float(stdout.decode().strip())
            logger.info("[Media] Duration: %.2fs.", duration)
            return duration
        except (ValueError, AttributeError):
            logger.warning("[Media] ffprobe did not return a usable duration.")
            return 0.0

    async def _probe_video_stream(self, input_path: str) -> dict:
        """
        Return a dict of the first video stream's properties via ffprobe.
        Keys include: codec_name, width, height, bit_rate, pix_fmt, etc.
        Returns an empty dict on any failure so callers can use .get() safely.
        """
        cmd = [
            self.ffprobe_path,
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries",
            "stream=codec_name,width,height,bit_rate,pix_fmt,r_frame_rate,profile,level",
            "-of", "json",
            input_path,
        ]
        logger.info("[Media] Probing video stream with %s.", self.ffprobe_path)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            logger.error("[Media] ffprobe executable was not found: %s", self.ffprobe_path)
            raise RuntimeError("ffprobe is not installed or is not on PATH.") from exc
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning("[Media] Video-stream probe failed: %s", stderr.decode(errors="ignore")[-500:].strip())
            return {}
        try:
            data    = json.loads(stdout.decode())
            streams = data.get("streams", [])
            stream = streams[0] if streams else {}
            logger.info("[Media] Video stream found: codec=%s, resolution=%sx%s.", stream.get("codec_name", "unknown"), stream.get("width", "?"), stream.get("height", "?"))
            return stream
        except Exception:
            logger.warning("[Media] ffprobe returned invalid video-stream data.")
            return {}

    # ─────────────────────────────────────────────────────────────────────────
    # Random segment generator
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _random_segments(
        total_duration: float,
        seg_duration: int,
        repeat_count: int,
    ) -> list[tuple[int, int]]:
        """
        Generate `repeat_count` non-overlapping windows of `seg_duration`
        seconds, deterministically spread across the video.
        """
        if total_duration <= seg_duration:
            return []

        usable     = total_duration - seg_duration
        slot_width = usable / repeat_count
        segments: list[tuple[int, int]] = []

        for i in range(repeat_count):
            start = int(slot_width * i + slot_width * 0.5)
            end   = start + seg_duration
            if end <= total_duration:
                segments.append((start, end))

        return segments

    # ─────────────────────────────────────────────────────────────────────────
    # drawtext filter builder
    # ─────────────────────────────────────────────────────────────────────────

    def _drawtext_filter(self, watermark: dict, time_offset: float = 0.0) -> str:
        text      = self._escape_filter_value(str(watermark.get("text", "")))
        color     = self._safe_color(str(watermark.get("color", "white")))
        font_size = self._clamp_int(watermark.get("font_size", 24), 8, 96)
        padding   = self._clamp_int(watermark.get("padding", 7),    0, 30)
        x_expr, y_expr = self._position_expr(
            str(watermark.get("position", "bot_right")), padding
        )

        parts = [
            f"text='{text}'",
            f"fontcolor={color}",
            f"fontsize={font_size}",
            f"x={x_expr}",
            f"y={y_expr}",
        ]

        # Prefer a valid user-selected/custom font. If no valid custom font
        # is configured, use the project's default.ttf. If neither exists,
        # leave fontfile unset so FFmpeg can use its normal fallback.
        font_path = str(watermark.get("font_path") or "").strip()
        if not (font_path and os.path.isfile(font_path)):
            font_path = self._default_watermark_font_path()

        if font_path:
            parts.insert(0, f"fontfile='{self._escape_filter_value(font_path)}'")

        # For segment encodes the watermark is always ON for the whole segment,
        # so we skip the enable= expr.
        if time_offset == 0.0:
            enable_expr = self._enable_expr(watermark)
            if enable_expr:
                parts.append(f"enable='{enable_expr}'")

        return "drawtext=" + ":".join(parts)

    def _default_watermark_font_path(self) -> str:
        """Return the project's default watermark font when it is available."""
        module_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.join(module_dir, "templates", "default.ttf"),
            os.path.join(module_dir, "src", "templates", "default.ttf"),
            os.path.join(os.getcwd(), "src", "templates", "default.ttf"),
        ]

        for candidate in candidates:
            if os.path.isfile(candidate):
                return os.path.abspath(candidate)

        logger.warning(
            "[Media] Default watermark font not found; using FFmpeg font fallback."
        )
        return ""

    # ─────────────────────────────────────────────────────────────────────────
    # Subprocess runner
    # ─────────────────────────────────────────────────────────────────────────

    async def _run(self, cmd: list[str], stage: str) -> None:
        executable = os.path.basename(str(cmd[0]))
        output = os.path.basename(str(cmd[-1]))
        logger.info("[Media] %s started (%s → %s).", stage, executable, output)
        started_at = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            logger.error("[Media] %s executable was not found: %s", executable, cmd[0])
            raise RuntimeError(f"{executable} is not installed or is not on PATH.") from exc
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            err = stderr.decode(errors="ignore")[-700:].strip()
            logger.error("[Media] %s failed after %.1fs: %s", stage, time.monotonic() - started_at, err or "unknown error")
            raise Exception(f"FFmpeg error: {err or 'unknown error'}")
        logger.info("[Media] %s completed in %.1fs.", stage, time.monotonic() - started_at)

    @staticmethod
    def _assert_output(path: str) -> None:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            raise Exception("Media processing did not create a valid output file")

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _has_metadata(metadata: dict) -> bool:
        return any(str(v).strip() for v in metadata.values())

    @staticmethod
    def _watermark_enabled(watermark: dict) -> bool:
        return bool(
            watermark.get("enabled") and str(watermark.get("text", "")).strip()
        )

    @staticmethod
    def _append_metadata(cmd: list[str], metadata: dict) -> None:
        mapping = {
            "movie_name": "title",
            "title_all":  "title",
            "artist":     "artist",
            "author":     "author",
            "encoder":    "encoder",
        }
        for key, ff_key in mapping.items():
            value = str(metadata.get(key) or "").strip()
            if value:
                cmd.extend(["-metadata", f"{ff_key}={value}"])

    @staticmethod
    def _position_expr(position: str, padding: int) -> tuple[str, str]:
        pad_x = f"(w*{padding}/100)"
        pad_y = f"(h*{padding}/100)"
        positions = {
            "top_left":  (pad_x,               pad_y),
            "top_mid":   ("(w-text_w)/2",       pad_y),
            "top_right": (f"w-text_w-{pad_x}",  pad_y),
            "mid_left":  (pad_x,                "(h-text_h)/2"),
            "mid_right": (f"w-text_w-{pad_x}",  "(h-text_h)/2"),
            "bot_left":  (pad_x,                f"h-text_h-{pad_y}"),
            "bot_right": (f"w-text_w-{pad_x}",  f"h-text_h-{pad_y}"),
        }
        return positions.get(position, positions["bot_right"])

    def _enable_expr(self, watermark: dict) -> str:
        """Only used for full-file encodes."""
        mode = str(watermark.get("timing_mode", "range"))
        if mode == "full":
            return ""
        if mode == "random_duration":
            duration = self._clamp_int(watermark.get("duration", 30), 1, 3600)
            return f"lt(mod(t\\,{duration * 2})\\,{duration})"
        # range
        start = self._clamp_int(watermark.get("start", 0), 0, 86400)
        end   = self._clamp_int(watermark.get("end",   0), 0, 86400)
        if end > start:
            return f"between(t\\,{start}\\,{end})"
        return ""

    @staticmethod
    def _escape_filter_value(value: str) -> str:
        value = value.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")
        return value.replace("[", "\\[").replace("]", "\\]")

    @staticmethod
    def _safe_color(value: str) -> str:
        value = value.strip().lower()
        if re.fullmatch(r"[a-z]+|#[0-9a-fA-F]{6}", value):
            return value
        return "white"

    @staticmethod
    def _clamp_int(value, low: int, high: int) -> int:
        try:
            number = int(value)
        except (TypeError, ValueError):
            number = low
        return max(low, min(high, number))
