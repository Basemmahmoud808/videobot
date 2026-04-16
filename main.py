"""
╔══════════════════════════════════════════════════════════════╗
║         🎬  VideoBot — TikTok & Instagram Downloader         ║
║        Advanced Edition — aiogram 3 + yt-dlp + FFmpeg        ║
╚══════════════════════════════════════════════════════════════╝

تثبيت:
    pip install aiogram yt-dlp

تشغيل:
    python main.py
"""

import asyncio
import calendar
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Optional

import yt_dlp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ══════════════════════════════════════════════
#  ⚙️  CONFIGURATION
# ══════════════════════════════════════════════

BOT_TOKEN:          str   = os.getenv("BOT_TOKEN", "8746593870:AAH6sfqQPYCxHATvpPL_NbcV1thPnOrIDws")
MAX_VIDEO_SIZE_MB:  int   = 50
MAX_CONCURRENT_DL:  int   = 4        # Semaphore limit — prevents CPU spikes
UPLOAD_TIMEOUT:     int   = 600      # 10 minutes
SOCKET_TIMEOUT:     int   = 30
COOKIES_FILE:       str   = "cookies.txt"
STATS_FILE:         str   = "stats.json"
MIN_DISK_FREE_MB:   int   = 500      # Minimum free disk space before download
RATE_LIMIT_WINDOW:  int   = 60       # Seconds for rate-limit window
RATE_LIMIT_MAX:     int   = 5        # Max requests per user per window
PROGRESS_UPDATE_INTERVAL: float = 3.0  # Seconds between progress message edits

# ══════════════════════════════════════════════
#  📋  LOGGING
# ══════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("VideoBot")

EXECUTOR = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DL, thread_name_prefix="dl")

# Semaphore to cap concurrent downloads and prevent CPU spikes
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DL)

# ══════════════════════════════════════════════
#  🔍  FFMPEG CHECK
# ══════════════════════════════════════════════

def check_ffmpeg() -> bool:
    """Verify FFmpeg is installed and accessible in PATH."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False

FFMPEG_AVAILABLE: bool = check_ffmpeg()

# ══════════════════════════════════════════════
#  🚦  RATE LIMITER
# ══════════════════════════════════════════════

class RateLimiter:
    """
    Per-user token-bucket rate limiter.
    Allows up to RATE_LIMIT_MAX requests per RATE_LIMIT_WINDOW seconds.
    """

    def __init__(self, max_requests: int = RATE_LIMIT_MAX, window: int = RATE_LIMIT_WINDOW):
        self._max      = max_requests
        self._window   = window
        self._buckets: dict[int, list[float]] = defaultdict(list)

    def is_allowed(self, user_id: int) -> bool:
        now = time.monotonic()
        bucket = self._buckets[user_id]
        # Evict timestamps outside current window
        self._buckets[user_id] = [t for t in bucket if now - t < self._window]
        if len(self._buckets[user_id]) >= self._max:
            return False
        self._buckets[user_id].append(now)
        return True

    def seconds_until_reset(self, user_id: int) -> int:
        now = time.monotonic()
        bucket = self._buckets[user_id]
        if not bucket:
            return 0
        oldest = min(bucket)
        return max(0, int(self._window - (now - oldest)))

rate_limiter = RateLimiter()

# ══════════════════════════════════════════════
#  💾  DISK SPACE CHECK
# ══════════════════════════════════════════════

def check_disk_space(min_free_mb: int = MIN_DISK_FREE_MB) -> bool:
    """Return True if there is at least min_free_mb of free disk space."""
    usage = shutil.disk_usage(tempfile.gettempdir())
    free_mb = usage.free / (1024 * 1024)
    logger.info("💾 Free disk: %.0f MB", free_mb)
    return free_mb >= min_free_mb

def get_free_disk_mb() -> float:
    usage = shutil.disk_usage(tempfile.gettempdir())
    return usage.free / (1024 * 1024)

# ══════════════════════════════════════════════
#  📊  PERSISTENT STATS
# ══════════════════════════════════════════════

class BotStats:
    """Stats persisted to stats.json; survive bot restarts."""

    def __init__(self):
        self.total:   int   = 0
        self.success: int   = 0
        self.failed:  int   = 0
        self.data_mb: float = 0.0
        self.active:  int   = 0
        self._monthly: dict = defaultdict(lambda: {"users": [], "downloads": 0})
        self._load()

    def _load(self):
        if Path(STATS_FILE).exists():
            try:
                with open(STATS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.total   = data.get("total", 0)
                self.success = data.get("success", 0)
                self.failed  = data.get("failed", 0)
                self.data_mb = data.get("data_mb", 0.0)
                for k, v in data.get("monthly", {}).items():
                    self._monthly[k]["users"]     = v.get("users", [])
                    self._monthly[k]["downloads"] = v.get("downloads", 0)
                logger.info("📂 Loaded stats from %s", STATS_FILE)
            except Exception as e:
                logger.warning("⚠️ Could not load stats: %s", e)

    def _save(self):
        try:
            data = {
                "total":   self.total,
                "success": self.success,
                "failed":  self.failed,
                "data_mb": self.data_mb,
                "monthly": {
                    k: {"users": list(set(v["users"])), "downloads": v["downloads"]}
                    for k, v in self._monthly.items()
                },
            }
            with open(STATS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("⚠️ Could not save stats: %s", e)

    def _month_key(self) -> str:
        return datetime.now().strftime("%Y-%m")

    def start(self, user_id: int):
        self.total  += 1
        self.active += 1
        key = self._month_key()
        if user_id not in self._monthly[key]["users"]:
            self._monthly[key]["users"].append(user_id)

    def done_ok(self, mb: float):
        self.success += 1
        self.data_mb += mb
        self.active  -= 1
        self._monthly[self._month_key()]["downloads"] += 1
        self._save()

    def done_err(self):
        self.failed += 1
        self.active -= 1
        self._save()

    def monthly_users(self) -> int:
        return len(set(self._monthly[self._month_key()]["users"]))

    def monthly_downloads(self) -> int:
        return self._monthly[self._month_key()]["downloads"]

    def format_report(self) -> str:
        now       = datetime.now()
        month     = now.strftime("%B %Y")
        days_left = calendar.monthrange(now.year, now.month)[1] - now.day
        return (
            f"📊 <b>إحصائيات البوت</b>\n\n"
            f"<b>📅 هذا الشهر ({month}):</b>\n"
            f"👤 مستخدمون فريدون  : <b>{self.monthly_users():,}</b>\n"
            f"⬇️ تحميلات ناجحة    : <b>{self.monthly_downloads():,}</b>\n"
            f"📆 أيام متبقية       : <b>{days_left}</b>\n\n"
            f"<b>📈 إجمالي كل الوقت:</b>\n"
            f"🔢 إجمالي الطلبات   : <b>{self.total:,}</b>\n"
            f"✅ ناجحة             : <b>{self.success:,}</b>\n"
            f"❌ فاشلة             : <b>{self.failed:,}</b>\n"
            f"📦 بيانات محوّلة    : <b>{self.data_mb:.1f} MB</b>\n"
            f"⚡ نشطة الآن        : <b>{self.active}</b>\n\n"
            f"<i>الإحصائيات محفوظة ولا تُمحى عند إعادة التشغيل ✅</i>"
        )

stats = BotStats()

# ══════════════════════════════════════════════
#  🎨  MESSAGES
# ══════════════════════════════════════════════

WELCOME_TEXT = """
🎬 <b>VideoBot — محمّل الفيديوهات</b>

أهلاً بك! 👋 أنا بوت لتحميل الفيديوهات من:
  🎵  <b>TikTok</b>
  📸  <b>Instagram</b> (Reels, Posts & Carousels)

<b>الاستخدام:</b>
  ١. انسخ رابط الفيديو
  ٢. أرسله هنا مباشرةً
  ٣. استلم الفيديو! 🚀

━━━━━━━━━━━━━━━━━━━━━━
<i>💡 الحساب لازم يكون عام وغير مقيّد</i>
<i>🎞️ جميع الفيديوهات متوافقة مع WhatsApp وجميع المنصات</i>
"""

HELP_TEXT = """
📖 <b>دليل الاستخدام</b>

<b>🔗 الروابط المقبولة:</b>
• <code>https://www.tiktok.com/@user/video/123</code>
• <code>https://vm.tiktok.com/XXXXX/</code>
• <code>https://vt.tiktok.com/XXXXX/</code>
• <code>https://www.instagram.com/reel/XXXXX/</code>
• <code>https://www.instagram.com/p/XXXXX/</code>

<b>⚠️ أسباب الفشل الشائعة:</b>
• الحساب خاص أو مقيّد 🔒
• الفيديو محذوف 🗑️
• الرابط منتهي الصلاحية ⏰
• حجم الفيديو أكبر من 50 MB 📦

<b>🎞️ ترميز الفيديو:</b>
• H.264 (libx264) + AAC — متوافق مع كل المنصات
• Pixel Format: yuv420p — ضروري للتشغيل على الموبايل
"""

# ══════════════════════════════════════════════
#  🔗  URL HELPERS
# ══════════════════════════════════════════════

_URL_RE = re.compile(
    r"https?://(www\.)?"
    r"(tiktok\.com|vm\.tiktok\.com|vt\.tiktok\.com|"
    r"instagram\.com|instagr\.am)"
    r"[^\s]+",
    re.IGNORECASE,
)

def extract_url(text: str) -> Optional[str]:
    m = _URL_RE.search(text)
    return m.group(0) if m else None

def platform_label(url: str) -> str:
    u = url.lower()
    if "tiktok"    in u: return "🎵 TikTok"
    if "instagram" in u: return "📸 Instagram"
    return "🌐 Unknown"

# ══════════════════════════════════════════════
#  📊  PROGRESS HOOK
# ══════════════════════════════════════════════

def _build_progress_bar(pct: float, width: int = 10) -> str:
    """Build a Unicode progress bar like ██████░░░░ 63%."""
    filled = int(width * pct / 100)
    bar    = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {pct:.0f}%"

class ProgressHook:
    """
    Thread-safe yt-dlp progress hook that queues updates
    to be consumed by the async progress-update task.
    """

    def __init__(self):
        self.last_text:    str   = ""
        self._last_update: float = 0.0
        self._lock = asyncio.Lock()
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=1)

    def hook(self, d: dict) -> None:
        """Called by yt-dlp in the download thread."""
        if d["status"] == "downloading":
            pct_raw   = d.get("_percent_str", "0%").strip()
            speed_raw = d.get("_speed_str",   "N/A").strip()
            eta_raw   = d.get("_eta_str",     "N/A").strip()
            try:
                pct = float(pct_raw.replace("%", ""))
            except ValueError:
                pct = 0.0

            bar  = _build_progress_bar(pct)
            text = (
                f"⬇️ <b>جاري التحميل...</b>\n\n"
                f"{bar}\n\n"
                f"⚡ السرعة : <b>{speed_raw}</b>\n"
                f"⏱️ المتبقي : <b>{eta_raw}</b>"
            )
            self.last_text = text

            # Non-blocking put — drop if queue is full (prevents stalling download thread)
            try:
                self._queue.put_nowait(text)
            except asyncio.QueueFull:
                pass

        elif d["status"] == "finished":
            self.last_text = "✅ <b>اكتمل التحميل، جاري المعالجة...</b>"
            try:
                self._queue.put_nowait(self.last_text)
            except asyncio.QueueFull:
                pass

    async def consume(self, edit_fn, interval: float = PROGRESS_UPDATE_INTERVAL) -> None:
        """
        Coroutine: drains the queue and calls edit_fn with the latest progress text.
        Runs until cancelled.
        """
        while True:
            try:
                text = await asyncio.wait_for(self._queue.get(), timeout=interval)
                try:
                    await edit_fn(text)
                except Exception:
                    pass  # Ignore Telegram edit errors (e.g., message not modified)
            except asyncio.TimeoutError:
                pass
            await asyncio.sleep(0)

# ══════════════════════════════════════════════
#  ⬇️  DOWNLOAD ENGINE
# ══════════════════════════════════════════════

def _ydl_opts(out: str, progress_hook=None) -> dict:
    """
    Build yt-dlp options with:
    - libx264 + AAC encoding for universal compatibility
    - yuv420p pixel format for mobile / WhatsApp playback
    - faststart (moov atom relocation) for streaming
    - Real-time progress hook
    """
    hooks = [progress_hook] if progress_hook else []

    if FFMPEG_AVAILABLE:
        fmt = (
            "bestvideo[ext=mp4]+bestaudio[ext=m4a]"
            "/bestvideo+bestaudio"
            "/best[ext=mp4]/best"
        )
        postprocessors = [
            # 1. Merge streams
            {
                "key":             "FFmpegVideoConvertor",
                "preferedformat":  "mp4",
            },
            # 2. Re-encode to H.264 + AAC with yuv420p + faststart
            {
                "key":            "FFmpegVideoRemuxer",
                "preferedformat": "mp4",
            },
        ]
        # Extra FFmpeg args for libx264 / yuv420p / faststart
        postprocessor_args = {
            "FFmpegVideoConvertor": [
                "-vcodec",  "libx264",
                "-acodec",  "aac",
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                "-preset",  "fast",
                "-crf",     "23",
            ]
        }
    else:
        fmt                = "best[ext=mp4]/best"
        postprocessors     = []
        postprocessor_args = {}

    opts: dict = {
        "format":                        fmt,
        "outtmpl":                       out,
        "merge_output_format":           "mp4",
        "quiet":                         True,
        "no_warnings":                   True,
        "noplaylist":                    True,
        "concurrent_fragment_downloads": 4,
        "retries":                       5,
        "fragment_retries":              5,
        "socket_timeout":                SOCKET_TIMEOUT,
        "postprocessors":                postprocessors,
        "postprocessor_args":            postprocessor_args,
        "progress_hooks":                hooks,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        },
    }

    if Path(COOKIES_FILE).exists():
        opts["cookiefile"] = COOKIES_FILE

    return opts


def _blocking_download(url: str, out_template: str, progress_hook=None) -> tuple[list[Path], bool]:
    """
    Download URL with yt-dlp.

    Returns:
        (list_of_paths, is_carousel)
        - Single video/image → ([path], False)
        - Instagram carousel  → ([path1, path2, ...], True)
    """
    with yt_dlp.YoutubeDL(_ydl_opts(out_template, progress_hook)) as ydl:
        info = ydl.extract_info(url, download=True)

    # ── Carousel detection ─────────────────────
    entries = info.get("entries")
    if entries:
        # Multi-item post (carousel)
        paths = []
        for entry in entries:
            if not entry:
                continue
            p = Path(ydl.prepare_filename(entry)).with_suffix(".mp4")
            if not p.exists():
                for ext in ("mp4", "mkv", "webm", "mov", "jpg", "jpeg", "png", "webp"):
                    candidates = list(p.parent.glob(f"*.{ext}"))
                    if candidates:
                        p = max(candidates, key=lambda x: x.stat().st_size)
                        break
            if p.exists():
                paths.append(p)
        return paths, True

    # ── Single item ────────────────────────────
    filename = ydl.prepare_filename(info)
    path     = Path(filename).with_suffix(".mp4")

    if not path.exists():
        for ext in ("mp4", "mkv", "webm", "mov"):
            candidates = list(path.parent.glob(f"*.{ext}"))
            if candidates:
                path = max(candidates, key=lambda p: p.stat().st_size)
                break

    if not path.exists():
        raise FileNotFoundError("لم يُعثر على الملف بعد التحميل.")

    return [path], False


async def download_video(
    url: str,
    progress_hook=None,
) -> tuple[list[Path], bool, Path]:
    """
    Async wrapper around _blocking_download.

    Returns:
        (paths, is_carousel, tmp_dir_path)
    """
    tmp_dir      = Path(tempfile.mkdtemp(prefix="vbot_"))
    out_template = str(tmp_dir / "%(id)s.%(ext)s")
    loop         = asyncio.get_running_loop()
    logger.info("⬇️  Start | %s", url)

    paths, is_carousel = await loop.run_in_executor(
        EXECUTOR, _blocking_download, url, out_template, progress_hook
    )
    return paths, is_carousel, tmp_dir

# ══════════════════════════════════════════════
#  ⌨️  KEYBOARDS
# ══════════════════════════════════════════════

def kb_main() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📖 مساعدة",   callback_data="help")
    b.button(text="📊 إحصائيات", callback_data="stats")
    b.adjust(2)
    return b.as_markup()

def kb_error() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📖 مساعدة", callback_data="help")
    b.adjust(1)
    return b.as_markup()

# ══════════════════════════════════════════════
#  📤  SAFE UPLOAD  (single video)
# ══════════════════════════════════════════════

async def safe_send_video(
    message:    Message,
    video_path: Path,
    caption:    str,
    retries:    int = 3,
) -> None:
    for attempt in range(1, retries + 1):
        try:
            await message.reply_video(
                video=FSInputFile(video_path),
                caption=caption,
                parse_mode=ParseMode.HTML,
                supports_streaming=True,
                request_timeout=UPLOAD_TIMEOUT,
            )
            return

        except TelegramRetryAfter as e:
            wait = e.retry_after + 2
            logger.warning("⏳ RateLimit — wait %ds (%d/%d)", wait, attempt, retries)
            await asyncio.sleep(wait)

        except TelegramNetworkError as e:
            if attempt == retries:
                raise
            logger.warning("🌐 NetworkError — retry %d/%d: %s", attempt, retries, e)
            await asyncio.sleep(5)


# ══════════════════════════════════════════════
#  📤  CAROUSEL UPLOAD  (media group)
# ══════════════════════════════════════════════

async def safe_send_carousel(
    message: Message,
    paths:   list[Path],
    caption: str,
    retries: int = 3,
) -> None:
    """
    Send an Instagram carousel as a Telegram media group.
    Supports mixed video + photo items.
    """
    IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

    media_group: list = []
    for i, p in enumerate(paths[:10]):  # Telegram limit: 10 items per group
        is_image = p.suffix.lower() in IMAGE_EXTS
        item_caption = caption if i == 0 else None
        if is_image:
            media_group.append(
                InputMediaPhoto(media=FSInputFile(p), caption=item_caption, parse_mode=ParseMode.HTML)
            )
        else:
            media_group.append(
                InputMediaVideo(
                    media=FSInputFile(p),
                    caption=item_caption,
                    parse_mode=ParseMode.HTML,
                    supports_streaming=True,
                )
            )

    for attempt in range(1, retries + 1):
        try:
            await message.reply_media_group(media=media_group, request_timeout=UPLOAD_TIMEOUT)
            return

        except TelegramRetryAfter as e:
            wait = e.retry_after + 2
            logger.warning("⏳ RateLimit — wait %ds (%d/%d)", wait, attempt, retries)
            await asyncio.sleep(wait)

        except TelegramNetworkError as e:
            if attempt == retries:
                raise
            logger.warning("🌐 NetworkError — retry %d/%d: %s", attempt, retries, e)
            await asyncio.sleep(5)

# ══════════════════════════════════════════════
#  💬  MAIN HANDLER
# ══════════════════════════════════════════════

async def handle_download(message: Message, bot: Bot, url: str) -> None:
    user_id  = message.from_user.id
    platform = platform_label(url)

    # ── Rate limiting ──────────────────────────
    if not rate_limiter.is_allowed(user_id):
        wait = rate_limiter.seconds_until_reset(user_id)
        await message.reply(
            f"⏱️ <b>بطّئ شوي!</b>\n\n"
            f"تجاوزت الحد المسموح. انتظر <b>{wait}s</b> قبل الطلب التالي."
        )
        return

    # ── Disk space check ───────────────────────
    if not check_disk_space():
        free = get_free_disk_mb()
        logger.error("💾 Disk full: %.0f MB free", free)
        await message.reply(
            f"⚠️ <b>مساحة التخزين ممتلئة</b>\n\n"
            f"المساحة الحرة: <b>{free:.0f} MB</b> — المطلوب: <b>{MIN_DISK_FREE_MB} MB</b>\n"
            f"حاول لاحقاً."
        )
        return

    stats.start(user_id)
    await bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    proc_msg = await message.reply(
        f"⏳ <b>جاري التحميل...</b>\n\nالمنصة: {platform}"
    )

    # ── Progress hook setup ────────────────────
    progress = ProgressHook()

    async def edit_progress(text: str) -> None:
        try:
            await proc_msg.edit_text(text, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    tmp_dir:    Optional[Path] = None
    video_paths: list[Path]   = []

    try:
        # ── Semaphore guards concurrency ───────
        async with download_semaphore:
            # Start progress updater alongside the download
            progress_task = asyncio.create_task(
                progress.consume(edit_progress, interval=PROGRESS_UPDATE_INTERVAL)
            )
            try:
                video_paths, is_carousel, tmp_dir = await download_video(
                    url, progress_hook=progress.hook
                )
            finally:
                progress_task.cancel()
                try:
                    await progress_task
                except asyncio.CancelledError:
                    pass

        if not video_paths:
            raise FileNotFoundError("لم يُعثر على أي ملف بعد التحميل.")

        # ── Size check (for single video) ──────
        if not is_carousel:
            size_mb = video_paths[0].stat().st_size / (1024 * 1024)
            logger.info("✅ Downloaded %.1f MB | user=%s", size_mb, user_id)

            if size_mb > MAX_VIDEO_SIZE_MB:
                stats.done_err()
                await proc_msg.edit_text(
                    f"⚠️ <b>الفيديو كبير جداً</b>\n\n"
                    f"الحجم: <b>{size_mb:.1f} MB</b> — الحد: <b>{MAX_VIDEO_SIZE_MB} MB</b>"
                )
                return

            await proc_msg.edit_text(
                f"📤 <b>جاري الرفع...</b>\n\n"
                f"المنصة: {platform} │ الحجم: <b>{size_mb:.1f} MB</b>"
            )
            await bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)

            await safe_send_video(
                message, video_paths[0],
                caption=(
                    f"✅ <b>تم التحميل!</b>\n"
                    f"المنصة: {platform} │ الحجم: <b>{size_mb:.1f} MB</b>\n"
                    f"<i>🎞️ H.264 · AAC · yuv420p · FastStart</i>"
                ),
            )
            stats.done_ok(size_mb)
            logger.info("📤 Sent %.1f MB → user=%s", size_mb, user_id)

        else:
            # ── Carousel ───────────────────────
            total_mb = sum(p.stat().st_size for p in video_paths) / (1024 * 1024)
            count    = len(video_paths)
            logger.info("🖼️ Carousel: %d items (%.1f MB) | user=%s", count, total_mb, user_id)

            await proc_msg.edit_text(
                f"📤 <b>جاري رفع {count} عنصر...</b>\n\nالمنصة: {platform}"
            )
            await bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_DOCUMENT)

            await safe_send_carousel(
                message, video_paths,
                caption=(
                    f"✅ <b>تم التحميل! ({count} عنصر)</b>\n"
                    f"المنصة: {platform} │ الحجم: <b>{total_mb:.1f} MB</b>"
                ),
            )
            stats.done_ok(total_mb)
            logger.info("📤 Sent carousel %d items → user=%s", count, user_id)

    except yt_dlp.utils.DownloadError as e:
        stats.done_err()
        err = str(e).lower()
        logger.error("❌ DownloadError: %s", e)

        if any(k in err for k in ("private", "login", "age", "restricted")):
            text = "🔒 <b>المحتوى محمي</b>\n\nهذا الفيديو خاص أو يتطلب تسجيل دخول."
        elif any(k in err for k in ("unavailable", "deleted", "removed", "does not exist")):
            text = "🗑️ <b>المحتوى غير متاح</b>\n\nتم حذف هذا الفيديو من المنصة."
        elif "ffmpeg" in err:
            text = (
                "⚙️ <b>FFmpeg غير مثبّت</b>\n\n"
                "لتحميل الفيديو بجودة عالية، ثبّت FFmpeg:\n"
                "🪟 Windows: <code>winget install ffmpeg</code>\n"
                "🐧 Linux:   <code>sudo apt install ffmpeg</code>\n"
                "🍎 macOS:   <code>brew install ffmpeg</code>"
            )
        else:
            text = "❌ <b>فشل التحميل</b>\n\nتحقق من الرابط وحاول مرة أخرى."

        await message.reply(text, reply_markup=kb_error())

    except FileNotFoundError:
        stats.done_err()
        await message.reply("⚠️ <b>خطأ داخلي</b>\n\nلم يُعثر على الملف بعد التحميل.")

    except TelegramNetworkError as e:
        stats.done_err()
        logger.error("🌐 TelegramNetworkError: %s", e)
        await message.reply("🌐 <b>خطأ في الشبكة</b>\n\nفشل رفع الفيديو. حاول مرة أخرى.")

    except Exception as e:
        stats.done_err()
        logger.exception("💥 Unexpected: %s", e)
        await message.reply("😕 <b>خطأ غير متوقع</b>\n\nحاول مرة أخرى لاحقاً.")

    finally:
        # ── Guaranteed cleanup ─────────────────
        try:
            await bot.delete_message(proc_msg.chat.id, proc_msg.message_id)
        except Exception:
            pass
        if tmp_dir and tmp_dir.exists():
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                logger.debug("🗑️ Cleaned up %s", tmp_dir)
            except Exception as e:
                logger.warning("⚠️ Cleanup failed for %s: %s", tmp_dir, e)

# ══════════════════════════════════════════════
#  🚀  BOT STARTUP
# ══════════════════════════════════════════════

async def main() -> None:
    if FFMPEG_AVAILABLE:
        logger.info("✅ FFmpeg متاح — H.264 + AAC + yuv420p مفعّل")
    else:
        logger.warning(
            "⚠️  FFmpeg غير موجود! الفيديوهات ستُحمَّل بجودة أقل وقد تكون غير متوافقة.\n"
            "    Windows → winget install ffmpeg\n"
            "    Linux   → sudo apt install ffmpeg\n"
            "    macOS   → brew install ffmpeg"
        )

    session = AiohttpSession(timeout=UPLOAD_TIMEOUT)
    bot     = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    # ── Commands ───────────────────────────────
    @dp.message(CommandStart())
    async def cmd_start(message: Message):
        await message.answer(WELCOME_TEXT, reply_markup=kb_main())

    @dp.message(Command("help"))
    async def cmd_help(message: Message):
        await message.answer(HELP_TEXT)

    @dp.message(Command("stats"))
    async def cmd_stats(message: Message):
        await message.answer(stats.format_report())

    # ── Callbacks ──────────────────────────────
    @dp.callback_query(F.data == "help")
    async def cb_help(cb: CallbackQuery):
        await cb.message.answer(HELP_TEXT)
        await cb.answer()

    @dp.callback_query(F.data == "stats")
    async def cb_stats(cb: CallbackQuery):
        await cb.message.answer(stats.format_report())
        await cb.answer()

    # ── Text handler ───────────────────────────
    @dp.message(F.text)
    async def on_text(message: Message):
        url = extract_url(message.text)
        if url:
            asyncio.create_task(handle_download(message, bot, url))
        else:
            await message.reply(
                "🤔 <b>رابط غير مدعوم</b>\n\nأرسل رابط TikTok أو Instagram.\n/help للمساعدة.",
                reply_markup=kb_error(),
            )

    logger.info(
        "🤖 VideoBot running | FFmpeg=%s | MaxConcurrent=%d | RateLimit=%d/%ds",
        FFMPEG_AVAILABLE, MAX_CONCURRENT_DL, RATE_LIMIT_MAX, RATE_LIMIT_WINDOW,
    )
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
