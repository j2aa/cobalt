#!/usr/bin/env python3
"""Authenticated, persistent LibreGet-compatible media worker for EduShareVN."""

from __future__ import annotations

import hashlib
import hmac
import http.client
import http.server
import importlib.util
import ipaddress
import json
import math
import mimetypes
import os
import queue
import re
import shutil
import signal
import ssl
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlsplit, urlunsplit

HOST = os.getenv("DOWNLOAD_WORKER_HOST", "0.0.0.0")
PORT = int(os.getenv("DOWNLOAD_WORKER_PORT") or os.getenv("PORT") or "8787")
DATA_DIR = Path(os.getenv("DOWNLOAD_DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "jobs.sqlite3"
OUTPUT_DIR = DATA_DIR / "files"
SECRET = os.getenv("DOWNLOAD_WORKER_SECRET", "")
ALLOW_ALL_PUBLIC_SITES = os.getenv("DOWNLOAD_WORKER_ALL_SITES", "1") != "0"
MAX_BODY = 8192
MAX_FILE_BYTES = int(os.getenv("DOWNLOAD_MAX_FILE_BYTES", str(512 * 1024 * 1024)))
MAX_USER_JOBS = 8
MAX_USER_STORAGE = 2 * 1024 * 1024 * 1024
MAX_GLOBAL_STORAGE = 4 * 1024 * 1024 * 1024
HISTORY_RETENTION_SECONDS = 30 * 24 * 60 * 60
COMPLETED_FILE_TTL_SECONDS = int(os.getenv("DOWNLOAD_COMPLETED_TTL_SECONDS", "600"))
DELETE_AFTER_STREAM = os.getenv("DOWNLOAD_DELETE_AFTER_STREAM", "1") != "0"
MAX_JOB_SECONDS = 45 * 60
MAX_JOB_ATTEMPTS = 4
MAX_WORKERS = max(1, int(os.getenv("DOWNLOAD_MAX_WORKERS", "1")))
MAX_THUMBNAIL_BYTES = 192 * 1024
SPEED_LIMITS_BPS = {0, 262_144, 524_288, 1_048_576, 5_242_880, 10_485_760}
AUDIO_OUTPUT_FORMATS = {"mp3", "m4a", "wav", "flac"}
RETRY_TTL_SECONDS = 5 * 60
ACTIVE: set[str] = set()
PROCESSES: dict[str, subprocess.Popen[str]] = {}
CONTROLS: dict[str, dict] = {}
LOCK = threading.RLock()
RATE: dict[str, list[float]] = {}
EXECUTOR = None


def db():
    class ClosingConnection:
        def __enter__(self):
            self.connection = sqlite3.connect(DB_PATH, timeout=10)
            self.connection.row_factory = sqlite3.Row
            return self.connection.__enter__()

        def __exit__(self, *args):
            try:
                return self.connection.__exit__(*args)
            finally:
                self.connection.close()
    return ClosingConnection()


def init_db():
    DATA_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    (DATA_DIR / "tmp").mkdir(mode=0o700, parents=True, exist_ok=True)
    with db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, user_id TEXT NOT NULL, url TEXT NOT NULL,
                title TEXT NOT NULL, platform TEXT NOT NULL, format_id TEXT NOT NULL,
                status TEXT NOT NULL, progress REAL NOT NULL DEFAULT 0,
                speed_bps INTEGER NOT NULL DEFAULT 0, downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                total_bytes INTEGER, eta_seconds INTEGER, file_name TEXT, size_bytes INTEGER NOT NULL DEFAULT 0,
                error TEXT, rate_limit_bps INTEGER NOT NULL DEFAULT 0,
                attempt_count INTEGER NOT NULL DEFAULT 0, analysis_id TEXT,
                retry_expires_at INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS jobs_owner_created ON jobs(user_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status, created_at);
            CREATE TABLE IF NOT EXISTS analyses (
                id TEXT PRIMARY KEY, user_id TEXT NOT NULL, url TEXT NOT NULL,
                title TEXT NOT NULL, platform TEXT NOT NULL, duration REAL,
                formats TEXT NOT NULL, expires_at INTEGER NOT NULL, thumbnail_data TEXT,
                consumed INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS request_nonces (
                user_id TEXT NOT NULL, nonce TEXT NOT NULL, expires_at INTEGER NOT NULL,
                PRIMARY KEY(user_id, nonce)
            );
            CREATE TABLE IF NOT EXISTS download_tickets (
                nonce TEXT PRIMARY KEY, expires_at INTEGER NOT NULL
            );
        """)
        job_columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
        if "rate_limit_bps" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN rate_limit_bps INTEGER NOT NULL DEFAULT 0")
        if "attempt_count" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0")
        if "analysis_id" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN analysis_id TEXT")
        if "retry_expires_at" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN retry_expires_at INTEGER NOT NULL DEFAULT 0")
        analysis_columns = {row["name"] for row in conn.execute("PRAGMA table_info(analyses)")}
        if "thumbnail_data" not in analysis_columns:
            conn.execute("ALTER TABLE analyses ADD COLUMN thumbnail_data TEXT")
        if "consumed" not in analysis_columns:
            conn.execute("ALTER TABLE analyses ADD COLUMN consumed INTEGER NOT NULL DEFAULT 0")
        # yt-dlp's .part files are resumable; interrupted work returns to the queue.
        conn.execute("UPDATE jobs SET status='queued', updated_at=? WHERE status IN ('downloading','paused')", (int(time.time()),))
    try:
        DB_PATH.chmod(0o600)
    except OSError:
        pass


def clean_expired():
    now_ts = int(time.time())
    cutoff = now_ts - HISTORY_RETENTION_SECONDS
    stale_file_cutoff = now_ts - COMPLETED_FILE_TTL_SECONDS
    with db() as conn:
        conn.execute("DELETE FROM analyses WHERE expires_at < ?", (now_ts,))
        conn.execute("DELETE FROM request_nonces WHERE expires_at < ?", (now_ts,))
        conn.execute("DELETE FROM download_tickets WHERE expires_at < ?", (now_ts,))
        stale_completed = conn.execute(
            "SELECT id,user_id FROM jobs WHERE status='completed' AND size_bytes>0 AND updated_at<?",
            (stale_file_cutoff,),
        ).fetchall()
        if stale_completed:
            conn.executemany("UPDATE jobs SET size_bytes=0 WHERE id=?", [(row["id"],) for row in stale_completed])
        expired = conn.execute("SELECT id,user_id FROM jobs WHERE created_at<? AND status NOT IN ('queued','downloading','paused')", (cutoff,)).fetchall()
        conn.executemany("DELETE FROM jobs WHERE id=?", [(row["id"],) for row in expired])
    for row in stale_completed:
        shutil.rmtree(OUTPUT_DIR / row["user_id"] / row["id"], ignore_errors=True)
    for row in expired:
        shutil.rmtree(OUTPUT_DIR / row["user_id"] / row["id"], ignore_errors=True)


def validate_url(raw: object) -> str:
    if not isinstance(raw, str) or not raw or len(raw) > 4096:
        raise ValueError("invalid_url")
    parsed = urlsplit(raw.strip())
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("invalid_url")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid_url") from exc
    if port not in (None, 80, 443):
        raise ValueError("unsupported_url")
    host = parsed.hostname.rstrip(".").encode("idna").decode("ascii")
    resolve_public_addresses(host, port or (443 if parsed.scheme == "https" else 80))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", parsed.query, ""))


def resolve_public_addresses(host: str, port: int) -> list[str]:
    try:
        addresses = [str(ipaddress.ip_address(host))]
    except ValueError:
        try:
            addresses = list(dict.fromkeys(ipaddress.ip_address(item[4][0]).compressed for item in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)))
        except OSError as exc:
            raise ValueError("host_unavailable") from exc
    if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise ValueError("unsupported_url")
    return addresses


class PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host: str, port: int, addresses: list[str], timeout: int = 25):
        super().__init__(host, port, timeout=timeout, context=ssl.create_default_context())
        self.addresses = addresses

    def connect(self):
        last_error = None
        for address in self.addresses:
            try:
                raw = socket.create_connection((address, self.port), self.timeout)
                self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
                return
            except OSError as exc:
                last_error = exc
        raise last_error or OSError("no_public_address")


class PinnedHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host: str, port: int, addresses: list[str], timeout: int = 25):
        super().__init__(host, port, timeout=timeout)
        self.addresses = addresses

    def connect(self):
        last_error = None
        for address in self.addresses:
            try:
                self.sock = socket.create_connection((address, self.port), self.timeout)
                return
            except OSError as exc:
                last_error = exc
        raise last_error or OSError("no_public_address")


def open_public_response(raw_url: str, headers: dict | None = None, timeout: int = 25):
    url = validate_url(raw_url)
    for _ in range(6):
        parsed = urlsplit(url)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        host = parsed.hostname or ""
        addresses = resolve_public_addresses(host, port)
        connection = (PinnedHTTPSConnection(host, port, addresses, timeout) if parsed.scheme == "https"
                      else PinnedHTTPConnection(host, port, addresses, timeout))
        path = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
        try:
            connection.request("GET", path, headers={"User-Agent": "EduShareVN/1.0 media-download", "Accept": "*/*", **(headers or {})})
            response = connection.getresponse()
        except Exception:
            connection.close()
            raise
        if response.status in (301, 302, 303, 307, 308):
            location = response.getheader("Location")
            response.read(1)
            connection.close()
            if not location:
                raise ValueError("redirect_invalid")
            url = validate_url(urljoin(url, location))
            continue
        return connection, response, url
    raise ValueError("redirect_limit")


def direct_file_url(url: str) -> bool:
    return Path(urlsplit(url).path).suffix.lower() in {
        ".mp3", ".m4a", ".aac", ".flac", ".wav", ".ogg", ".opus",
        ".mp4", ".m4v", ".mov", ".mkv", ".webm", ".avi", ".mpeg", ".mpg",
        ".pdf", ".zip", ".epub", ".jpg", ".jpeg", ".png", ".webp", ".gif",
    }


def thumbnail_data_url(raw_url: object) -> str | None:
    if not isinstance(raw_url, str) or len(raw_url) > 4096:
        return None
    connection = response = None
    try:
        connection, response, _ = open_public_response(
            raw_url, {"Range": f"bytes=0-{MAX_THUMBNAIL_BYTES - 1}"}, timeout=5)
        content_type = (response.getheader("Content-Type") or "").split(";", 1)[0].lower()
        signatures = {
            "image/jpeg": lambda data: data.startswith(b"\xff\xd8\xff"),
            "image/png": lambda data: data.startswith(b"\x89PNG\r\n\x1a\n"),
            "image/webp": lambda data: len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP",
            "image/gif": lambda data: data.startswith((b"GIF87a", b"GIF89a")),
        }
        signature = signatures.get(content_type)
        if response.status not in (200, 206) or not signature:
            return None
        total = response.getheader("Content-Range", "").rsplit("/", 1)[-1]
        if total.isdigit() and int(total) > MAX_THUMBNAIL_BYTES:
            return None
        content_length = response.getheader("Content-Length", "")
        if content_length.isdigit() and int(content_length) > MAX_THUMBNAIL_BYTES:
            return None
        data = response.read(MAX_THUMBNAIL_BYTES + 1)
        if not data or len(data) > MAX_THUMBNAIL_BYTES or not signature(data):
            return None
        import base64
        return f"data:{content_type};base64,{base64.b64encode(data).decode('ascii')}"
    except Exception:
        return None
    finally:
        if response:
            response.close()
        if connection:
            connection.close()


KNOWN_PLATFORM_DOMAINS = (
    "youtube.com", "youtu.be", "youtube-nocookie.com",
    "vimeo.com", "bilibili.com", "b23.tv",
    "tiktok.com", "douyin.com", "iesdouyin.com",
    "instagram.com", "instagr.am", "threads.net",
    "facebook.com", "fb.watch", "fb.com",
    "x.com", "twitter.com", "reddit.com", "redd.it",
    "soundcloud.com", "bandcamp.com", "dailymotion.com", "dai.ly",
    "twitch.tv", "kick.com", "pinterest.com", "pin.it",
    "ted.com", "loom.com", "nicovideo.jp", "vk.com",
)

YOUTUBE_JUNK_PARAMS = {
    "list", "index", "pp", "ab_channel", "si", "feature",
    "start_radio", "rv", "playnext", "embeds_referring_euri", "source_ve_path",
}
UNIVERSAL_JUNK_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "utm_id",
    "fbclid", "gclid", "dclid", "mc_cid", "mc_eid", "ref_src", "ref_url",
}
SOCIAL_JUNK_PARAMS = {
    "is_from_webapp", "sender_device", "sender_web_id", "web_id", "_r", "_t",
    "u_code", "share_app_id", "share_item_id", "share_link_id", "checksum",
    "mibextid", "rdid", "share_url", "igsh", "igshid",
    "spm_id_from", "vd_source", "from_spmid", "share_source", "buvid",
}


def has_impersonate() -> bool:
    return importlib.util.find_spec("curl_cffi") is not None


def is_youtube_url(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower().rstrip(".")
    return any(host == d or host.endswith("." + d) for d in ("youtube.com", "youtu.be", "youtube-nocookie.com"))


def needs_impersonate(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower().rstrip(".")
    return any(host == d or host.endswith("." + d) for d in ("tiktok.com", "douyin.com", "instagram.com", "facebook.com", "fb.watch"))


def normalize_media_url(url: str) -> str:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    segments = [seg for seg in parsed.path.split("/") if seg]
    query_pairs = parse_qsl(parsed.query, keep_blank_values=False)

    if is_youtube_url(url):
        video_id = None
        if host == "youtu.be" or host.endswith(".youtu.be"):
            video_id = segments[0] if segments else None
        elif segments and segments[0] in ("shorts", "embed", "live", "v") and len(segments) >= 2:
            video_id = segments[1]
        elif parsed.path == "/watch" or (segments and segments[0] == "watch"):
            video_id = dict(query_pairs).get("v")
        if video_id and re.fullmatch(r"[A-Za-z0-9_-]{6,20}", video_id):
            return f"https://www.youtube.com/watch?v={video_id}"
        cleaned_query = urlencode([(k, v) for k, v in query_pairs if k not in YOUTUBE_JUNK_PARAMS and k not in UNIVERSAL_JUNK_PARAMS])
        netloc = "www.youtube.com" if host in ("m.youtube.com", "music.youtube.com") else parsed.netloc
        return urlunsplit((parsed.scheme, netloc, parsed.path or "/", cleaned_query, ""))

    cleaned_query = urlencode([(k, v) for k, v in query_pairs if k not in UNIVERSAL_JUNK_PARAMS and k not in SOCIAL_JUNK_PARAMS])
    netloc = parsed.netloc
    if host in ("twitter.com", "www.twitter.com", "mobile.twitter.com", "mobile.x.com", "vxtwitter.com", "fxtwitter.com"):
        netloc = "x.com"
    elif host in ("m.facebook.com", "mbasic.facebook.com"):
        netloc = "www.facebook.com"
    elif host == "m.bilibili.com":
        netloc = "www.bilibili.com"
    elif host == "m.soundcloud.com":
        netloc = "soundcloud.com"
    return urlunsplit((parsed.scheme, netloc, parsed.path or "/", cleaned_query, ""))


def platform_url(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower().rstrip(".")
    return any(host == domain or host.endswith("." + domain) for domain in KNOWN_PLATFORM_DOMAINS)


def analyze_direct(url: str) -> dict:
    connection, response, final_url = open_public_response(url, {"Range": "bytes=0-0"})
    try:
        if response.status not in (200, 206):
            raise RuntimeError("source_unavailable")
        content_type = (response.getheader("Content-Type") or "application/octet-stream").split(";", 1)[0].lower()
        if content_type in ("text/html", "application/json", "application/xml") or content_type.endswith("+json"):
            raise ValueError("direct_file_required")
        raw_size = response.getheader("Content-Range", "").rsplit("/", 1)[-1]
        if not raw_size.isdigit():
            raw_size = response.getheader("Content-Length", "")
        size = int(raw_size) if raw_size.isdigit() else None
        if size is not None and size > MAX_FILE_BYTES:
            raise ValueError("file_too_large")
        disposition = response.getheader("Content-Disposition", "")
        match = re.search(r"filename\*=UTF-8''([^;]+)|filename=\"?([^\";]+)", disposition, re.I)
        name = unquote(match.group(1) or match.group(2)).strip() if match else Path(unquote(urlsplit(final_url).path)).name
        name = re.sub(r"[\\/:*?\"<>|\x00-\x1f]", "_", name).strip(" .")[:180] or "download"
        formats = [{"id": "direct", "label": "Tệp gốc", "height": None,
                    "ext": Path(name).suffix.lstrip(".")[:12], "size": size,
                    "hasAudio": content_type.startswith("audio/"), "hasVideo": content_type.startswith("video/")}]
        if content_type.startswith(("audio/", "video/")) and shutil.which("ffmpeg"):
            formats.extend({"id": f"edushare-audio-{ext}", "label": f"Âm thanh · {ext.upper()}", "height": None, "ext": ext, "size": None, "hasAudio": True, "hasVideo": False} for ext in sorted(AUDIO_OUTPUT_FORMATS))
        return {"sourceUrl": final_url, "title": name, "platform": "Direct URL", "duration": None,
                "formats": formats}
    finally:
        response.close()
        connection.close()


def verify_signature(handler: http.server.BaseHTTPRequestHandler, body: bytes) -> str:
    user_id = handler.headers.get("X-Edushare-User", "")
    timestamp = handler.headers.get("X-Edushare-Timestamp", "")
    nonce = handler.headers.get("X-Edushare-Nonce", "")
    signature = handler.headers.get("X-Edushare-Signature", "")
    if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", user_id) or not timestamp.isdigit() or not re.fullmatch(r"[0-9a-f]{32}", nonce):
        raise PermissionError("unauthorized")
    now = int(time.time())
    if abs(now - int(timestamp)) > 180:
        raise PermissionError("unauthorized")
    payload_hash = hashlib.sha256(body).hexdigest()
    message = f"{user_id}\n{timestamp}\n{nonce}\n{handler.command}\n{handler.path}\n{payload_hash}".encode()
    expected = hmac.new(SECRET.encode(), message, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise PermissionError("unauthorized")
    with LOCK, db() as conn:
        try:
            conn.execute("INSERT INTO request_nonces VALUES(?,?,?)", (user_id, nonce, now + 240))
        except sqlite3.IntegrityError as exc:
            raise PermissionError("unauthorized") from exc
        recent = [event for event in RATE.get(user_id, []) if event > now - 60]
        limit = 120 if handler.command == "GET" else 30
        if len(recent) >= limit:
            raise OverflowError("rate_limited")
        RATE[user_id] = recent + [now]
    return user_id


def extractor_env() -> dict[str, str]:
    tmp_dir = str((DATA_DIR / "tmp").resolve())
    child_env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": tmp_dir,
        "TMPDIR": tmp_dir,
        "MALLOC_ARENA_MAX": "2",
    }
    if os.name == "nt":
        child_env.update({name: os.environ[name] for name in ("SYSTEMROOT", "WINDIR", "TEMP", "TMP", "PATHEXT") if name in os.environ})
    for name, value in os.environ.items():
        if name.lower() in {"http_proxy", "https_proxy", "all_proxy", "no_proxy", "ssl_cert_file", "ssl_cert_dir", "requests_ca_bundle"}:
            child_env[name] = value
    return child_env


def ytdlp_antiblock_args(url: str, fallback: bool = False) -> list[str]:
    args: list[str] = []
    if is_youtube_url(url):
        clients = "web_safari,mweb,web" if fallback else "ios,web,mweb"
        args.extend(("--extractor-args", f"youtube:player_client={clients}"))
    elif has_impersonate() and needs_impersonate(url):
        args.extend(("--impersonate", "chrome"))
    return args


def _codec_rank(vcodec: object) -> int:
    code = str(vcodec or "").lower()
    if code.startswith(("avc", "h264")):
        return 0
    if code.startswith(("hev", "hvc", "h265")):
        return 1
    if code.startswith(("vp9", "vp09")):
        return 2
    if code.startswith(("av01", "av1")):
        return 3
    return 4


def _normalize_fps(fps: object) -> int:
    if not isinstance(fps, (int, float)) or fps <= 0:
        return 0
    for target in (24, 25, 30, 48, 50, 60, 120):
        if abs(float(fps) - target) <= 1.0:
            return target
    return int(round(float(fps)))


def run_probe(url: str) -> dict:
    base_cmd = [
        sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-cookies", "--no-cache-dir", "--no-playlist",
        "--no-warnings", "--skip-download", "--dump-single-json", "--socket-timeout", "15",
    ]
    if shutil.which("node"):
        base_cmd.extend(("--js-runtimes", "node"))

    attempts = [ytdlp_antiblock_args(url, fallback=False)]
    if is_youtube_url(url):
        attempts.append(ytdlp_antiblock_args(url, fallback=True))
        attempts.append([])

    info = None
    last_exc: Exception | None = None
    for extra_args in attempts:
        command = [*base_cmd, *extra_args, url]
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=40, env=extractor_env())
            first_line = next((line for line in result.stdout.splitlines() if line.strip().startswith("{")), result.stdout)
            info = json.loads(first_line)
            break
        except FileNotFoundError as exc:
            raise RuntimeError("worker_unavailable") from exc
        except subprocess.TimeoutExpired as exc:
            last_exc = exc
        except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            last_exc = exc

    if info is None:
        if isinstance(last_exc, subprocess.TimeoutExpired):
            raise RuntimeError("source_timeout") from last_exc
        # Never return raw yt-dlp output: extractor errors may contain signed media URLs.
        raise RuntimeError("source_unavailable") from last_exc

    raw_formats = info.get("formats") or []
    duration = info.get("duration") if isinstance(info.get("duration"), (int, float)) else None
    video_candidates = []
    other_formats = []
    seen_ids: set[str] = set()

    for item in raw_formats:
        format_id = item.get("format_id")
        if not isinstance(format_id, str) or not re.fullmatch(r"[A-Za-z0-9+._/-]{1,100}", format_id) or format_id in seen_ids:
            continue
        seen_ids.add(format_id)
        ext = str(item.get("ext") or "")[:12]
        if ext in ("mhtml", "vtt", "srt", "ttml"):
            continue
        height = item.get("height") if isinstance(item.get("height"), int) and item.get("height") > 0 else None
        has_audio = item.get("acodec") not in (None, "none")
        has_video = item.get("vcodec") not in (None, "none") or height is not None
        size = item.get("filesize") or item.get("filesize_approx")
        if not isinstance(size, int) and duration and isinstance(item.get("tbr"), (int, float)) and item["tbr"] > 0:
            size = int(math.floor(float(item["tbr"]) * 1000 * float(duration) / 8))
        size_val = size if isinstance(size, int) and size > 0 else None

        if has_video and height:
            fps = _normalize_fps(item.get("fps"))
            quality_tag = "4K (2160p)" if height >= 2160 else "2K (1440p)" if height >= 1440 else f"{height}p"
            fps_tag = f" {fps}fps" if fps > 30 else ""
            ext_tag = f" · {ext.upper()}" if ext else ""
            video_candidates.append({
                "id": format_id,
                "label": f"{quality_tag}{fps_tag}{ext_tag}",
                "height": height,
                "ext": ext or "mp4",
                "size": size_val,
                "hasAudio": has_audio,
                "hasVideo": True,
                "_fps": fps,
                "_rank": _codec_rank(item.get("vcodec")),
                "_tbr": float(item.get("tbr") or 0),
                "_has_audio_bonus": 0 if has_audio else 1,
            })
        elif has_audio and not has_video:
            abr = item.get("abr")
            abr_tag = f" · {int(round(float(abr)))} kbps" if isinstance(abr, (int, float)) and abr > 0 else ""
            other_formats.append({
                "id": format_id,
                "label": f"Âm thanh gốc ({(ext or 'audio').upper()}{abr_tag})",
                "height": None,
                "ext": ext,
                "size": size_val,
                "hasAudio": True,
                "hasVideo": False,
            })
        else:
            other_formats.append({
                "id": format_id,
                "label": str(item.get("format_note") or ext or "Tự động")[:80],
                "height": height,
                "ext": ext,
                "size": size_val,
                "hasAudio": has_audio,
                "hasVideo": has_video,
            })

    video_candidates.sort(key=lambda item: (-item["height"], -item["_fps"], item["_rank"], item["_has_audio_bonus"], -item["_tbr"]))
    deduped_videos = []
    seen_buckets: set[tuple[int, int]] = set()
    for item in video_candidates:
        bucket = (item["height"], item["_fps"])
        if bucket in seen_buckets:
            continue
        seen_buckets.add(bucket)
        deduped_videos.append({k: v for k, v in item.items() if not k.startswith("_")})

    formats = [
        {"id": "best", "label": "Chất lượng cao nhất khả dụng (MP4)", "height": None, "ext": "mp4", "size": None, "hasAudio": True, "hasVideo": True},
        *deduped_videos,
        *other_formats[:12],
    ]
    if shutil.which("ffmpeg"):
        formats.extend(
            {"id": f"edushare-audio-{ext}", "label": f"Chuyển đổi âm thanh · {ext.upper()}", "height": None, "ext": ext, "size": None, "hasAudio": True, "hasVideo": False}
            for ext in sorted(AUDIO_OUTPUT_FORMATS)
        )
    thumbnail = info.get("thumbnail") or next((item.get("url") for item in reversed(info.get("thumbnails", [])) if isinstance(item, dict) and isinstance(item.get("url"), str)), None)
    audio_formats = [item for item in formats if item["id"].startswith("edushare-audio-")]
    regular_formats = [item for item in formats if not item["id"].startswith("edushare-audio-")]
    return {
        "sourceUrl": url,
        "title": str(info.get("title") or "Nội dung đa phương tiện")[:240],
        "platform": str(info.get("extractor_key") or info.get("extractor") or "Web")[:80],
        "duration": duration,
        "thumbnailData": thumbnail_data_url(thumbnail),
        "formats": regular_formats[:80 - len(audio_formats)] + audio_formats,
    }


def analyze_url(raw: object) -> tuple[str, dict]:
    url = normalize_media_url(validate_url(raw))
    if direct_file_url(url):
        return url, analyze_direct(url)
    if not ALLOW_ALL_PUBLIC_SITES and not platform_url(url):
        raise ValueError("unsupported_platform")
    return url, run_probe(url)


def rate_limit(value: object) -> int:
    if type(value) is not int or value not in SPEED_LIMITS_BPS:
        raise ValueError("invalid_rate_limit")
    return value


def job_capacity_error(conn: sqlite3.Connection, user_id: str) -> str | None:
    count = conn.execute("SELECT COUNT(*) FROM jobs WHERE user_id=? AND status IN ('queued','downloading','paused')", (user_id,)).fetchone()[0]
    if count >= MAX_USER_JOBS:
        return "queue_limit"
    used = conn.execute("SELECT COALESCE(SUM(size_bytes),0) FROM jobs WHERE user_id=? AND status='completed'", (user_id,)).fetchone()[0]
    if used + (count + 1) * MAX_FILE_BYTES > MAX_USER_STORAGE:
        return "storage_limit"
    global_used = conn.execute("SELECT COALESCE(SUM(size_bytes),0) FROM jobs WHERE status='completed'").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','downloading','paused')").fetchone()[0]
    if global_used + (pending + 1) * MAX_FILE_BYTES > MAX_GLOBAL_STORAGE:
        return "worker_storage_limit"
    return None


def row_json(row: sqlite3.Row) -> dict:
    result = {key: row[key] for key in (
        "id", "title", "platform", "format_id", "status", "progress", "speed_bps",
        "downloaded_bytes", "total_bytes", "eta_seconds", "file_name", "size_bytes",
        "error", "rate_limit_bps", "attempt_count", "created_at", "updated_at",
    )}
    result["retryable"] = row["status"] == "failed" and row["attempt_count"] < MAX_JOB_ATTEMPTS and row["retry_expires_at"] >= int(time.time())
    return result


def update_job(job_id: str, **values):
    if not values:
        return
    now = int(time.time())
    if values.get("status") == "failed":
        values["retry_expires_at"] = now + RETRY_TTL_SECONDS
    values["updated_at"] = now
    columns = ",".join(f"{key}=?" for key in values)
    with db() as conn:
        conn.execute(f"UPDATE jobs SET {columns} WHERE id=?", (*values.values(), job_id))
        if values.get("status") == "failed":
            conn.execute("UPDATE analyses SET expires_at=? WHERE id=(SELECT analysis_id FROM jobs WHERE id=?)", (now + RETRY_TTL_SECONDS, job_id))
        if values.get("status") in ("completed", "cancelled"):
            conn.execute("DELETE FROM analyses WHERE id=(SELECT analysis_id FROM jobs WHERE id=?)", (job_id,))


def throttle_download(downloaded: int, started: float, limit_bps: int):
    if limit_bps:
        time.sleep(max(0, downloaded / limit_bps - (time.monotonic() - started)))


def stop_extractor(process: subprocess.Popen[str], force: bool = False):
    if os.name == "nt":
        if process.poll() is None:
            (process.kill if force else process.terminate)()
        return
    try:
        os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
    except ProcessLookupError:
        pass


def pause_extractor(process: subprocess.Popen[str]):
    if os.name == "nt" or not hasattr(signal, "SIGSTOP"):
        return
    try:
        os.killpg(process.pid, signal.SIGSTOP)
    except ProcessLookupError:
        pass


def resume_extractor(process: subprocess.Popen[str]):
    if os.name == "nt" or not hasattr(signal, "SIGCONT"):
        return
    try:
        os.killpg(process.pid, signal.SIGCONT)
    except ProcessLookupError:
        pass


def extractor_command(row: sqlite3.Row, output: str) -> list[str]:
    selected = row["format_id"]
    audio_format = selected.removeprefix("edushare-audio-") if selected.startswith("edushare-audio-") else None
    if audio_format and audio_format not in AUDIO_OUTPUT_FORMATS:
        raise ValueError("invalid_audio_format")
    has_ffmpeg = bool(shutil.which("ffmpeg"))
    if audio_format:
        format_id = "bestaudio/best"
    elif selected == "best":
        format_id = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo*+bestaudio/best" if has_ffmpeg else "best[ext=mp4]/best"
    else:
        format_id = selected
    command = [
        sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-cookies", "--no-cache-dir", "--no-playlist", "--no-warnings",
        "--newline", "--progress", "--progress-template",
        "download:%(progress._percent_str)s|%(progress.downloaded_bytes)s|%(progress.total_bytes_estimate)s|%(progress.speed)s|%(progress.eta)s",
        "--continue", "--http-chunk-size", "10M", "--max-filesize", str(MAX_FILE_BYTES), "--socket-timeout", "15",
        "--retries", "3", "--fragment-retries", "3", "--format", format_id,
    ]
    if shutil.which("node"):
        command.extend(("--js-runtimes", "node"))
    command.extend(ytdlp_antiblock_args(row["url"], fallback=False))
    if audio_format:
        command.extend(("--extract-audio", "--audio-format", audio_format, "--postprocessor-args", "ffmpeg:-threads 1"))
    elif has_ffmpeg:
        command.extend(("--merge-output-format", "mp4", "--postprocessor-args", "ffmpeg:-threads 1"))
    if row["rate_limit_bps"]:
        command.extend(("--limit-rate", str(row["rate_limit_bps"])))
    return [*command, "--output", output, row["url"]]


def parse_progress(line: str):
    if not line.startswith("download:"):
        return None
    pieces = line.removeprefix("download:").strip().split("|")
    if len(pieces) < 5:
        return None
    percent = re.search(r"([0-9]+(?:\.[0-9]+)?)%", pieces[0])
    def integer(value):
        return int(value) if value.isdigit() else None
    try:
        speed = int(float(pieces[3])) if pieces[3] not in ("NA", "None", "") else 0
    except ValueError:
        speed = 0
    return {
        "progress": max(0, min(100, float(percent.group(1)))) if percent else 0,
        "downloaded_bytes": integer(pieces[1]) or 0,
        "total_bytes": integer(pieces[2]),
        "speed_bps": max(0, speed),
        "eta_seconds": integer(pieces[4]),
    }


def run_job(job_id: str):
    process = None
    reader = None
    with db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row or row["status"] == "cancelled":
        with LOCK:
            ACTIVE.discard(job_id)
        return
    folder = OUTPUT_DIR / row["user_id"] / job_id
    try:
        folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError:
        update_job(job_id, status="failed", error="storage_unavailable", url="")
        with LOCK:
            ACTIVE.discard(job_id)
        return
    output = str(folder / "%(title).120B.%(ext)s")
    if row["format_id"] == "direct" or row["platform"] == "Direct URL":
        run_direct_job(job_id, row, folder)
        return
    control = {"paused": threading.Event(), "cancelled": threading.Event()}
    with LOCK:
        CONTROLS[job_id] = control
        with db() as conn:
            state = conn.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
        if state and state["status"] == "paused":
            control["paused"].set()
        elif state and state["status"] == "cancelled":
            control["cancelled"].set()
    if control["cancelled"].is_set():
        shutil.rmtree(folder, ignore_errors=True)
        with LOCK:
            CONTROLS.pop(job_id, None)
            ACTIVE.discard(job_id)
        return
    command = extractor_command(row, output)
    try:
        started = time.monotonic()
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                   bufsize=1, start_new_session=True,
                                   env=extractor_env())
        with LOCK:
            PROCESSES[job_id] = process
            with db() as conn:
                state = conn.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if state and state["status"] == "cancelled":
                control["cancelled"].set()
                stop_extractor(process)
            elif state and state["status"] == "paused":
                control["paused"].set()
                pause_extractor(process)
            elif state and state["status"] == "queued":
                update_job(job_id, status="downloading")
        output_lines: queue.Queue[str | None] = queue.Queue()

        def read_output():
            if process.stdout:
                for output_line in process.stdout:
                    output_lines.put(output_line)
            output_lines.put(None)

        reader = threading.Thread(target=read_output, daemon=True, name=f"media-output-{job_id}")
        reader.start()
        reader_done = False
        while process.poll() is None or not reader_done:
            if process.poll() is None and time.monotonic() - started > MAX_JOB_SECONDS:
                stop_extractor(process)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    stop_extractor(process, force=True)
                    process.wait(timeout=5)
                else:
                    stop_extractor(process, force=True)
                continue
            try:
                line = output_lines.get(timeout=1)
            except queue.Empty:
                continue
            if line is None:
                reader_done = True
                continue
            progress = parse_progress(line)
            if progress:
                update_job(job_id, **progress)
        reader.join(timeout=2)
        with LOCK:
            with db() as conn:
                current = conn.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if current and current["status"] == "cancelled":
                shutil.rmtree(folder, ignore_errors=True)
            elif time.monotonic() - started > MAX_JOB_SECONDS:
                shutil.rmtree(folder, ignore_errors=True)
                update_job(job_id, status="failed", error="download_timeout", url="")
            elif process.returncode == 0:
                candidates = [path for path in folder.iterdir() if path.is_file() and not path.name.endswith((".part", ".ytdl"))]
                final = max(candidates, key=lambda path: path.stat().st_size, default=None)
                if final and 0 < final.stat().st_size <= MAX_FILE_BYTES:
                    update_job(job_id, status="completed", progress=100, file_name=final.name, size_bytes=final.stat().st_size, error=None, url="")
                else:
                    shutil.rmtree(folder, ignore_errors=True)
                    update_job(job_id, status="failed", error="output_unavailable", url="")
            else:
                shutil.rmtree(folder, ignore_errors=True)
                update_job(job_id, status="failed", error="download_failed", url="")
    except Exception:
        if process and process.poll() is None:
            stop_extractor(process)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                stop_extractor(process, force=True)
                process.wait(timeout=5)
        if reader:
            reader.join(timeout=2)
        shutil.rmtree(folder, ignore_errors=True)
        update_job(job_id, status="failed", error="worker_error", url="")
    finally:
        with LOCK:
            PROCESSES.pop(job_id, None)
            CONTROLS.pop(job_id, None)
            ACTIVE.discard(job_id)


def convert_direct_audio(job_id: str, source: Path, output: Path, audio_format: str, control: dict, started: float):
    if audio_format not in AUDIO_OUTPUT_FORMATS:
        raise ValueError("invalid_audio_format")
    executable = shutil.which("ffmpeg")
    if not executable:
        raise RuntimeError("worker_unavailable")
    # Only local media containers are accepted; playlists cannot fetch URLs or reference local secrets.
    command = [executable, "-nostdin", "-hide_banner", "-loglevel", "error", "-threads", "1", "-y",
               "-protocol_whitelist", "file", "-format_whitelist", "mov,matroska,webm,avi,mpeg,mpegts,mp3,wav,flac,ogg,aac",
               "-i", str(source.resolve()), "-map", "0:a:0", "-vn", "-fs", str(MAX_FILE_BYTES + 1), str(output.resolve())]
    process = None
    try:
        while True:
            if control["cancelled"].is_set():
                raise InterruptedError
            if time.monotonic() - started > MAX_JOB_SECONDS:
                raise RuntimeError("download_timeout")
            if control["paused"].is_set():
                time.sleep(0.2)
                continue
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, start_new_session=True, env=extractor_env())
            if os.name != "nt":
                with LOCK:
                    PROCESSES[job_id] = process
                    if control["paused"].is_set():
                        os.killpg(process.pid, signal.SIGSTOP)
            restart = False
            while True:
                if control["cancelled"].is_set():
                    raise InterruptedError
                if time.monotonic() - started > MAX_JOB_SECONDS:
                    raise RuntimeError("download_timeout")
                if output.exists() and output.stat().st_size > MAX_FILE_BYTES:
                    raise RuntimeError("file_too_large")
                if os.name == "nt" and control["paused"].is_set():
                    # Windows has no SIGSTOP: resume re-encodes the retained private input.
                    stop_extractor(process)
                    process.wait(timeout=5)
                    output.unlink(missing_ok=True)
                    restart = True
                    break
                try:
                    code = process.wait(timeout=0.2)
                    break
                except subprocess.TimeoutExpired:
                    continue
            if restart:
                continue
            if code != 0:
                raise RuntimeError("download_failed")
            if not output.is_file() or not 0 < output.stat().st_size <= MAX_FILE_BYTES:
                raise RuntimeError("file_too_large" if output.is_file() and output.stat().st_size > MAX_FILE_BYTES else "download_failed")
            return
    finally:
        if process and process.poll() is None:
            stop_extractor(process)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                stop_extractor(process, force=True)
                process.wait(timeout=5)
        with LOCK:
            PROCESSES.pop(job_id, None)


def run_direct_job(job_id: str, row: sqlite3.Row, folder: Path):
    partial = folder / (row["title"] + ".part")
    audio_format = row["format_id"].removeprefix("edushare-audio-") if row["format_id"].startswith("edushare-audio-") else None
    final = folder / (f"{Path(row['title']).stem}.{audio_format}" if audio_format else row["title"])
    converted = folder / (f"converted.{audio_format}" if audio_format else "converted")
    control = {"paused": threading.Event(), "cancelled": threading.Event()}
    with LOCK:
        CONTROLS[job_id] = control
        with db() as conn:
            state = conn.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
        if state and state["status"] == "paused":
            control["paused"].set()
        elif state and state["status"] == "cancelled":
            control["cancelled"].set()
    if control["cancelled"].is_set():
        shutil.rmtree(folder, ignore_errors=True)
        with LOCK:
            CONTROLS.pop(job_id, None)
            ACTIVE.discard(job_id)
        return
    connection = None
    started = time.monotonic()
    try:
        if audio_format and audio_format not in AUDIO_OUTPUT_FORMATS:
            raise ValueError("invalid_audio_format")
        existing = partial.stat().st_size if partial.exists() else 0
        headers = {"Range": f"bytes={existing}-"} if existing else {}
        connection, response, _ = open_public_response(row["url"], headers)
        if control["cancelled"].is_set():
            raise InterruptedError
        if existing and response.status == 200:
            existing = 0
        if response.status not in ((206,) if existing else (200, 206)):
            raise RuntimeError("source_unavailable")
        if existing and response.status == 206:
            content_range = re.match(r"bytes (\d+)-\d+/(?:\d+|\*)", response.getheader("Content-Range", ""))
            if not content_range or int(content_range.group(1)) != existing:
                raise RuntimeError("source_unavailable")
        content_range_size = response.getheader("Content-Range", "").rsplit("/", 1)[-1]
        content_length = response.getheader("Content-Length", "")
        if content_range_size.isdigit():
            total = int(content_range_size)
        elif content_length.isdigit():
            total = existing + int(content_length)
        else:
            total = None
        if total is not None and total > MAX_FILE_BYTES:
            raise RuntimeError("file_too_large")
        downloaded = existing
        rate_started = time.monotonic()
        rate_bytes = 0
        last_update = time.monotonic()
        last_downloaded = downloaded
        update_job(job_id, total_bytes=total, downloaded_bytes=downloaded, progress=(downloaded / total * (90 if audio_format else 100) if total else 0))
        mode = "ab" if existing else "wb"
        with partial.open(mode) as target:
            while chunk := response.read(65536):
                if time.monotonic() - started > MAX_JOB_SECONDS:
                    raise RuntimeError("download_timeout")
                if control["cancelled"].is_set():
                    raise InterruptedError
                while control["paused"].is_set() and not control["cancelled"].is_set():
                    time.sleep(0.2)
                if control["cancelled"].is_set():
                    raise InterruptedError
                target.write(chunk)
                downloaded += len(chunk)
                rate_bytes += len(chunk)
                if downloaded > MAX_FILE_BYTES:
                    raise RuntimeError("file_too_large")
                throttle_download(rate_bytes, rate_started, row["rate_limit_bps"])
                now = time.monotonic()
                if now - last_update >= 1 or (total and downloaded >= total):
                    speed = max(0, int((downloaded - last_downloaded) / max(now - last_update, 0.001)))
                    update_job(job_id, downloaded_bytes=downloaded, total_bytes=total,
                               progress=(min(90 if audio_format else 99.9, downloaded / total * (90 if audio_format else 100)) if total else 0),
                               speed_bps=speed,
                               eta_seconds=max(0, int((total - downloaded) / speed)) if total and speed else None)
                    last_update = now
                    last_downloaded = downloaded
        with LOCK:
            with db() as conn:
                current = conn.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if control["cancelled"].is_set() or not current or current["status"] == "cancelled":
                raise InterruptedError
            if total is not None and downloaded != total:
                raise RuntimeError("download_failed")
        if audio_format:
            connection.close()
            connection = None
            update_job(job_id, progress=90, speed_bps=0, eta_seconds=None)
            convert_direct_audio(job_id, partial, converted, audio_format, control, started)
        with LOCK:
            if control["cancelled"].is_set():
                raise InterruptedError
            (converted if audio_format else partial).replace(final)
            if audio_format:
                partial.unlink(missing_ok=True)
            update_job(job_id, status="completed", progress=100, downloaded_bytes=downloaded,
                       total_bytes=total, file_name=final.name, size_bytes=final.stat().st_size, error=None, url="")
    except InterruptedError:
        partial.unlink(missing_ok=True)
        converted.unlink(missing_ok=True)
        update_job(job_id, status="cancelled", error=None, url="")
    except Exception as exc:
        partial.unlink(missing_ok=True)
        converted.unlink(missing_ok=True)
        update_job(job_id, status="failed", error=str(exc) if str(exc) in ("file_too_large", "download_timeout", "invalid_audio_format", "worker_unavailable") else "download_failed", url="")
    finally:
        if connection:
            connection.close()
        with LOCK:
            CONTROLS.pop(job_id, None)
            ACTIVE.discard(job_id)


def schedule_loop():
    renewed_at = 0.0
    while True:
        with LOCK:
            now_mono = time.monotonic()
            if now_mono - renewed_at >= 60:
                expiry = int(time.time()) + RETRY_TTL_SECONDS
                with db() as conn:
                    conn.execute("UPDATE analyses SET expires_at=? WHERE id IN (SELECT analysis_id FROM jobs WHERE status IN ('queued','downloading','paused') AND analysis_id IS NOT NULL)", (expiry,))
                    conn.execute("UPDATE jobs SET retry_expires_at=? WHERE status IN ('queued','downloading','paused') AND analysis_id IS NOT NULL", (expiry,))
                renewed_at = now_mono
            slots = MAX_WORKERS - len(ACTIVE)
            if slots > 0:
                with db() as conn:
                    rows = conn.execute("SELECT id FROM jobs WHERE status='queued' ORDER BY created_at LIMIT ?", (slots,)).fetchall()
                    for row in rows:
                        ACTIVE.add(row["id"])
                        conn.execute("UPDATE jobs SET status='downloading',attempt_count=attempt_count+1,updated_at=? WHERE id=? AND status='queued'", (int(time.time()), row["id"]))
                        EXECUTOR.submit(run_job, row["id"])
        time.sleep(0.5)


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "EduShareMediaWorker/1.0"

    def log_message(self, _format, *_args):
        # Request bodies and source URLs are intentionally never logged.
        return

    def send_json(self, status: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length < 0 or length > MAX_BODY:
            raise OverflowError("body_too_large")
        raw = self.rfile.read(length)
        payload = json.loads(raw or b"{}")
        if not isinstance(payload, dict):
            raise ValueError("invalid_json")
        return raw, payload

    def do_HEAD(self):
        if self.path in ("/health", "/"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_GET(self):
        try:
            if self.path in ("/health", "/"):
                return self.send_json(200, {
                    "ok": True,
                    "ytDlp": importlib.util.find_spec("yt_dlp") is not None,
                    "ffmpeg": bool(shutil.which("ffmpeg")),
                    "impersonate": has_impersonate(),
                    "allSites": ALLOW_ALL_PUBLIC_SITES,
                })
            ticket = re.fullmatch(r"(?:/v1/jobs/([0-9a-f-]{36})/file|/files/([0-9a-f-]{36}))\?ticket=([0-9]+)\.([0-9a-f]{32})\.([0-9a-f]{64})", self.path)
            if ticket:
                job_id = ticket.group(1) or ticket.group(2)
                expiry, nonce, signature = ticket.group(3), ticket.group(4), ticket.group(5)
                with db() as conn:
                    row = conn.execute("SELECT user_id FROM jobs WHERE id=? AND status='completed'", (job_id,)).fetchone()
                if not row or not int(time.time()) <= int(expiry) <= int(time.time()) + 120:
                    return self.send_json(401, {"error": "download_ticket_expired"})
                user_id = row["user_id"]
                message = f"file\n{job_id}\n{user_id}\n{expiry}\n{nonce}".encode()
                expected = hmac.new(SECRET.encode(), message, hashlib.sha256).hexdigest()
                if not hmac.compare_digest(expected, signature):
                    return self.send_json(401, {"error": "download_ticket_invalid"})
                try:
                    with db() as conn:
                        conn.execute("INSERT INTO download_tickets VALUES(?,?)", (nonce, int(expiry)))
                except sqlite3.IntegrityError:
                    return self.send_json(401, {"error": "download_ticket_used"})
                return self.send_file(job_id, user_id)
            user = verify_signature(self, b"")
            clean_expired()
            if self.path in ("/v1/jobs", "/jobs"):
                with db() as conn:
                    rows = conn.execute("SELECT * FROM jobs WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (user,)).fetchall()
                return self.send_json(200, {"jobs": [row_json(row) for row in rows]})
            match = re.fullmatch(r"(?:/v1)?/jobs/([0-9a-f-]{36})", self.path)
            if match:
                with db() as conn:
                    row = conn.execute("SELECT * FROM jobs WHERE id=? AND user_id=?", (match.group(1), user)).fetchone()
                return self.send_json(200, {"job": row_json(row)}) if row else self.send_json(404, {"error": "job_not_found"})
            file_match = re.fullmatch(r"(?:/v1/jobs/([0-9a-f-]{36})/file|/files/([0-9a-f-]{36}))", self.path)
            if file_match:
                return self.send_file(file_match.group(1) or file_match.group(2), user)
            self.send_json(404, {"error": "not_found"})
        except Exception as exc:
            self.handle_error(exc)

    def do_POST(self):
        try:
            raw, payload = self.body()
            user = verify_signature(self, raw)
            clean_expired()
            if self.path in ("/v1/jobs/analyze", "/analyze"):
                url, result = analyze_url(payload.get("url"))
                analysis_id = str(uuid.uuid4())
                with db() as conn:
                    conn.execute("INSERT INTO analyses(id,user_id,url,title,platform,duration,formats,expires_at,thumbnail_data) VALUES(?,?,?,?,?,?,?,?,?)", (analysis_id, user, result.get("sourceUrl", url), result["title"], result["platform"], result["duration"], json.dumps(result["formats"]), int(time.time()) + 300, result.get("thumbnailData")))
                return self.send_json(200, {"analysisId": analysis_id, **{key: result[key] for key in ("title", "platform", "duration", "formats")}, "thumbnailData": result.get("thumbnailData")})
            if self.path in ("/v1/jobs", "/jobs"):
                analysis_id = payload.get("analysisId")
                format_id = payload.get("formatId", "best")
                if set(payload) - {"analysisId", "formatId", "rateLimitBps"}:
                    return self.send_json(400, {"error": "invalid_job"})
                limit_bps = rate_limit(payload.get("rateLimitBps", 0))
                if not isinstance(analysis_id, str) or not isinstance(format_id, str) or not re.fullmatch(r"[A-Za-z0-9+._/-]{1,100}", format_id):
                    return self.send_json(400, {"error": "invalid_job"})
                with LOCK, db() as conn:
                    analysis = conn.execute("SELECT * FROM analyses WHERE id=? AND user_id=? AND expires_at>=? AND consumed=0", (analysis_id, user, int(time.time()))).fetchone()
                    if not analysis:
                        return self.send_json(400, {"error": "analysis_expired"})
                    formats = json.loads(analysis["formats"])
                    selected_format = next((item for item in formats if item["id"] == format_id), None)
                    if selected_format is None:
                        return self.send_json(400, {"error": "invalid_format"})
                    if format_id != "direct" and format_id != "best" and selected_format.get("hasVideo") and not selected_format.get("hasAudio"):
                        format_id = f"{format_id}+bestaudio"
                    job_id, now = str(uuid.uuid4()), int(time.time())
                    if error := job_capacity_error(conn, user):
                        return self.send_json(429, {"error": error})
                    conn.execute("UPDATE analyses SET expires_at=?,consumed=1 WHERE id=?", (now + RETRY_TTL_SECONDS, analysis_id))
                    conn.execute("INSERT INTO jobs(id,user_id,url,title,platform,format_id,status,rate_limit_bps,analysis_id,retry_expires_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (job_id, user, analysis["url"], analysis["title"], analysis["platform"], format_id, "queued", limit_bps, analysis_id, now + RETRY_TTL_SECONDS, now, now))
                    row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                return self.send_json(202, {"job": row_json(row)})
            action_match = re.fullmatch(r"(?:/v1)?/jobs/([0-9a-f-]{36})/action", self.path)
            if action_match:
                job_id, action = action_match.group(1), payload.get("action")
                if action not in ("pause", "resume", "cancel", "retry"):
                    return self.send_json(400, {"error": "invalid_action"})
                with LOCK, db() as conn:
                    row = conn.execute("SELECT * FROM jobs WHERE id=? AND user_id=?", (job_id, user)).fetchone()
                    if not row:
                        return self.send_json(404, {"error": "job_not_found"})
                    if action == "retry":
                        now = int(time.time())
                        if row["status"] != "failed" or row["attempt_count"] >= MAX_JOB_ATTEMPTS or row["retry_expires_at"] < now or not row["analysis_id"]:
                            return self.send_json(409, {"error": "retry_unavailable"})
                        analysis = conn.execute("SELECT url FROM analyses WHERE id=? AND user_id=? AND expires_at>=?", (row["analysis_id"], user, now)).fetchone()
                        if not analysis:
                            return self.send_json(409, {"error": "retry_unavailable"})
                        if error := job_capacity_error(conn, user):
                            return self.send_json(429, {"error": error})
                        shutil.rmtree(OUTPUT_DIR / user / job_id, ignore_errors=True)
                        conn.execute("UPDATE analyses SET expires_at=? WHERE id=?", (now + RETRY_TTL_SECONDS, row["analysis_id"]))
                        conn.execute("UPDATE jobs SET url=?,status='queued',progress=0,speed_bps=0,downloaded_bytes=0,total_bytes=NULL,eta_seconds=NULL,file_name=NULL,size_bytes=0,error=NULL,updated_at=? WHERE id=?", (analysis["url"], now, job_id))
                        conn.execute("UPDATE jobs SET retry_expires_at=? WHERE id=?", (now + RETRY_TTL_SECONDS, job_id))
                    process = PROCESSES.get(job_id)
                    if action == "cancel" and row["status"] in ("queued", "downloading", "paused"):
                        conn.execute("UPDATE jobs SET status='cancelled',url='',updated_at=? WHERE id=?", (int(time.time()), job_id))
                        conn.execute("DELETE FROM analyses WHERE id=?", (row["analysis_id"],))
                        if process and process.poll() is None:
                            stop_extractor(process)
                        control = CONTROLS.get(job_id)
                        if control:
                            control["cancelled"].set()
                    elif action == "pause" and row["status"] == "queued":
                        conn.execute("UPDATE jobs SET status='paused',updated_at=? WHERE id=?", (int(time.time()), job_id))
                    elif action == "pause" and row["status"] == "downloading" and process and process.poll() is None:
                        pause_extractor(process)
                        if job_id in CONTROLS:
                            CONTROLS[job_id]["paused"].set()
                        conn.execute("UPDATE jobs SET status='paused',updated_at=? WHERE id=?", (int(time.time()), job_id))
                    elif action == "pause" and row["status"] == "downloading" and job_id in CONTROLS:
                        CONTROLS[job_id]["paused"].set()
                        conn.execute("UPDATE jobs SET status='paused',updated_at=? WHERE id=?", (int(time.time()), job_id))
                    elif action == "pause" and row["status"] == "downloading":
                        conn.execute("UPDATE jobs SET status='paused',updated_at=? WHERE id=?", (int(time.time()), job_id))
                    elif action == "resume" and row["status"] == "paused":
                        if process and process.poll() is None:
                            resume_extractor(process)
                            if job_id in CONTROLS:
                                CONTROLS[job_id]["paused"].clear()
                            conn.execute("UPDATE jobs SET status='downloading',updated_at=? WHERE id=?", (int(time.time()), job_id))
                        elif job_id in CONTROLS:
                            CONTROLS[job_id]["paused"].clear()
                            conn.execute("UPDATE jobs SET status='downloading',updated_at=? WHERE id=?", (int(time.time()), job_id))
                        else:
                            conn.execute("UPDATE jobs SET status='queued',updated_at=? WHERE id=?", (int(time.time()), job_id))
                    updated = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                return self.send_json(200, {"job": row_json(updated)})
            self.send_json(404, {"error": "not_found"})
        except Exception as exc:
            self.handle_error(exc)

    def send_file(self, job_id: str, user: str):
        with db() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=? AND user_id=? AND status='completed'", (job_id, user)).fetchone()
        if not row or not row["file_name"]:
            return self.send_json(404, {"error": "file_not_found"})
        folder = OUTPUT_DIR / user / job_id
        path = (folder / row["file_name"]).resolve()
        if path.parent != folder.resolve() or not path.is_file() or path.stat().st_size > MAX_FILE_BYTES:
            return self.send_json(404, {"error": "file_not_found"})
        self.send_response(200)
        self.send_header("Content-Type", mimetypes.guess_type(path.name)[0] or "application/octet-stream")
        self.send_header("Content-Length", str(path.stat().st_size))
        self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{quote(path.name)}")
        self.send_header("Cache-Control", "private, no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        streamed_ok = False
        try:
            with path.open("rb") as source:
                shutil.copyfileobj(source, self.wfile, length=65536)
            self.wfile.flush()
            streamed_ok = True
        finally:
            if streamed_ok and DELETE_AFTER_STREAM:
                shutil.rmtree(folder, ignore_errors=True)
                try:
                    with db() as conn:
                        conn.execute("UPDATE jobs SET size_bytes=0 WHERE id=?", (job_id,))
                except Exception:
                    pass

    def handle_error(self, exc):
        if isinstance(exc, PermissionError):
            return self.send_json(401, {"error": "unauthorized"})
        if isinstance(exc, OverflowError):
            return self.send_json(429 if str(exc) == "rate_limited" else 413, {"error": str(exc)})
        if isinstance(exc, json.JSONDecodeError):
            return self.send_json(400, {"error": "invalid_json"})
        if isinstance(exc, ValueError):
            return self.send_json(400, {"error": str(exc)})
        if isinstance(exc, RuntimeError):
            status = 503 if str(exc) == "worker_unavailable" else 422
            return self.send_json(status, {"error": str(exc)})
        self.send_json(500, {"error": "worker_error"})


def main():
    global EXECUTOR
    if len(SECRET) < 32:
        raise SystemExit("DOWNLOAD_WORKER_SECRET must contain at least 32 characters")
    os.umask(0o077)
    import concurrent.futures
    init_db()
    EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="media")
    threading.Thread(target=schedule_loop, daemon=True, name="media-queue").start()
    http.server.ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
