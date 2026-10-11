#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OffAir Music: single-file, low-bandwidth local music streamer and prefetcher.
Built for slow, high-latency connections (~70 kb/s).

Requires:
    pip install yt-dlp

Design notes:
    - Exactly one network-heavy job runs at a time. A single worker thread
      downloads tracks one by one (current track first, then the next two),
      and only tops the queue up from Radio when nothing is downloading.
    - Interrupted downloads keep their .part file and resume where they
      stopped instead of starting over.
    - Radio blends the YouTube Music radio lists (~35 KB gzipped each) of up to
      five tracks from your queue, personalised by your cookies when set. Each
      list is fetched once, cached, and only while no download is running.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import math
import os
import pathlib
import random
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator, Optional
from urllib.parse import parse_qs, urlparse

try:
    import yt_dlp
except ImportError:
    print(
        "Error: yt-dlp is required. Please install it with: pip install yt-dlp",
        file=sys.stderr,
    )
    sys.exit(1)

# -----------------------------------------------------------------------------
# Configuration & Storage Paths
# -----------------------------------------------------------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
VAULT_DIR = BASE_DIR / "music_vault"
VAULT_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = BASE_DIR / "library.db"
SETTINGS_PATH = BASE_DIR / "settings.json"

DEFAULT_SETTINGS = {
    "bitrate": 48,
    "cookies_file": r"C:\Users\pc\Downloads\www.youtube.com_cookies.txt",
    "radio_autofill": True,
    "max_length_min": 10,    # Radio and Search hide songs longer than this (0 = no limit)
    "target_length_min": 4,  # Radio autofill favours songs close to this length (0 = ignore length)
}

VIDEO_ID_RE = re.compile(r"^[0-9A-Za-z_-]{11}$")
PARTIAL_SUFFIXES = (".part", ".ytdl", ".temp")

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
YTM_CLIENT_VERSION = "1.20250929.03.00"
WEB_CLIENT_VERSION = "2.20250929.01.00"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
if sys.stderr is None:
    # Started without a console (pythonw.exe, e.g. from the Windows Startup folder):
    # keep the logs in a small rotating file next to the script instead.
    from logging.handlers import RotatingFileHandler

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[RotatingFileHandler(BASE_DIR / "offair.log", maxBytes=1_000_000, backupCount=1, encoding="utf-8")],
    )
else:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("OffAir")


def load_settings() -> dict:
    if SETTINGS_PATH.exists():
        try:
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return {**DEFAULT_SETTINGS, **data}
        except Exception as e:
            log.warning("Could not read settings: %s", e)
    return dict(DEFAULT_SETTINGS)


def save_settings(settings: dict) -> None:
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2)


def watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def parse_duration(text: str) -> Optional[int]:
    """'3:45' or '1:02:03' -> seconds; None when unknown."""
    parts = (text or "").strip().split(":")
    if len(parts) < 2 or not all(p.isdigit() for p in parts):
        return None
    total = 0
    for p in parts:
        total = total * 60 + int(p)
    return total


def length_limits() -> tuple[float, float]:
    """(max seconds, target seconds) from settings; 0 means disabled."""
    settings = load_settings()

    def minutes(key: str) -> float:
        try:
            return max(0.0, float(settings.get(key) or 0)) * 60
        except (TypeError, ValueError):
            return float(DEFAULT_SETTINGS[key]) * 60

    return minutes("max_length_min"), minutes("target_length_min")


def too_long(seconds: Optional[float], max_sec: float) -> bool:
    return bool(max_sec and seconds and seconds > max_sec)


def length_fit(seconds: Optional[float], target_sec: float) -> float:
    """
    1.0 for a song exactly the target length, falling off with the ratio between
    the two: half or double the target scores 0.5. Unknown lengths get a neutral 0.7.
    """
    if not target_sec:
        return 1.0
    if not seconds:
        return 0.7
    return 1.0 / (1.0 + 1.44 * abs(math.log(seconds / target_sec)))


def fmt_duration(seconds) -> str:
    if not isinstance(seconds, (int, float)) or seconds <= 0:
        return ""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def load_youtube_cookies(filepath: str) -> dict[str, str]:
    """Reads the youtube.com cookies out of a Netscape cookies.txt file."""
    cookies: dict[str, str] = {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # #HttpOnly_ lines hold the auth tokens, so they must be kept.
                if line.startswith("#HttpOnly_"):
                    line = line[10:]
                elif line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) >= 7 and parts[0].lstrip(".").endswith("youtube.com"):
                    cookies[parts[5]] = parts[6]
    except Exception as e:
        log.warning("Cookie parsing failed: %s", e)
    return cookies


# -----------------------------------------------------------------------------
# Database Setup (Permanent Library)
# -----------------------------------------------------------------------------
def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tracks (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                uploader TEXT,
                filename TEXT NOT NULL,
                downloaded_at REAL NOT NULL
            )
        """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tracks)")}
        if "duration" not in columns:
            conn.execute("ALTER TABLE tracks ADD COLUMN duration TEXT DEFAULT ''")
        conn.commit()


def db_get_all_tracks() -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, title, uploader, duration, filename, downloaded_at "
            "FROM tracks ORDER BY downloaded_at DESC"
        ).fetchall()
    # Hide rows whose file was removed outside the app.
    return [dict(r) for r in rows if (VAULT_DIR / r["filename"]).exists()]


def db_add_track(info: dict, filename: str) -> None:
    duration = info.get("duration")
    duration = fmt_duration(duration) if isinstance(duration, (int, float)) else (duration or "")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO tracks (id, title, uploader, filename, downloaded_at, duration)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                info.get("id"),
                info.get("title") or "Unknown",
                info.get("uploader") or info.get("channel") or "Unknown",
                filename,
                time.time(),
                duration,
            ),
        )
        conn.commit()


def db_delete_track(video_id: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM tracks WHERE id = ?", (video_id,))
        conn.commit()


# -----------------------------------------------------------------------------
# Search helpers
# -----------------------------------------------------------------------------
def extract_youtube_id(text: str) -> Optional[str]:
    m = re.search(r"(?:v=|\/|be\/)([0-9A-Za-z_-]{11})", text.strip())
    return m.group(1) if m else None


def search_entry_to_track(entry: dict) -> dict:
    return {
        "id": entry.get("id"),
        "title": entry.get("title") or "Unknown",
        "uploader": entry.get("uploader") or entry.get("channel") or "Unknown",
        "duration": fmt_duration(entry.get("duration")),
        "url": entry.get("url") or watch_url(entry.get("id") or ""),
    }


# -----------------------------------------------------------------------------
# Radio (recommendations)
# -----------------------------------------------------------------------------
_RADIO_CACHE: dict[str, tuple[float, list[dict]]] = {}
_RADIO_LOCK = threading.Lock()
RADIO_TTL = 6 * 3600
RADIO_FAIL_TTL = 90
RADIO_MAX_SEEDS = 5
RADIO_PERSONALISED: Optional[bool] = None  # did YouTube treat the last radio request as logged in?


def _post_json(url: str, payload: dict, headers: dict, timeout: int = 30) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip",
            "User-Agent": BROWSER_UA,
            **headers,
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
    return json.loads(raw)


def _cookie_headers(origin: str) -> dict:
    """Cookie + SAPISIDHASH headers so YouTube personalises Radio, if cookies are set."""
    path = (load_settings().get("cookies_file") or "").strip()
    if not path or not os.path.isfile(path):
        return {}
    jar = load_youtube_cookies(path)
    if not jar:
        return {}
    headers = {"Cookie": "; ".join(f"{k}={v}" for k, v in jar.items())}
    sapisid = jar.get("SAPISID") or jar.get("__Secure-3PAPISID")
    if sapisid:
        ts = str(int(time.time()))
        digest = hashlib.sha1(f"{ts} {sapisid} {origin}".encode("utf-8")).hexdigest()
        headers["Authorization"] = f"SAPISIDHASH {ts}_{digest}"
        headers["X-Origin"] = origin
        headers["X-Goog-AuthUser"] = "0"
    return headers


def _post_with_optional_auth(url: str, payload: dict, headers: dict, origin: str) -> dict:
    """Tries the request with the user's cookies first, then anonymously if YouTube rejects them."""
    auth = _cookie_headers(origin)
    last_error: Optional[Exception] = None
    for extra in ([auth, {}] if auth else [{}]):
        try:
            return _post_json(url, payload, {**headers, **extra})
        except urllib.error.HTTPError as e:
            last_error = e
            if extra:
                log.warning("Radio request with cookies was rejected (%s); retrying anonymously", e.code)
    raise last_error  # type: ignore[misc]


def _find_all(node, key: str) -> Iterator[dict]:
    """Depth-first, document-order search for every value stored under `key`."""
    stack = [node]
    while stack:
        n = stack.pop()
        if isinstance(n, dict):
            if key in n:
                yield n[key]
            stack.extend(reversed(list(n.values())))
        elif isinstance(n, list):
            stack.extend(reversed(n))


def _text(node) -> str:
    if not isinstance(node, dict):
        return ""
    if "simpleText" in node:
        return node["simpleText"]
    if "content" in node:
        return node["content"]
    return "".join(run.get("text", "") for run in node.get("runs", []))


def _fetch_ytm_radio(seed_id: str) -> list[dict]:
    origin = "https://music.youtube.com"
    payload = {
        "context": {
            "client": {"clientName": "WEB_REMIX", "clientVersion": YTM_CLIENT_VERSION, "hl": "en", "gl": "US"}
        },
        "videoId": seed_id,
        "playlistId": f"RDAMVM{seed_id}",
        "isAudioOnly": True,
        "enablePersistentPlaylistPanel": True,
    }
    headers = {
        "Origin": origin,
        "X-YouTube-Client-Name": "67",
        "X-YouTube-Client-Version": YTM_CLIENT_VERSION,
    }
    data = _post_with_optional_auth(f"{origin}/youtubei/v1/next?prettyPrint=false", payload, headers, origin)

    global RADIO_PERSONALISED
    for service in (data.get("responseContext") or {}).get("serviceTrackingParams") or []:
        for p in service.get("params") or []:
            if p.get("key") == "logged_in":
                RADIO_PERSONALISED = p.get("value") == "1"

    tracks = []
    for r in _find_all(data, "playlistPanelVideoRenderer"):
        vid = r.get("videoId")
        if not vid or r.get("selected"):  # the selected entry is the seed itself
            continue
        tracks.append({
            "id": vid,
            "title": _text(r.get("title")) or "Unknown",
            "uploader": _text(r.get("shortBylineText")) or "Unknown",
            "duration": _text(r.get("lengthText")),
            "url": watch_url(vid),
        })
    return tracks


def _fetch_web_related(seed_id: str) -> list[dict]:
    origin = "https://www.youtube.com"
    payload = {
        "context": {"client": {"clientName": "WEB", "clientVersion": WEB_CLIENT_VERSION, "hl": "en", "gl": "US"}},
        "videoId": seed_id,
    }
    headers = {
        "Origin": origin,
        "X-YouTube-Client-Name": "1",
        "X-YouTube-Client-Version": WEB_CLIENT_VERSION,
    }
    data = _post_with_optional_auth(f"{origin}/youtubei/v1/next?prettyPrint=false", payload, headers, origin)

    tracks = []
    # Current layout: related videos are lockupViewModel entries.
    for lv in _find_all(data, "lockupViewModel"):
        if lv.get("contentType") != "LOCKUP_CONTENT_TYPE_VIDEO" or not lv.get("contentId"):
            continue
        meta = (lv.get("metadata") or {}).get("lockupMetadataViewModel") or {}
        rows = ((meta.get("metadata") or {}).get("contentMetadataViewModel") or {}).get("metadataRows") or []
        uploader = ""
        if rows and rows[0].get("metadataParts"):
            uploader = _text(rows[0]["metadataParts"][0].get("text"))
        duration = next(
            (
                b.get("text", "")
                for b in _find_all(lv.get("contentImage"), "thumbnailBadgeViewModel")
                if re.fullmatch(r"\d+(:\d\d){1,2}", b.get("text", ""))
            ),
            "",
        )
        tracks.append({
            "id": lv["contentId"],
            "title": _text(meta.get("title")) or "Unknown",
            "uploader": uploader or "Unknown",
            "duration": duration,
            "url": watch_url(lv["contentId"]),
        })
    # Older layout, kept in case YouTube serves it again.
    for vr in _find_all(data, "compactVideoRenderer"):
        if vr.get("videoId"):
            tracks.append({
                "id": vr["videoId"],
                "title": _text(vr.get("title")) or "Unknown",
                "uploader": _text(vr.get("shortBylineText")) or "Unknown",
                "duration": _text(vr.get("lengthText")),
                "url": watch_url(vr["videoId"]),
            })
    return tracks


def get_recommendations(seed_id: str) -> list[dict]:
    """Radio tracks for a seed. Cached per seed; one fetch at a time to spare the link."""
    with _RADIO_LOCK:
        hit = _RADIO_CACHE.get(seed_id)
        if hit and time.time() < hit[0]:
            return hit[1]

        tracks: list[dict] = []
        for name, fetch in (("YouTube Music radio", _fetch_ytm_radio), ("YouTube related", _fetch_web_related)):
            try:
                tracks = fetch(seed_id)
            except Exception as e:
                log.warning("%s failed for %s: %s", name, seed_id, e)
                continue
            if tracks:
                break
            log.warning("%s returned nothing for %s", name, seed_id)

        seen = {seed_id}
        deduped = []
        for t in tracks:
            if t["id"] not in seen:
                seen.add(t["id"])
                deduped.append(t)

        if deduped:
            log.info("Radio: %d tracks for %s", len(deduped), seed_id)
        _RADIO_CACHE[seed_id] = (time.time() + (RADIO_TTL if deduped else RADIO_FAIL_TTL), deduped)
        return deduped


def is_radio_cached(seed_id: str) -> bool:
    hit = _RADIO_CACHE.get(seed_id)
    return bool(hit) and time.time() < hit[0]


def blend_recommendations(
    seeds: list[tuple[str, float]],
    exclude: set[str],
    max_sec: float = 0,
    target_sec: float = 0,
) -> list[dict]:
    """
    Merges the cached radio lists of several seed tracks into one ranking.

    Each list contributes weight / (1 + rank / 8) to every track it contains, so a
    track that sits near the top of several seeds' radios beats one that is only
    high on a single list. Songs over `max_sec` are dropped; with `target_sec`,
    each score is multiplied by how close the song's length is to the target.
    Never touches the network.
    """
    scores: dict[str, float] = {}
    tracks: dict[str, dict] = {}
    for seed_id, weight in seeds:
        hit = _RADIO_CACHE.get(seed_id)
        for rank, t in enumerate(hit[1] if hit else []):
            if t["id"] in exclude:
                continue
            scores[t["id"]] = scores.get(t["id"], 0.0) + weight / (1 + rank / 8)
            tracks.setdefault(t["id"], t)

    ranked = []
    for vid, score in scores.items():
        seconds = parse_duration(tracks[vid].get("duration", ""))
        if too_long(seconds, max_sec):
            continue
        ranked.append((score * length_fit(seconds, target_sec), tracks[vid]))
    ranked.sort(key=lambda x: x[0], reverse=True)
    return [t for _, t in ranked]


# -----------------------------------------------------------------------------
# yt-dlp Live Event Logger
# -----------------------------------------------------------------------------
class LiveEngineLogger:
    def __init__(self, engine: PrefetchEngine, video_id: str):
        self.engine = engine
        self.video_id = video_id

    def debug(self, msg: str):
        if not msg.startswith("[download]"):
            log.info(f"yt-dlp: {msg}")
        self._inspect(msg)

    def info(self, msg: str):
        log.info(f"yt-dlp: {msg}")
        self._inspect(msg)

    def warning(self, msg: str):
        if "JavaScript runtime" not in msg:
            log.warning(f"yt-dlp: {msg}")
        self._inspect(f"Warning: {msg}")

    def error(self, msg: str):
        log.error(f"yt-dlp: {msg}")
        self._inspect(f"Error: {msg}")

    def _inspect(self, msg: str):
        clean = msg.strip()
        hints = {
            "Downloading tv player API JSON": "Requesting stream (TV client)",
            "Downloading ios player API JSON": "Requesting stream (iOS client)",
            "Downloading android player API JSON": "Requesting stream (Android client)",
            "Downloading m3u8 information": "Reading stream manifest",
            "Resuming download": "Resuming partial download",
            "Destination:": "Stream found, transferring audio",
            "has already been downloaded": "Already in the vault",
            "HTTP Error 403: Forbidden": "Stream access denied (403)",
        }
        for token, user_hint in hints.items():
            if token in clean:
                with self.engine.lock:
                    st = self.engine.status_map.get(self.video_id)
                    if st:
                        st["msg"] = user_hint
                        if st["progress"] < 15.0:
                            st["progress"] += 3.0
                break


# -----------------------------------------------------------------------------
# Low-Bandwidth Sequential Download Engine
# -----------------------------------------------------------------------------
class PrefetchEngine:
    PREFETCH_AHEAD = 2   # besides the current track, keep this many upcoming tracks downloaded
    RADIO_MIN_AHEAD = 2  # radio keeps at least this many tracks queued after the current one
    MAX_ATTEMPTS = 3     # failed downloads are retried with backoff, then left alone

    def __init__(self):
        self.lock = threading.RLock()
        self.queue: list[dict] = []
        self.current_index: int = -1
        self.active_download_id: Optional[str] = None
        self.abort_current_download = threading.Event()
        self.wake = threading.Event()
        self.status_map: dict[str, dict] = {}
        self.failures: dict[str, tuple[int, float]] = {}  # id -> (attempts, retry_after)
        self.history: list[str] = []
        self.radio_enabled = bool(load_settings().get("radio_autofill", True))
        self.radio_exhausted_sig: Optional[tuple] = None

        # The one and only download thread: downloads never run in parallel.
        self.worker_thread = threading.Thread(
            target=self._worker_loop, daemon=True, name="PrefetchWorker"
        )
        self.worker_thread.start()

    # ---- vault helpers -------------------------------------------------------
    @staticmethod
    def _find_vault_file(video_id: str) -> Optional[pathlib.Path]:
        if not VIDEO_ID_RE.match(video_id or ""):
            return None
        for f in VAULT_DIR.glob(f"{video_id}.*"):
            if f.suffix not in PARTIAL_SUFFIXES and f.stat().st_size > 1024:
                return f
        return None

    def _is_downloaded(self, video_id: str) -> bool:
        return self._find_vault_file(video_id) is not None

    @staticmethod
    def _vault_ids() -> set[str]:
        ids = set()
        for f in VAULT_DIR.iterdir():
            if f.suffix not in PARTIAL_SUFFIXES and f.stat().st_size > 1024:
                ids.add(f.name.split(".", 1)[0])
        return ids

    @staticmethod
    def _purge_files(video_id: str) -> Optional[str]:
        """Deletes every file for a track. Returns an error string if a file stays locked."""
        error = None
        for _ in range(10):
            error = None
            for p in VAULT_DIR.glob(f"{video_id}.*"):
                try:
                    p.unlink(missing_ok=True)
                except OSError as e:
                    error = str(e)
            if not error:
                return None
            time.sleep(0.3)  # Windows: the file may still be open by a finishing audio response
        return error

    # ---- queue changes (callers hold no lock; each takes it) -----------------
    @staticmethod
    def _clean_track(t: dict) -> dict:
        vid = str(t.get("id") or "")
        if not VIDEO_ID_RE.match(vid):
            raise ValueError(f"Invalid video id: {vid!r}")
        return {
            "id": vid,
            "title": t.get("title") or "Unknown",
            "uploader": t.get("uploader") or "",
            "duration": t.get("duration") or "",
            "url": watch_url(vid),
            "source": t.get("source", "user"),
        }

    def _changed(self) -> None:
        """Must be called (under lock) after every queue change."""
        if 0 <= self.current_index < len(self.queue):
            cid = self.queue[self.current_index]["id"]
            if not self.history or self.history[-1] != cid:
                self.history = (self.history + [cid])[-300:]
        target = self._get_desired_download_target()
        if self.active_download_id and self.active_download_id != target:
            log.info("Queue changed: pausing download of %s (partial file kept)", self.active_download_id)
            self.abort_current_download.set()
        self.wake.set()

    def add(self, track: dict, mode: str = "end") -> None:
        track = self._clean_track(track)
        vid = track["id"]
        with self.lock:
            self.failures.pop(vid, None)
            if not self.queue or self.current_index < 0:
                self.queue = [track]
                self.current_index = 0
            elif mode == "end":
                if not any(t["id"] == vid for t in self.queue[self.current_index + 1:]):
                    self.queue.append(track)
            elif mode == "play" and self.queue[self.current_index]["id"] == vid:
                pass
            else:  # "next" or "play": slot it right after the current track
                head = self.queue[: self.current_index + 1]
                tail = [t for t in self.queue[self.current_index + 1:] if t["id"] != vid]
                self.queue = head + [track] + tail
                if mode == "play":
                    self.current_index += 1
            self._changed()

    def set_queue(self, tracks: list[dict], start_index: int = 0) -> None:
        cleaned = [self._clean_track(t) for t in tracks]
        with self.lock:
            self.queue = cleaned
            self.current_index = min(max(start_index, 0), len(cleaned) - 1)
            self._changed()

    def play_index(self, index: int) -> None:
        with self.lock:
            if 0 <= index < len(self.queue):
                self.current_index = index
                self.failures.pop(self.queue[index]["id"], None)
                self._changed()

    def next(self) -> bool:
        with self.lock:
            if self.current_index + 1 >= len(self.queue):
                if not self._extend_from_radio_cache():
                    self._extend_from_vault()
            elif not self._is_downloaded(self.queue[self.current_index + 1]["id"]):
                # Next track is still downloading: play a vault track meanwhile.
                self._insert_vault_filler()
            if self.current_index + 1 < len(self.queue):
                self.current_index += 1
                self._changed()
                return True
            return False

    def prev(self) -> None:
        with self.lock:
            if self.current_index > 0:
                self.current_index -= 1
                self._changed()

    def move_to(self, src: int, dst: int) -> None:
        """Moves the track at `src` so that it ends up at index `dst`."""
        with self.lock:
            if src == dst or not (0 <= src < len(self.queue) and 0 <= dst < len(self.queue)):
                return
            self.queue.insert(dst, self.queue.pop(src))
            cur = self.current_index
            if cur == src:
                cur = dst
            elif src < cur <= dst:
                cur -= 1
            elif dst <= cur < src:
                cur += 1
            self.current_index = cur
            self._changed()

    def remove(self, idx: int) -> None:
        with self.lock:
            if 0 <= idx < len(self.queue):
                self.queue.pop(idx)
                if self.current_index > idx:
                    self.current_index -= 1
                self.current_index = min(self.current_index, len(self.queue) - 1)
                self._changed()

    def clear_upcoming(self) -> None:
        with self.lock:
            self.queue = self.queue[: self.current_index + 1]
            self._changed()

    def set_radio(self, enabled: bool) -> None:
        with self.lock:
            self.radio_enabled = enabled
            self.radio_exhausted_sig = None
            self._changed()

    def delete_from_vault(self, video_id: str) -> Optional[str]:
        if not VIDEO_ID_RE.match(video_id or ""):
            return "Invalid track id"
        with self.lock:
            # Take it out of the queue too, or the prefetcher would fetch it straight back.
            i = 0
            while i < len(self.queue):
                if self.queue[i]["id"] == video_id:
                    self.queue.pop(i)
                    if i < self.current_index:
                        self.current_index -= 1
                    continue
                i += 1
            self.current_index = min(self.current_index, len(self.queue) - 1)
            self.status_map.pop(video_id, None)
            self.failures.pop(video_id, None)
            self._changed()
        error = self._purge_files(video_id)
        if error:
            return f"File is still in use: {error}"
        db_delete_track(video_id)
        log.info("Deleted from vault: %s", video_id)
        return None

    # ---- queue extension (called under lock) ---------------------------------
    def _upcoming_exclusions(self) -> set[str]:
        return {t["id"] for t in self.queue} | set(self.history)

    def _radio_seeds(self) -> list[tuple[str, float, str]]:
        """
        The tracks Radio is tuned from, as (id, weight, title).

        The current track counts most, then tracks you picked yourself, then
        tracks Radio added that you have already listened to.
        """
        if not (0 <= self.current_index < len(self.queue)):
            return []
        cur = self.current_index
        weights = {"user": 1.0, "vault": 0.6, "radio": 0.4}
        order = sorted(range(len(self.queue)), key=lambda i: (i != cur, abs(i - cur)))
        seeds: list[tuple[str, float, str]] = []
        seen: set[str] = set()
        for i in order:
            t = self.queue[i]
            source = t.get("source", "user")
            if t["id"] in seen or (i > cur and source == "radio"):
                continue  # upcoming radio picks are not a taste signal yet
            seen.add(t["id"])
            weight = 1.1 if i == cur else weights.get(source, 1.0)
            seeds.append((t["id"], weight, t.get("title", "")))
        # Keep the strongest signals when the queue is long (stable sort keeps proximity order).
        head, rest = seeds[:1], sorted(seeds[1:], key=lambda s: -s[1])
        return (head + rest)[:RADIO_MAX_SEEDS]

    def _blend(self, for_autofill: bool = False) -> list[dict]:
        """Radio ranking. Autofill also weighs each song's length against the target."""
        seeds = [(sid, w) for sid, w, _ in self._radio_seeds()]
        max_sec, target_sec = length_limits()
        return blend_recommendations(
            seeds, self._upcoming_exclusions(), max_sec, target_sec if for_autofill else 0
        )

    def _extend_from_radio_cache(self) -> bool:
        if not self.radio_enabled:
            return False
        recs = self._blend(for_autofill=True)
        if recs:
            self.queue.append({**recs[0], "source": "radio"})
            return True
        return False

    def _vault_pick(self) -> Optional[dict]:
        recent = set(self.history[-30:]) | {t["id"] for t in self.queue}
        pool = [t for t in db_get_all_tracks() if t["id"] not in recent]
        if not pool:
            return None
        t = random.choice(pool)
        return {**self._clean_track(t), "source": "vault"}

    def _extend_from_vault(self) -> None:
        pick = self._vault_pick()
        if pick:
            self.queue.append(pick)

    def _insert_vault_filler(self) -> None:
        pick = self._vault_pick()
        if pick:
            self.queue.insert(self.current_index + 1, pick)

    # ---- worker --------------------------------------------------------------
    def _get_desired_download_target(self) -> Optional[str]:
        if not (0 <= self.current_index < len(self.queue)):
            return None
        now = time.time()
        end = min(len(self.queue), self.current_index + 1 + self.PREFETCH_AHEAD)
        for i in range(self.current_index, end):
            vid = self.queue[i]["id"]
            if self._is_downloaded(vid):
                continue
            attempts, retry_after = self.failures.get(vid, (0, 0.0))
            if attempts >= self.MAX_ATTEMPTS or now < retry_after:
                continue
            return vid
        return None

    def _radio_step(self) -> bool:
        """
        One unit of idle-time Radio work: fetch one missing seed list, or top the
        queue up from the blend. Returns False when there is nothing to do.
        Only runs when no download is pending, so it never competes with audio.
        """
        with self.lock:
            if not self.radio_enabled:
                return False
            seeds = self._radio_seeds()
            missing = [sid for sid, _, _ in seeds if not is_radio_cached(sid)]
            need = self.RADIO_MIN_AHEAD - (len(self.queue) - self.current_index - 1)
            sig = tuple(sid for sid, _, _ in seeds)

        # The current track's list comes first: filling the queue depends on it.
        if seeds and seeds[0][0] in missing:
            get_recommendations(seeds[0][0])
            return True

        if need > 0 and seeds and sig != self.radio_exhausted_sig:
            with self.lock:
                recs = self._blend(for_autofill=True)[:need]
                for r in recs:
                    self.queue.append({**r, "source": "radio"})
                if recs:
                    log.info("Radio: queued %d track(s) blended from %d seed(s)", len(recs), len(seeds))
                if len(recs) < need:
                    self.radio_exhausted_sig = sig  # wait for the seeds to change before retrying
                self._changed()
            if recs:
                return True

        # Warm the remaining seeds so later picks reflect the whole queue.
        if missing:
            get_recommendations(missing[0])
            with self.lock:
                self.radio_exhausted_sig = None
            return True
        return False

    def radio_view(self) -> dict:
        """Blended Radio list for the UI."""
        with self.lock:
            seeds = self._radio_seeds()
        missing = [sid for sid, _, _ in seeds if not is_radio_cached(sid)]
        # The worker warms seeds while autofill is on; otherwise warm one per request.
        if missing and (seeds[0][0] == missing[0] or not self.radio_enabled):
            get_recommendations(missing.pop(0))
        with self.lock:
            tracks = self._blend()[:50]
        return {
            "seeds": [{"id": sid, "title": title} for sid, _, title in seeds],
            "pending": len(missing),
            "personalised": RADIO_PERSONALISED,
            "tracks": tracks,
        }

    def _worker_loop(self) -> None:
        while True:
            self.wake.clear()
            with self.lock:
                target_id = self._get_desired_download_target()
                track = next((t for t in self.queue if t["id"] == target_id), None) if target_id else None

            if track:
                self._download_track(track)
                continue

            try:
                if self._radio_step():
                    continue
            except Exception as e:
                log.error("Radio step failed: %s", e)

            self.wake.wait(1.0)

    def _download_track(self, track: dict) -> None:
        video_id = track["id"]
        if self._is_downloaded(video_id):
            return

        with self.lock:
            self.active_download_id = video_id
            self.abort_current_download.clear()
            self.status_map[video_id] = {
                "status": "connecting",
                "progress": 3.0,
                "msg": "Contacting YouTube",
            }

        settings = load_settings()

        def progress_hook(d):
            if self.abort_current_download.is_set():
                raise yt_dlp.utils.DownloadCancelled("Download paused by a queue change")

            if d.get("status") == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                downloaded = d.get("downloaded_bytes", 0)
                speed = d.get("speed")
                eta = d.get("eta")

                parts = [f"{round(speed / 1024, 1)} KB/s" if speed else "measuring speed"]
                parts.append(f"{downloaded // 1024} / {total // 1024} KB" if total else f"{downloaded // 1024} KB")
                if eta:
                    parts.append(f"{eta}s left")
                pct = round((downloaded / total) * 100, 1) if total > 0 else 5.0

                with self.lock:
                    self.status_map[video_id] = {
                        "status": "downloading",
                        "progress": pct,
                        "msg": " · ".join(parts),
                    }

        target_br = int(settings.get("bitrate", 48))
        if target_br <= 48:
            format_str = "599/worstaudio[abr<=48]/139/249/250/worstaudio"
        else:
            format_str = "250/249/139/ba[abr<=64]/worstaudio/worst"

        ydl_opts = {
            "format": format_str,
            "outtmpl": str(VAULT_DIR / f"{video_id}.%(ext)s"),
            "logger": LiveEngineLogger(self, video_id),
            "progress_hooks": [progress_hook],
            "quiet": False,
            "no_warnings": False,
            "socket_timeout": 30,
            "retries": 2,
            "fragment_retries": 2,
            "concurrent_fragment_downloads": 1,  # one connection, one file, at all times
            "continuedl": True,                  # resume .part files left by a paused download
            "buffersize": 1024 * 8,
            "nocheckcertificate": True,
            # No cookies for downloads: anonymous mobile/TV clients only.
            "extractor_args": {
                "youtube": [
                    "player_client=tv,ios,android",
                    "player_skip=webpage,configs,js",
                ]
            },
        }

        success = False
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(watch_url(video_id), download=True)
                downloaded_file = self._find_vault_file(video_id)
                if downloaded_file:
                    meta = info or track
                    db_add_track({**track, **meta, "id": video_id}, downloaded_file.name)
                    success = True
                    log.info("Stored in vault: %s (%s)", meta.get("title"), downloaded_file.name)
                    if info:
                        self._refresh_queue_metadata(video_id, info)
        except yt_dlp.utils.DownloadCancelled:
            with self.lock:
                still_queued = any(t["id"] == video_id for t in self.queue)
                self.status_map[video_id] = {"status": "paused", "progress": 0.0, "msg": "Paused"}
            if not still_queued:
                self._purge_files(video_id)
        except Exception as e:
            err_msg = str(e).split("\n")[0][:80]
            low = err_msg.lower()
            if "reloaded" in low or "bot" in low or "400" in low:
                err_msg = "YouTube blocked the request (rate limited)"
            with self.lock:
                attempts = self.failures.get(video_id, (0, 0.0))[0] + 1
                self.failures[video_id] = (attempts, time.time() + 20 * attempts)
                gave_up = attempts >= self.MAX_ATTEMPTS
                self.status_map[video_id] = {
                    "status": "error",
                    "progress": 0.0,
                    "msg": f"{err_msg}" + (" · press play to retry" if gave_up else f" · retry {attempts}/{self.MAX_ATTEMPTS}"),
                }
        finally:
            with self.lock:
                self.active_download_id = None
                if success:
                    self.failures.pop(video_id, None)
                    self.status_map[video_id] = {"status": "ready", "progress": 100.0, "msg": "Saved"}
                self.wake.set()

    def _refresh_queue_metadata(self, video_id: str, info: dict) -> None:
        """Pasted links start with a placeholder title; replace it with the real one."""
        with self.lock:
            for t in self.queue:
                if t["id"] == video_id:
                    t["title"] = info.get("title") or t["title"]
                    t["uploader"] = info.get("uploader") or info.get("channel") or t["uploader"]
                    t["duration"] = fmt_duration(info.get("duration")) or t["duration"]

    def get_state(self) -> dict:
        with self.lock:
            vault = self._vault_ids()
            items = []
            for item in self.queue:
                ready = item["id"] in vault
                st = self.status_map.get(item["id"], {})
                if ready:
                    status = "ready"
                elif item["id"] == self.active_download_id:
                    status = "downloading"
                elif st.get("status") == "error":
                    status = "error"
                else:
                    status = "queued"
                items.append({
                    **item,
                    "ready": ready,
                    "status": status,
                    "progress": 100.0 if ready else (st.get("progress", 0.0) if status == "downloading" else 0.0),
                    "msg": st.get("msg", "") if status in ("downloading", "error") else "",
                })
            return {
                "queue": items,
                "current_index": self.current_index,
                "active_download_id": self.active_download_id,
                "radio_enabled": self.radio_enabled,
                "radio_sig": ",".join(sid for sid, _, _ in self._radio_seeds()),
            }


ENGINE = PrefetchEngine()


# -----------------------------------------------------------------------------
# Threaded HTTP Server
# -----------------------------------------------------------------------------
class ThreadedAudioRequestHandler(BaseHTTPRequestHandler):
    def log_request(self, code="-", size="-"):
        # The UI polls /api/state every second; don't flood the console with it.
        if self.path.startswith("/api/state") and str(code).startswith("2"):
            return
        super().log_request(code, size)

    def log_message(self, format, *args):
        # Through logging rather than straight to stderr, which is None under pythonw.
        log.info("%s %s", self.address_string(), format % args)

    def end_headers(self):
        if not self.path.startswith("/api/audio/"):
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        super().end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._serve_embedded_html()
        elif path.startswith("/api/audio/"):
            self._serve_audio_range(path)
        elif path == "/api/state":
            self._json_response(ENGINE.get_state())
        elif path == "/api/library":
            self._json_response(db_get_all_tracks())
        elif path == "/api/settings":
            self._json_response(load_settings())
        elif path == "/api/search_stream":
            self._handle_stream_search(qs.get("q", [""])[0])
        elif path == "/api/recommendations":
            self._json_response(ENGINE.radio_view())
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length > 0 else {}
        except (ValueError, json.JSONDecodeError):
            self._json_response({"ok": False, "error": "Bad request body"}, 400)
            return

        try:
            result: dict = {"ok": True}
            if path == "/api/queue/add":
                ENGINE.add(body.get("track") or {}, body.get("mode", "end"))
            elif path == "/api/queue/set":
                ENGINE.set_queue(body.get("tracks", []), int(body.get("index", 0)))
            elif path == "/api/queue/play":
                ENGINE.play_index(int(body.get("index", 0)))
            elif path == "/api/queue/next":
                result["ok"] = ENGINE.next()
            elif path == "/api/queue/prev":
                ENGINE.prev()
            elif path == "/api/queue/move":
                ENGINE.move_to(int(body.get("from", 0)), int(body.get("to", 0)))
            elif path == "/api/queue/remove":
                ENGINE.remove(int(body.get("index", 0)))
            elif path == "/api/queue/clear":
                ENGINE.clear_upcoming()
            elif path == "/api/library/delete":
                error = ENGINE.delete_from_vault(str(body.get("id", "")))
                if error:
                    result = {"ok": False, "error": error}
            elif path == "/api/settings":
                settings = load_settings()
                for key in ("bitrate", "cookies_file", "radio_autofill", "max_length_min", "target_length_min"):
                    if key in body:
                        settings[key] = body[key]
                save_settings(settings)
                # Re-evaluates autofill too, so new length limits apply straight away.
                ENGINE.set_radio(bool(body.get("radio_autofill", ENGINE.radio_enabled)))
            else:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self._json_response(result)
        except ValueError as e:
            self._json_response({"ok": False, "error": str(e)}, 400)

    def _json_response(self, data: dict | list, status: int = 200):
        payload = json.dumps(data).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def _serve_audio_range(self, path: str):
        video_id = path[len("/api/audio/"):].split(".")[0]
        file_path = PrefetchEngine._find_vault_file(video_id)
        if not file_path:
            self.send_error(HTTPStatus.NOT_FOUND, "Track not found on disk")
            return

        file_size = file_path.stat().st_size
        mime_map = {".webm": "audio/webm", ".m4a": "audio/mp4", ".mp3": "audio/mpeg", ".opus": "audio/ogg"}
        content_type = mime_map.get(file_path.suffix.lower(), "audio/mpeg")

        start, end = 0, file_size - 1
        status = HTTPStatus.OK
        match = re.match(r"bytes=(\d*)-(\d*)$", (self.headers.get("Range") or "").strip())
        if match and (match.group(1) or match.group(2)):
            if match.group(1):
                start = int(match.group(1))
                if match.group(2):
                    end = min(int(match.group(2)), file_size - 1)
            else:  # suffix range: the last N bytes
                start = max(0, file_size - int(match.group(2)))
            if start > end:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{file_size}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            status = HTTPStatus.PARTIAL_CONTENT

        length = end - start + 1
        try:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(length))
            if status == HTTPStatus.PARTIAL_CONTENT:
                self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.end_headers()
            with open(file_path, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def _handle_stream_search(self, query: str):
        """Sends search results as newline-delimited JSON as soon as they are parsed."""
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.end_headers()

        def emit(track: dict) -> None:
            self.wfile.write(json.dumps(track).encode("utf-8") + b"\n")
            self.wfile.flush()

        try:
            vid = extract_youtube_id(query) if "youtu" in query else None
            if vid:
                emit({"id": vid, "title": f"YouTube link ({vid})", "uploader": "Pasted link",
                      "duration": "", "url": watch_url(vid)})
                return

            ydl_opts = {
                "extract_flat": True,
                "quiet": True,
                "no_warnings": True,
                "socket_timeout": 15,
                "ignoreerrors": True,
            }
            max_sec, _ = length_limits()
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"ytsearch12:{query}", download=False) or {}
                for entry in info.get("entries") or []:
                    if entry and entry.get("id") and not too_long(entry.get("duration"), max_sec):
                        emit(search_entry_to_track(entry))
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception as e:
            log.error("Search failed: %s", e)

    def _serve_embedded_html(self):
        html = EMBEDDED_UI.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html)))
        self.end_headers()
        self.wfile.write(html)


# -----------------------------------------------------------------------------
# Embedded Web Frontend
# -----------------------------------------------------------------------------
EMBEDDED_UI = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="dark">
<title>OffAir</title>
<style>
  :root {
    --bg: #0c0d0f;
    --panel: #111215;
    --raise: #17191d;
    --hover: #1e2126;
    --line: #23262b;
    --line-strong: #30343b;
    --text: #ecebe6;
    --muted: #8e9199;
    --faint: #5e626a;
    --accent: #f0a63a;
    --accent-dim: rgba(240, 166, 58, 0.11);
    --accent-line: rgba(240, 166, 58, 0.38);
    --accent-ink: #1b1204;
    --danger: #e5584f;
    --danger-dim: rgba(229, 88, 79, 0.12);
    --radius: 6px;
    --sans: "Inter", "Segoe UI Variable Text", "Segoe UI", system-ui, -apple-system, Roboto, sans-serif;
    --display: "Inter Display", "Segoe UI Variable Display", "Segoe UI", system-ui, -apple-system, sans-serif;
    --mono: "JetBrains Mono", "Cascadia Mono", Consolas, ui-monospace, monospace;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; }
  body { background: var(--bg); color: var(--text); font: 14px/1.45 var(--sans); -webkit-font-smoothing: antialiased; overflow: hidden; }
  button { font: inherit; color: inherit; background: none; border: 0; cursor: pointer; }
  svg { display: block; flex: none; }
  :focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

  .shell { display: grid; grid-template-rows: 56px minmax(0, 1fr) auto; height: 100vh; }

  /* Top bar */
  .top { display: flex; align-items: center; gap: 28px; padding: 0 20px; border-bottom: 1px solid var(--line); }
  .brand { display: flex; align-items: center; gap: 10px; font: 800 16px/1 var(--display); letter-spacing: 0.06em; text-transform: uppercase; }
  .lamp { width: 9px; height: 9px; border-radius: 50%; background: var(--line-strong); transition: background .3s, box-shadow .3s; }
  .lamp.on { background: var(--accent); box-shadow: 0 0 0 3px var(--accent-dim), 0 0 14px rgba(240, 166, 58, .45); }
  .tabs { display: flex; height: 100%; }
  .tab { position: relative; height: 100%; padding: 0 14px; font-weight: 650; color: var(--muted); }
  .tab:hover, .tab.active { color: var(--text); }
  .tab.active::after { content: ""; position: absolute; left: 14px; right: 14px; bottom: -1px; height: 2px; background: var(--accent); }
  .tab .n { margin-left: 6px; font: 500 11px var(--mono); color: var(--faint); }
  .spacer { flex: 1; }
  .net { display: flex; align-items: center; gap: 8px; min-width: 0; max-width: 420px; font: 12px var(--mono); color: var(--muted); }
  .net span { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .net .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--faint); flex: none; }
  .net.busy .dot { background: var(--accent); animation: pulse 1.4s ease-in-out infinite; }
  .net.err { color: var(--danger); }
  .net.err .dot { background: var(--danger); }
  @keyframes pulse { 50% { opacity: .3; } }

  /* Panes */
  .body { display: grid; grid-template-columns: minmax(0, 1fr) 340px; min-height: 0; }
  .pane { display: flex; flex-direction: column; min-height: 0; }
  .pane + .pane { border-left: 1px solid var(--line); }
  .head { padding: 26px 24px 14px; }
  .head h1 { font: 800 30px/1.1 var(--display); letter-spacing: -0.025em; }
  .head .sub { margin-top: 6px; color: var(--muted); font-size: 13px; }
  .head .sub b { color: var(--text); font-weight: 650; }
  .qhead { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 22px 16px 12px; }
  .label { font: 700 11px/1 var(--sans); letter-spacing: .14em; text-transform: uppercase; color: var(--muted); }
  .search { display: none; align-items: center; gap: 10px; max-width: 560px; height: 44px; margin-top: 16px; padding: 0 14px; background: var(--raise); border: 1px solid var(--line); border-radius: var(--radius); color: var(--faint); }
  .search.show { display: flex; }
  .search:focus-within { border-color: var(--line-strong); color: var(--muted); }
  .search input { flex: 1; min-width: 0; background: none; border: 0; outline: 0; color: var(--text); font: inherit; font-size: 15px; }
  .search input::placeholder { color: var(--faint); }

  .list { flex: 1; overflow-y: auto; padding: 4px 12px 24px; }
  .list::-webkit-scrollbar { width: 10px; }
  .list::-webkit-scrollbar-thumb { background: var(--line); border-radius: 5px; border: 3px solid var(--bg); }

  /* Track rows */
  .row { display: grid; grid-template-columns: 30px minmax(0, 1fr) auto auto; align-items: center; gap: 14px; padding: 8px 10px; border-radius: var(--radius); }
  .row:hover { background: var(--raise); }
  .idx { font: 500 12px var(--mono); color: var(--faint); text-align: right; }
  .info { min-width: 0; }
  .t { font-weight: 650; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .a { font-size: 12.5px; color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .d { font: 12px var(--mono); color: var(--faint); }
  .row.now .t, .row.now .idx { color: var(--accent); }
  .acts { display: flex; gap: 2px; opacity: 0; transition: opacity .12s; }
  .row:hover .acts, .row:focus-within .acts { opacity: 1; }
  @media (hover: none) { .acts { opacity: 1; } }
  .ib { width: 32px; height: 32px; display: grid; place-items: center; border-radius: var(--radius); color: var(--muted); }
  .ib:hover { background: var(--hover); color: var(--text); }
  .ib.danger:hover { color: var(--danger); background: var(--danger-dim); }
  .ib.confirm { width: auto; padding: 0 10px; color: var(--danger); background: var(--danger-dim); font-size: 12px; font-weight: 700; }
  .ib[disabled] { opacity: .25; pointer-events: none; }

  /* Queue rows */
  .qrow { grid-template-columns: 14px minmax(0, 1fr) auto; gap: 8px; padding: 8px 8px 8px 6px; cursor: grab; }
  .grip { color: var(--faint); opacity: 0; transition: opacity .12s; }
  .qrow:hover .grip { opacity: 1; }
  .qrow.dragging { opacity: .35; }
  .qrow.drop-before { box-shadow: inset 0 2px 0 var(--accent); }
  .qrow.drop-after { box-shadow: inset 0 -2px 0 var(--accent); }
  .qrow.now.drop-before { box-shadow: inset 2px 0 0 var(--accent), inset 0 2px 0 var(--accent); }
  .qrow.now.drop-after { box-shadow: inset 2px 0 0 var(--accent), inset 0 -2px 0 var(--accent); }
  .qrow.now { background: var(--accent-dim); box-shadow: inset 2px 0 0 var(--accent); }
  .qrow.now .t { color: var(--accent); }
  .qrow.past { opacity: .42; }
  .meta { display: flex; align-items: center; gap: 8px; min-width: 0; margin-top: 2px; }
  .chip { flex: none; font: 600 10px/1 var(--mono); letter-spacing: .05em; text-transform: uppercase; padding: 3px 5px; border-radius: 4px; color: var(--faint); border: 1px solid var(--line); }
  .chip.dl { color: var(--accent); border-color: var(--accent-line); }
  .chip.err { color: var(--danger); border-color: rgba(229, 88, 79, .38); }
  .qbar { height: 2px; margin-top: 7px; background: var(--line); border-radius: 1px; overflow: hidden; }
  .qbar i { display: block; height: 100%; background: var(--accent); transition: width .4s; }
  .qmsg { margin-top: 4px; font: 11px var(--mono); color: var(--faint); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .qmsg.err { color: var(--danger); }

  .switch { display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--muted); cursor: pointer; user-select: none; }
  .switch input { appearance: none; -webkit-appearance: none; position: relative; width: 28px; height: 16px; border-radius: 8px; background: var(--line-strong); cursor: pointer; transition: background .2s; }
  .switch input::after { content: ""; position: absolute; top: 2px; left: 2px; width: 12px; height: 12px; border-radius: 50%; background: var(--text); transition: transform .2s; }
  .switch input:checked { background: var(--accent); }
  .switch input:checked::after { transform: translateX(12px); background: var(--accent-ink); }

  .empty { padding: 56px 24px; text-align: center; color: var(--muted); font-size: 13px; }
  .empty b { display: block; margin-bottom: 6px; color: var(--text); font: 700 16px var(--display); }
  .empty .btn { margin-top: 16px; }
  .btn { height: 34px; padding: 0 14px; border-radius: var(--radius); font-size: 13px; font-weight: 650; background: var(--raise); border: 1px solid var(--line-strong); }
  .btn:hover { background: var(--hover); }
  .btn.primary { background: var(--accent); border-color: var(--accent); color: var(--accent-ink); }
  .btn.primary:hover { filter: brightness(1.08); }

  /* Player */
  .player { position: relative; display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.4fr) minmax(0, 1fr); align-items: center; gap: 28px; height: 88px; padding: 0 20px; background: var(--panel); border-top: 1px solid var(--line); }
  .dlbar { position: absolute; left: 0; right: 0; top: -1px; height: 2px; pointer-events: none; }
  .dlbar i { display: block; width: 0; height: 100%; background: var(--accent); transition: width .4s; }
  .np { min-width: 0; }
  .np-t { font: 750 15px/1.3 var(--display); letter-spacing: -0.01em; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .np-a { font-size: 12.5px; color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .transport { display: flex; flex-direction: column; align-items: center; gap: 6px; min-width: 0; }
  .ctrls { display: flex; align-items: center; gap: 12px; }
  .ctrls .ib { color: var(--text); }
  .pp { width: 38px; height: 38px; border-radius: 50%; display: grid; place-items: center; background: var(--text); color: var(--bg); transition: transform .1s; }
  .pp:hover { transform: scale(1.05); }
  .seek { display: flex; align-items: center; gap: 10px; width: 100%; font: 11.5px var(--mono); color: var(--faint); }
  .seek span { width: 40px; }
  .seek span:first-child { text-align: right; }
  input[type=range] { -webkit-appearance: none; appearance: none; flex: 1; min-width: 0; height: 4px; border-radius: 2px; outline: 0; cursor: pointer; background: linear-gradient(to right, var(--text) var(--p, 0%), var(--line-strong) var(--p, 0%)); }
  input[type=range]::-webkit-slider-thumb { -webkit-appearance: none; width: 12px; height: 12px; border-radius: 50%; background: var(--text); opacity: 0; transition: opacity .15s; }
  input[type=range]:hover::-webkit-slider-thumb, input[type=range]:focus-visible::-webkit-slider-thumb { opacity: 1; }
  input[type=range]::-moz-range-thumb { width: 12px; height: 12px; border: 0; border-radius: 50%; background: var(--text); }
  .aux { display: flex; flex-direction: column; align-items: flex-end; gap: 10px; min-width: 0; }
  .upnext { max-width: 100%; font-size: 12px; color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .upnext b { color: var(--text); font-weight: 650; }
  .upnext em { font-style: normal; color: var(--accent); font-family: var(--mono); font-size: 11.5px; }
  .vol { display: flex; align-items: center; gap: 8px; width: 150px; color: var(--faint); }

  /* Modal + toast */
  .modal-bg { position: fixed; inset: 0; z-index: 10; display: none; place-items: center; background: rgba(0, 0, 0, .6); }
  .modal-bg.open { display: grid; }
  .modal { width: min(460px, calc(100vw - 32px)); display: flex; flex-direction: column; gap: 18px; padding: 24px; background: var(--panel); border: 1px solid var(--line-strong); border-radius: 10px; }
  .modal h3 { font: 800 20px var(--display); letter-spacing: -0.02em; }
  .field { display: flex; flex-direction: column; gap: 7px; }
  .field select, .field input { height: 38px; padding: 0 10px; background: var(--raise); border: 1px solid var(--line); border-radius: var(--radius); color: var(--text); font: inherit; }
  .field .hint { font-size: 12px; color: var(--faint); }
  .pair { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .unit { display: flex; align-items: center; gap: 8px; color: var(--muted); font-size: 13px; }
  .unit input { width: 100%; min-width: 0; }
  .foot { display: flex; justify-content: flex-end; gap: 8px; }
  .toast { position: fixed; right: 20px; bottom: 104px; z-index: 20; max-width: 360px; padding: 10px 14px; background: var(--raise); border: 1px solid var(--line-strong); border-radius: var(--radius); font-size: 13px; opacity: 0; transform: translateY(6px); transition: opacity .2s, transform .2s; pointer-events: none; }
  .toast.show { opacity: 1; transform: none; }

  @media (max-width: 880px) {
    .top { gap: 12px; padding: 0 16px; }
    .net { display: none; }
    .body { grid-template-columns: minmax(0, 1fr); grid-template-rows: minmax(0, 1fr) minmax(0, 38%); }
    .pane + .pane { border-left: 0; border-top: 1px solid var(--line); }
    .head { padding: 18px 16px 10px; }
    .head h1 { font-size: 24px; }
    .player { grid-template-columns: minmax(0, 1fr); height: auto; gap: 8px; padding: 12px 16px; }
    .aux { display: none; }
  }
</style>
</head>
<body>
<div class="shell">
  <header class="top">
    <div class="brand"><span class="lamp" id="lamp"></span>OffAir</div>
    <nav class="tabs" id="tabs">
      <button class="tab active" data-view="library">Vault<span class="n" id="libCount"></span></button>
      <button class="tab" data-view="search">Search</button>
      <button class="tab" data-view="radio">Radio</button>
    </nav>
    <div class="spacer"></div>
    <div class="net" id="net"><i class="dot"></i><span id="netText">Idle</span></div>
    <button class="ib" id="settingsBtn" title="Settings" aria-label="Settings"></button>
  </header>

  <main class="body">
    <section class="pane">
      <div class="head">
        <h1 id="viewTitle">Vault</h1>
        <p class="sub" id="viewSub"></p>
        <label class="search" id="searchBox">
          <span id="searchIcon"></span>
          <input type="text" id="searchInput" placeholder="Artist, song, or a YouTube link" autocomplete="off" spellcheck="false">
        </label>
      </div>
      <div class="list" id="mainList"></div>
    </section>
    <aside class="pane">
      <div class="qhead">
        <span class="label">Queue</span>
        <label class="switch" title="Keep the queue topped up with similar tracks">Radio autofill <input type="checkbox" id="radioToggle"></label>
      </div>
      <div class="list" id="queueList"></div>
    </aside>
  </main>

  <footer class="player">
    <div class="dlbar"><i id="dlBar"></i></div>
    <div class="np">
      <div class="np-t" id="npTitle">Nothing playing</div>
      <div class="np-a" id="npSub">Pick something from your vault or search</div>
    </div>
    <div class="transport">
      <div class="ctrls">
        <button class="ib" id="prevBtn" title="Previous" aria-label="Previous"></button>
        <button class="pp" id="ppBtn" title="Play / pause" aria-label="Play or pause"></button>
        <button class="ib" id="nextBtn" title="Next" aria-label="Next"></button>
      </div>
      <div class="seek">
        <span id="curTime">0:00</span>
        <input type="range" id="seek" min="0" max="100" step="0.1" value="0" aria-label="Seek">
        <span id="totTime">0:00</span>
      </div>
    </div>
    <div class="aux">
      <div class="upnext" id="upNext"></div>
      <div class="vol"><span id="volIcon"></span><input type="range" id="vol" min="0" max="1" step="0.01" value="0.9" aria-label="Volume"></div>
    </div>
  </footer>
</div>

<audio id="audio" preload="auto"></audio>
<div class="toast" id="toast"></div>

<div class="modal-bg" id="settingsModal">
  <div class="modal" role="dialog" aria-labelledby="settingsTitle">
    <h3 id="settingsTitle">Settings</h3>
    <div class="field">
      <span class="label">Download quality</span>
      <select id="cfgBitrate">
        <option value="48">Ultra-low · 30 to 48 kbps · about 1 MB per song</option>
        <option value="64">Compact · 64 kbps · about 1.5 MB per song</option>
      </select>
    </div>
    <div class="pair">
      <div class="field">
        <span class="label">Max song length</span>
        <span class="unit"><input type="number" id="cfgMaxLen" min="0" max="180" step="1"> min</span>
      </div>
      <div class="field">
        <span class="label">Target length</span>
        <span class="unit"><input type="number" id="cfgTargetLen" min="0" max="60" step="0.5"> min</span>
      </div>
    </div>
    <span class="hint" style="margin-top:-10px">Longer songs are hidden from Radio and Search. Radio autofill prefers songs near the target length, weighed against how well they match. 0 turns either off.</span>
    <div class="field">
      <span class="label">Cookies file (optional)</span>
      <input type="text" id="cfgCookies" placeholder="C:\Users\you\Downloads\www.youtube.com_cookies.txt">
      <span class="hint">Netscape cookies.txt. Only used to personalise Radio; downloads never send cookies.</span>
    </div>
    <div class="foot">
      <button class="btn" id="cfgCancel">Cancel</button>
      <button class="btn primary" id="cfgSave">Save</button>
    </div>
  </div>
</div>

<script>
const $ = (id) => document.getElementById(id);
const audio = $('audio');

const ICONS = {
  play: '<path d="M7 4.6v14.8a.6.6 0 0 0 .9.5l12.2-7.4a.6.6 0 0 0 0-1L7.9 4.1a.6.6 0 0 0-.9.5z" fill="currentColor" stroke="none"/>',
  pause: '<rect x="6" y="4.5" width="4" height="15" rx="1" fill="currentColor" stroke="none"/><rect x="14" y="4.5" width="4" height="15" rx="1" fill="currentColor" stroke="none"/>',
  prev: '<path d="M6 5v14"/><path d="M19 5.6v12.8a.6.6 0 0 1-.9.5L9 12.5a.6.6 0 0 1 0-1l9.1-6.4a.6.6 0 0 1 .9.5z" fill="currentColor" stroke="none"/>',
  next: '<path d="M18 5v14"/><path d="M5 5.6v12.8a.6.6 0 0 0 .9.5l9.1-6.4a.6.6 0 0 0 0-1L5.9 5.1a.6.6 0 0 0-.9.5z" fill="currentColor" stroke="none"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  playnext: '<path d="M4 6h12M4 11h8M4 16h5"/><path d="M14 13l5 3.5-5 3.5z" fill="currentColor"/>',
  grip: '<circle cx="9" cy="6" r="1.4"/><circle cx="15" cy="6" r="1.4"/><circle cx="9" cy="12" r="1.4"/><circle cx="15" cy="12" r="1.4"/><circle cx="9" cy="18" r="1.4"/><circle cx="15" cy="18" r="1.4"/>',
  x: '<path d="M6 6l12 12M18 6L6 18"/>',
  trash: '<path d="M4 7h16M10 11v6M14 11v6M6 7l1 12.5a1 1 0 0 0 1 .5h8a1 1 0 0 0 1-.5L18 7M9 7V4.5h6V7"/>',
  search: '<circle cx="11" cy="11" r="6.5"/><path d="M20 20l-4.2-4.2"/>',
  settings: '<path d="M4 7h9M17 7h3M4 17h3M11 17h9"/><circle cx="15" cy="7" r="2"/><circle cx="9" cy="17" r="2"/>',
  volume: '<path d="M4 9.5h3.5L12 5.5v13l-4.5-4H4z" fill="currentColor" stroke="none"/><path d="M15.5 9a4 4 0 0 1 0 6M18 6.5a7.5 7.5 0 0 1 0 11"/>',
};
const icon = (name, size = 16) =>
  `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICONS[name]}</svg>`;
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

async function api(path, body) {
  const opts = body === undefined ? {} : { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) };
  const res = await fetch(path, opts);
  return res.json();
}

function toast(msg) {
  const el = $('toast');
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => el.classList.remove('show'), 3500);
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let state = { queue: [], current_index: -1, active_download_id: null, radio_enabled: true };
let view = 'library';
let library = [];
let search = { query: '', results: [], status: 'idle', ctl: null, timer: null };
let radio = { sig: null, seeds: [], tracks: [], pending: 0, personalised: null, status: 'idle', timer: null };
let drag = null;  // { from, to } while a queue row is being dragged
let pendingDelete = null;
let queueSig = '';
let lastActive = null;

// Playback: the <audio> element only ever switches tracks when
//  - the user explicitly picked a track that is already downloaded (userJump), or
//  - nothing is currently playing (the previous track ended, or playback is paused).
// A track that finishes downloading never cuts off the song you are listening to.
let loadedId = null;
let loadedMeta = null;
let userJump = false;

const toWire = (t) => ({ id: t.id, title: t.title, uploader: t.uploader, duration: t.duration || '' });
const target = () => state.queue[state.current_index] || null;
const isPlaying = () => !!loadedId && !audio.paused && !audio.ended;

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------
function itemsForView() {
  return view === 'library' ? library : view === 'search' ? search.results : radio.tracks;
}

function switchView(v) {
  view = v;
  pendingDelete = null;
  document.querySelectorAll('.tab').forEach((b) => b.classList.toggle('active', b.dataset.view === v));
  $('searchBox').classList.toggle('show', v === 'search');
  if (v === 'search') $('searchInput').focus();
  if (v === 'radio') ensureRadio();
  renderMain();
}

function emptyState(title, text, action = '') {
  return `<div class="empty"><b>${title}</b>${text}${action}</div>`;
}

function renderHead() {
  const titles = { library: 'Vault', search: 'Search', radio: 'Radio' };
  $('viewTitle').textContent = titles[view];
  let sub = '';
  if (view === 'library') {
    sub = library.length ? `${library.length} track${library.length === 1 ? '' : 's'} saved for offline listening` : 'Everything you play is saved here.';
  } else if (view === 'search') {
    sub = 'Results appear as soon as YouTube answers.';
  } else {
    sub = radioSubtitle();
  }
  $('viewSub').innerHTML = sub;
}

function radioSubtitle() {
  if (!radio.seeds.length) return 'Music picked from everything in your queue.';
  const names = radio.seeds.slice(0, 3).map((x) => `<b>${esc(x.title)}</b>`).join(', ');
  const more = radio.seeds.length > 3 ? ` and ${radio.seeds.length - 3} more` : '';
  const who = radio.personalised === true ? 'Personalised with your YouTube account'
    : radio.personalised === false ? 'Not personalised (cookies not accepted)' : '';
  const pending = radio.pending ? `Still tuning ${radio.pending} of ${radio.seeds.length} tracks` : '';
  return `Blended from ${names}${more}` + [who, pending].filter(Boolean).map((x) => `<br>${x}`).join('');
}

function renderMain() {
  renderHead();
  const list = $('mainList');
  const items = itemsForView();

  if (!items.length) {
    if (view === 'library') {
      list.innerHTML = emptyState('Your vault is empty', 'Search for something to play. Each track is downloaded once and kept for offline listening.');
    } else if (view === 'search') {
      list.innerHTML =
        search.status === 'loading' ? emptyState('Searching', 'Waiting for YouTube to answer.')
        : search.status === 'error' ? emptyState('Search interrupted', 'The connection dropped. Try again.')
        : search.status === 'done' ? emptyState('No results', 'Try different words or paste a link.')
        : emptyState('Find music', 'Type an artist, a song, or paste a YouTube link.');
    } else {
      list.innerHTML =
        !radio.seeds.length && radio.status !== 'loading' ? emptyState('Nothing to tune from yet', 'Play a track and Radio will blend picks from your whole queue.')
        : radio.status === 'loading' || radio.pending ? emptyState('Tuning in', 'Fetching radio lists, about 35 KB per track.')
        : emptyState('Radio could not load', 'YouTube did not return recommendations for this track.', '<br><button class="btn" data-act="retry-radio">Try again</button>');
    }
    return;
  }

  list.innerHTML = items.map((t, i) => {
    const del = view === 'library'
      ? (pendingDelete === t.id
          ? `<button class="ib confirm" data-act="del">Delete</button>`
          : `<button class="ib danger" data-act="del" title="Delete from vault" aria-label="Delete from vault">${icon('trash')}</button>`)
      : '';
    return `
      <div class="row${t.id === loadedId ? ' now' : ''}" data-i="${i}">
        <div class="idx">${String(i + 1).padStart(2, '0')}</div>
        <div class="info"><div class="t">${esc(t.title)}</div><div class="a">${esc(t.uploader || 'Unknown artist')}</div></div>
        <div class="d">${esc(t.duration || '')}</div>
        <div class="acts">
          <button class="ib" data-act="play" title="Play" aria-label="Play">${icon('play')}</button>
          <button class="ib" data-act="next" title="Play next" aria-label="Play next">${icon('playnext')}</button>
          <button class="ib" data-act="add" title="Add to queue" aria-label="Add to queue">${icon('plus')}</button>
          ${del}
        </div>
      </div>`;
  }).join('');
}

function renderQueue() {
  const q = state.queue;
  const ci = state.current_index;
  const sig = JSON.stringify([q, ci]);
  if (drag || sig === queueSig) return;  // never re-render under a drag in progress
  queueSig = sig;

  const list = $('queueList');
  if (!q.length) {
    list.innerHTML = emptyState('Queue is empty', 'Press play on anything to start.');
    return;
  }
  list.innerHTML = q.map((t, i) => {
    let chip = '';
    let extra = '';
    if (t.status === 'downloading') {
      const p = Math.max(1, Math.round(t.progress || 0));
      chip = `<span class="chip dl">${p}%</span>`;
      extra = `<div class="qbar"><i style="width:${p}%"></i></div><div class="qmsg">${esc(t.msg)}</div>`;
    } else if (t.status === 'error') {
      chip = '<span class="chip err">Failed</span>';
      extra = `<div class="qmsg err">${esc(t.msg)}</div>`;
    } else if (t.status === 'queued') {
      chip = '<span class="chip">Waiting</span>';
    } else if (t.source === 'radio') {
      chip = '<span class="chip">Radio</span>';
    }
    const cls = i === ci ? ' now' : i < ci ? ' past' : '';
    return `
      <div class="row qrow${cls}" data-i="${i}" draggable="true">
        <div class="grip">${icon('grip', 14)}</div>
        <div class="info">
          <div class="t">${esc(t.title)}</div>
          <div class="meta"><span class="a">${esc(t.uploader || '')}</span>${chip}</div>
          ${extra}
        </div>
        <div class="acts">
          <button class="ib" data-act="play" title="Play" aria-label="Play">${icon('play', 14)}</button>
          <button class="ib danger" data-act="rm" title="Remove" aria-label="Remove from queue">${icon('x', 14)}</button>
        </div>
      </div>`;
  }).join('');
}

function renderNet() {
  const net = $('net');
  const active = state.queue.find((t) => t.id === state.active_download_id);
  if (active) {
    net.className = 'net busy';
    $('netText').textContent = `${active.title} · ${active.msg || 'starting'}`;
    $('dlBar').style.width = `${Math.max(1, active.progress || 0)}%`;
  } else {
    const failed = state.queue.find((t, i) => t.status === 'error' && i >= state.current_index && i <= state.current_index + 2);
    net.className = failed ? 'net err' : 'net';
    $('netText').textContent = failed ? `Download failed: ${failed.title}` : 'Idle';
    $('dlBar').style.width = '0%';
  }
}

function statusWord(t) {
  if (t.ready) return 'ready';
  if (t.status === 'downloading') return `${Math.round(t.progress || 0)}%`;
  if (t.status === 'error') return 'failed';
  return 'waiting';
}

function renderNowPlaying() {
  const t = target();
  const m = (loadedId && (state.queue.find((x) => x.id === loadedId) || loadedMeta)) || null;

  if (m) {
    $('npTitle').textContent = m.title;
    $('npSub').textContent = m.uploader || '';
  } else if (t) {
    $('npTitle').textContent = t.title;
    $('npSub').textContent = t.status === 'error' ? 'Download failed' : 'Downloading before playback';
  } else {
    $('npTitle').textContent = 'Nothing playing';
    $('npSub').textContent = 'Pick something from your vault or search';
  }

  let up = '';
  if (t && m && t.id !== m.id) {
    up = `After this track: <b>${esc(t.title)}</b> <em>${statusWord(t)}</em>`;
  } else {
    const nx = state.queue[state.current_index + 1];
    if (nx) up = `Next: <b>${esc(nx.title)}</b>${nx.ready ? '' : ` <em>${statusWord(nx)}</em>`}`;
  }
  $('upNext').innerHTML = up;
  $('lamp').classList.toggle('on', isPlaying());
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------
async function loadLibrary() {
  try {
    library = await api('/api/library');
    $('libCount').textContent = library.length || '';
    if (view === 'library') renderMain();
  } catch (e) {}
}

async function ensureRadio(force = false) {
  const sig = state.radio_sig || '';
  if (!force && radio.sig === sig && radio.status !== 'error') return;
  clearTimeout(radio.timer);
  radio.sig = sig;
  if (!radio.tracks.length) radio.status = 'loading';
  if (view === 'radio') renderMain();
  try {
    const res = await api('/api/recommendations');
    if (radio.sig !== sig) return;
    Object.assign(radio, { seeds: res.seeds || [], tracks: res.tracks || [], pending: res.pending || 0, personalised: res.personalised });
    radio.status = radio.tracks.length || radio.pending || !radio.seeds.length ? 'done' : 'error';
  } catch (e) {
    if (radio.sig === sig) radio.status = 'error';
  }
  // Some seed lists are still being fetched in the background: look again shortly.
  if (radio.pending && view === 'radio') radio.timer = setTimeout(() => ensureRadio(true), 4000);
  if (view === 'radio') renderMain();
}

async function runSearch(query) {
  if (search.ctl) search.ctl.abort();
  const ctl = new AbortController();
  search = { ...search, query, results: [], status: 'loading', ctl };
  renderMain();
  try {
    const res = await fetch('/api/search_stream?q=' + encodeURIComponent(query), { signal: ctl.signal });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        if (line.trim()) search.results.push(JSON.parse(line));
      }
      if (view === 'search') renderMain();
    }
    search.status = 'done';
  } catch (err) {
    if (err.name === 'AbortError') return;
    search.status = 'error';
  }
  if (view === 'search') renderMain();
}

// ---------------------------------------------------------------------------
// Playback
// ---------------------------------------------------------------------------
function loadTrack(t) {
  userJump = false;
  loadedId = t.id;
  loadedMeta = { ...t };
  audio.src = '/api/audio/' + encodeURIComponent(t.id);
  audio.play().catch(() => {});
  if ('mediaSession' in navigator) {
    navigator.mediaSession.metadata = new MediaMetadata({ title: t.title, artist: t.uploader || '' });
  }
  renderMain();
}

function reconcilePlayer() {
  const t = target();
  if (t && t.ready && t.id !== loadedId && (userJump || !isPlaying())) {
    loadTrack(t);
  } else if (t && !t.ready && userJump) {
    // Picked a track that is still downloading: keep the current song going.
    userJump = false;
  }
  if (view === 'radio' && (state.radio_sig || '') !== radio.sig) ensureRadio();
  renderNowPlaying();
}

async function sync() {
  try {
    state = await api('/api/state');
  } catch (e) {
    return;
  }
  if (lastActive && lastActive !== state.active_download_id) loadLibrary();
  lastActive = state.active_download_id;
  const tog = $('radioToggle');
  if (document.activeElement !== tog) tog.checked = !!state.radio_enabled;
  renderQueue();
  renderNet();
  reconcilePlayer();
}

async function queueAction(path, body) {
  try {
    const res = await api(path, body);
    if (res && res.ok === false && res.error) toast(res.error);
    return res;
  } finally {
    sync();
  }
}

async function playNext() {
  userJump = true;
  const res = await queueAction('/api/queue/next', {});
  if (res && res.ok === false) {
    userJump = false;
    toast('End of queue');
  }
}

async function playPrev() {
  if (audio.currentTime > 4) {
    audio.currentTime = 0;
    return;
  }
  userJump = true;
  await queueAction('/api/queue/prev', {});
}

function togglePlay() {
  if (!loadedId) {
    const t = target();
    if (t && t.ready) loadTrack(t);
    return;
  }
  if (audio.paused) audio.play().catch(() => {});
  else audio.pause();
}

async function deleteTrack(id) {
  if (loadedId === id) {
    audio.pause();
    audio.removeAttribute('src');
    audio.load();
    loadedId = null;
    loadedMeta = null;
  }
  const res = await queueAction('/api/library/delete', { id });
  if (res && res.ok) toast('Removed from vault');
  loadLibrary();
}

audio.addEventListener('ended', async () => {
  const t = target();
  if (t && t.id !== loadedId) {
    // A switch was waiting for this song to finish.
    if (t.ready) loadTrack(t);
    renderNowPlaying();
    return;
  }
  await playNext();
});
audio.addEventListener('play', () => { $('ppBtn').innerHTML = icon('pause', 18); renderNowPlaying(); });
audio.addEventListener('pause', () => { $('ppBtn').innerHTML = icon('play', 18); renderNowPlaying(); });

let seeking = false;
const seekEl = $('seek');
function fmt(secs) {
  if (!secs || !isFinite(secs)) return '0:00';
  const m = Math.floor(secs / 60);
  const s = Math.floor(secs % 60);
  return `${m}:${String(s).padStart(2, '0')}`;
}
audio.addEventListener('timeupdate', () => {
  const dur = audio.duration || 0;
  if (!seeking) {
    seekEl.max = dur || 100;
    seekEl.value = audio.currentTime || 0;
    seekEl.style.setProperty('--p', dur ? `${(audio.currentTime / dur) * 100}%` : '0%');
  }
  $('curTime').textContent = fmt(audio.currentTime);
  $('totTime').textContent = fmt(dur);
});
seekEl.addEventListener('input', () => {
  seeking = true;
  const dur = audio.duration || 0;
  seekEl.style.setProperty('--p', dur ? `${(seekEl.value / dur) * 100}%` : '0%');
});
seekEl.addEventListener('change', () => {
  if (audio.duration) audio.currentTime = Number(seekEl.value);
  seeking = false;
});

const volEl = $('vol');
function setVolume(v) {
  audio.volume = v;
  volEl.value = v;
  volEl.style.setProperty('--p', `${v * 100}%`);
}
volEl.addEventListener('input', () => {
  setVolume(Number(volEl.value));
  try { localStorage.setItem('offair.volume', volEl.value); } catch (e) {}
});

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------
$('tabs').addEventListener('click', (e) => {
  const b = e.target.closest('[data-view]');
  if (b) switchView(b.dataset.view);
});

$('searchInput').addEventListener('input', (e) => {
  clearTimeout(search.timer);
  const q = e.target.value.trim();
  if (!q) {
    if (search.ctl) search.ctl.abort();
    search = { ...search, query: '', results: [], status: 'idle' };
    renderMain();
    return;
  }
  search.timer = setTimeout(() => runSearch(q), 450);
});

$('mainList').addEventListener('click', async (e) => {
  const btn = e.target.closest('[data-act]');
  if (!btn) return;
  const act = btn.dataset.act;
  if (act === 'retry-radio') return ensureRadio(true);

  const row = btn.closest('.row');
  const t = row && itemsForView()[Number(row.dataset.i)];
  if (!t) return;

  if (act === 'play') {
    userJump = true;
    await queueAction('/api/queue/add', { track: toWire(t), mode: 'play' });
  } else if (act === 'next') {
    await queueAction('/api/queue/add', { track: toWire(t), mode: 'next' });
    toast(`Playing next: ${t.title}`);
  } else if (act === 'add') {
    await queueAction('/api/queue/add', { track: toWire(t), mode: 'end' });
    toast(`Added to queue: ${t.title}`);
  } else if (act === 'del') {
    if (pendingDelete !== t.id) {
      pendingDelete = t.id;
      clearTimeout(deleteTrack.timer);
      deleteTrack.timer = setTimeout(() => { pendingDelete = null; if (view === 'library') renderMain(); }, 3000);
      renderMain();
      return;
    }
    pendingDelete = null;
    await deleteTrack(t.id);
  }
});

$('mainList').addEventListener('dblclick', (e) => {
  const row = e.target.closest('.row');
  if (row && !e.target.closest('[data-act]')) row.querySelector('[data-act="play"]').click();
});

$('queueList').addEventListener('click', async (e) => {
  const btn = e.target.closest('[data-act]');
  const row = e.target.closest('.row');
  if (!btn || !row) return;
  const i = Number(row.dataset.i);
  const act = btn.dataset.act;
  if (act === 'play') {
    userJump = true;
    await queueAction('/api/queue/play', { index: i });
  } else if (act === 'rm') {
    await queueAction('/api/queue/remove', { index: i });
  }
});

// Drag and drop reordering in the queue.
const queueEl = $('queueList');
function clearDropMarks() {
  queueEl.querySelectorAll('.drop-before, .drop-after').forEach((el) => el.classList.remove('drop-before', 'drop-after'));
}
queueEl.addEventListener('dragstart', (e) => {
  const row = e.target.closest('.qrow');
  if (!row) return;
  drag = { from: Number(row.dataset.i), to: null };
  e.dataTransfer.effectAllowed = 'move';
  e.dataTransfer.setData('text/plain', row.dataset.i);
  requestAnimationFrame(() => row.classList.add('dragging'));
});
queueEl.addEventListener('dragover', (e) => {
  const row = e.target.closest('.qrow');
  if (!drag || !row) return;
  e.preventDefault();
  e.dataTransfer.dropEffect = 'move';
  const i = Number(row.dataset.i);
  const r = row.getBoundingClientRect();
  const before = e.clientY < r.top + r.height / 2;
  const slot = before ? i : i + 1;                       // insertion point in the old list
  drag.to = slot > drag.from ? slot - 1 : slot;          // final index after removal
  clearDropMarks();
  if (drag.to !== drag.from) row.classList.add(before ? 'drop-before' : 'drop-after');
});
queueEl.addEventListener('drop', async (e) => {
  e.preventDefault();
  const d = drag;
  drag = null;
  clearDropMarks();
  if (d && d.to !== null && d.to !== d.from) {
    queueSig = '';
    await queueAction('/api/queue/move', { from: d.from, to: d.to });
  } else {
    queueSig = '';
    renderQueue();
  }
});
queueEl.addEventListener('dragend', () => {
  if (!drag) return;  // drop already handled it
  drag = null;
  clearDropMarks();
  queueSig = '';
  renderQueue();
});

$('radioToggle').addEventListener('change', (e) => {
  queueAction('/api/settings', { radio_autofill: e.target.checked });
});

$('ppBtn').addEventListener('click', togglePlay);
$('nextBtn').addEventListener('click', playNext);
$('prevBtn').addEventListener('click', playPrev);

document.addEventListener('keydown', (e) => {
  const typing = /^(INPUT|SELECT|TEXTAREA)$/.test(document.activeElement.tagName);
  if (e.code === 'Space' && !typing) {
    e.preventDefault();
    togglePlay();
  } else if (e.key === 'Escape') {
    closeSettings();
  }
});

if ('mediaSession' in navigator) {
  navigator.mediaSession.setActionHandler('play', togglePlay);
  navigator.mediaSession.setActionHandler('pause', togglePlay);
  navigator.mediaSession.setActionHandler('nexttrack', playNext);
  navigator.mediaSession.setActionHandler('previoustrack', playPrev);
}

// Settings
async function openSettings() {
  const s = await api('/api/settings');
  $('cfgBitrate').value = String(s.bitrate || 48);
  $('cfgCookies').value = s.cookies_file || '';
  $('cfgMaxLen').value = s.max_length_min ?? 10;
  $('cfgTargetLen').value = s.target_length_min ?? 4;
  $('settingsModal').classList.add('open');
}
function closeSettings() { $('settingsModal').classList.remove('open'); }
$('settingsBtn').addEventListener('click', openSettings);
$('cfgCancel').addEventListener('click', closeSettings);
$('settingsModal').addEventListener('click', (e) => { if (e.target.id === 'settingsModal') closeSettings(); });
$('cfgSave').addEventListener('click', async () => {
  await api('/api/settings', {
    bitrate: parseInt($('cfgBitrate').value, 10),
    cookies_file: $('cfgCookies').value.trim(),
    max_length_min: Math.max(0, Number($('cfgMaxLen').value) || 0),
    target_length_min: Math.max(0, Number($('cfgTargetLen').value) || 0),
  });
  if (view === 'radio') ensureRadio(true);
  closeSettings();
  toast('Settings saved');
});

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
$('settingsBtn').innerHTML = icon('settings', 18);
$('searchIcon').innerHTML = icon('search', 18);
$('prevBtn').innerHTML = icon('prev', 18);
$('nextBtn').innerHTML = icon('next', 18);
$('ppBtn').innerHTML = icon('play', 18);
$('volIcon').innerHTML = icon('volume', 16);
let savedVol = 0.9;
try { savedVol = Number(localStorage.getItem('offair.volume') ?? 0.9); } catch (e) {}
setVolume(isFinite(savedVol) ? savedVol : 0.9);

renderMain();
loadLibrary();
sync();
setInterval(sync, 1000);
</script>
</body>
</html>
"""


# -----------------------------------------------------------------------------
# CLI Entry Point
# -----------------------------------------------------------------------------
def run(host: str = "127.0.0.1", port: int = 8080) -> None:
    init_db()
    httpd = ThreadingHTTPServer((host, port), ThreadedAudioRequestHandler)
    print("=" * 60)
    print("OffAir Music  (low-bandwidth mode)")
    print(f"  Open:   http://{host}:{port}")
    print(f"  Vault:  {VAULT_DIR}")
    print("=" * 60)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
        httpd.server_close()


if __name__ == "__main__":
    run()
