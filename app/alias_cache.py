import json
import os
import threading
import time
from typing import Dict, Iterable

from config import ALIAS_CACHE_PATH

_CACHE_VERSION = 1
_CACHE_LOCK = threading.Lock()
_CACHE_DATA: Dict[str, Dict] | None = None


def _load_cache_data() -> Dict[str, Dict]:
    global _CACHE_DATA
    if _CACHE_DATA is not None:
        return _CACHE_DATA

    with _CACHE_LOCK:
        if _CACHE_DATA is not None:
            return _CACHE_DATA

        try:
            with open(ALIAS_CACHE_PATH, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            payload = {"version": _CACHE_VERSION, "entries": {}}
        except json.JSONDecodeError:
            payload = {"version": _CACHE_VERSION, "entries": {}}

        entries = payload.get("entries", {}) if isinstance(payload, dict) else {}
        if not isinstance(entries, dict):
            entries = {}

        _CACHE_DATA = entries
        return _CACHE_DATA


def get_alias_cache_entries(keys: Iterable[str]) -> Dict[str, Dict]:
    entries = _load_cache_data()
    out: Dict[str, Dict] = {}
    for key in keys:
        key = str(key or "").strip()
        if not key or key not in entries:
            continue
        payload = dict(entries[key])
        payload.pop("cached_at", None)
        out[key] = payload
    return out


def save_alias_cache_entries(entries: Dict[str, Dict]) -> None:
    if not entries:
        return

    cache_entries = _load_cache_data()
    with _CACHE_LOCK:
        for key, value in entries.items():
            normalized_key = str(key or "").strip()
            if not normalized_key:
                continue
            payload = dict(value)
            payload.setdefault("cached_at", int(time.time()))
            cache_entries[normalized_key] = payload

        cache_dir = os.path.dirname(ALIAS_CACHE_PATH)
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        temp_path = f"{ALIAS_CACHE_PATH}.{os.getpid()}.tmp"
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "version": _CACHE_VERSION,
                    "entries": cache_entries,
                },
                handle,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        os.replace(temp_path, ALIAS_CACHE_PATH)
