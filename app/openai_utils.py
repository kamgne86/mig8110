import json
import logging
import socket
import urllib.error
import urllib.request
from threading import Lock
from typing import Any, Dict, List, Optional

from config import (
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_EMBEDDING_MODEL,
    OPENAI_LLM_MODEL,
    OPENAI_TIMEOUT_S,
)

logger = logging.getLogger(__name__)

_OPENAI_DISABLED_REASON: Optional[str] = None
_EMBEDDING_CACHE: Dict[tuple[str, str], List[float]] = {}
_EMBEDDING_CACHE_LOCK = Lock()


class OpenAIUnavailableError(RuntimeError):
    """Raised when the OpenAI API cannot be used."""


def is_openai_available() -> bool:
    return bool(OPENAI_API_KEY) and not _OPENAI_DISABLED_REASON


def get_openai_disabled_reason() -> Optional[str]:
    if _OPENAI_DISABLED_REASON:
        return _OPENAI_DISABLED_REASON
    if not OPENAI_API_KEY:
        return "OPENAI_API_KEY missing"
    return None


def _disable_openai(reason: str) -> None:
    global _OPENAI_DISABLED_REASON
    if not _OPENAI_DISABLED_REASON:
        _OPENAI_DISABLED_REASON = reason
        logger.warning("OpenAI integration disabled: %s", reason)


def _post_json(path: str, payload: Dict) -> Dict:
    if not OPENAI_API_KEY:
        raise OpenAIUnavailableError("OPENAI_API_KEY missing")
    if _OPENAI_DISABLED_REASON:
        raise OpenAIUnavailableError(_OPENAI_DISABLED_REASON)

    request = urllib.request.Request(
        f"{OPENAI_BASE_URL}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=OPENAI_TIMEOUT_S) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if exc.code in (401, 403):
            _disable_openai(f"HTTP {exc.code}: {body[:160]}")
        raise OpenAIUnavailableError(f"HTTP {exc.code}: {body[:200]}") from exc
    except urllib.error.URLError as exc:
        raise OpenAIUnavailableError(str(exc.reason)) from exc
    except (TimeoutError, socket.timeout) as exc:
        raise OpenAIUnavailableError("OpenAI request timed out") from exc


def get_text_embeddings(texts: List[str]) -> List[List[float]]:
    """Return embedding vectors for a batch of texts."""
    if not texts:
        return []

    cache_keys = [(OPENAI_EMBEDDING_MODEL, str(text)) for text in texts]
    vectors: List[Optional[List[float]]] = [None] * len(texts)
    missing_texts: List[str] = []
    missing_indexes: List[int] = []

    with _EMBEDDING_CACHE_LOCK:
        for index, cache_key in enumerate(cache_keys):
            cached_vector = _EMBEDDING_CACHE.get(cache_key)
            if cached_vector is not None:
                vectors[index] = list(cached_vector)
                continue
            missing_indexes.append(index)
            missing_texts.append(texts[index])

    if missing_texts:
        payload = {
            "model": OPENAI_EMBEDDING_MODEL,
            "input": missing_texts,
        }
        response = _post_json("/embeddings", payload)
        data = sorted(response.get("data", []), key=lambda item: item.get("index", 0))
        new_vectors = [item.get("embedding", []) for item in data]

        if len(new_vectors) != len(missing_texts):
            raise OpenAIUnavailableError("Embedding count mismatch")

        with _EMBEDDING_CACHE_LOCK:
            for local_index, global_index in enumerate(missing_indexes):
                vector = list(new_vectors[local_index])
                _EMBEDDING_CACHE[cache_keys[global_index]] = vector
                vectors[global_index] = vector

    if any(vector is None for vector in vectors):
        raise OpenAIUnavailableError("Embedding cache resolution failed")

    return [list(vector or []) for vector in vectors]


def _extract_output_text(response: Dict[str, Any]) -> str:
    direct_text = response.get("output_text")
    if isinstance(direct_text, str) and direct_text.strip():
        return direct_text.strip()

    parts: List[str] = []
    for item in response.get("output", []):
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if content.get("type") in {"output_text", "text"} and isinstance(text, str) and text.strip():
                parts.append(text.strip())

    return "\n".join(parts).strip()


def get_structured_json_response(
    *,
    instructions: str,
    user_input: str,
    schema_name: str,
    schema: Dict[str, Any],
    model: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "model": model or OPENAI_LLM_MODEL,
        "input": [
            {"role": "developer", "content": instructions},
            {"role": "user", "content": user_input},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "schema": schema,
                "strict": True,
            }
        },
    }

    selected_model = str(payload["model"])
    if selected_model.startswith("gpt-5"):
        payload["reasoning"] = {"effort": "low"}

    response = _post_json("/responses", payload)
    output_text = _extract_output_text(response)
    if not output_text:
        raise OpenAIUnavailableError("Empty structured output from Responses API")

    try:
        return json.loads(output_text)
    except json.JSONDecodeError as exc:
        raise OpenAIUnavailableError(f"Invalid JSON output: {output_text[:200]}") from exc
