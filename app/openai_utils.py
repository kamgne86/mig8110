import json
import logging
import urllib.error
import urllib.request
from typing import Dict, List, Optional

from config import (
    OPENAI_ALIAS_MODEL,
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_EMBEDDING_MODEL,
    OPENAI_TIMEOUT_S,
)

logger = logging.getLogger(__name__)

_OPENAI_DISABLED_REASON: Optional[str] = None


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


def _extract_output_text(response: Dict) -> str:
    if isinstance(response.get("output_text"), str) and response["output_text"].strip():
        return response["output_text"]

    texts: List[str] = []
    for item in response.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            text = content.get("text") or content.get("value")
            if isinstance(text, str) and text.strip():
                texts.append(text)

    return "\n".join(texts).strip()


def _extract_json_object(text: str) -> Dict:
    text = text.strip()
    if not text:
        raise OpenAIUnavailableError("Empty model output")

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise OpenAIUnavailableError("Model output is not valid JSON")
        return json.loads(text[start : end + 1])


def normalize_aliases_with_llm(items: List[Dict[str, object]]) -> Dict[str, Dict]:
    """Normalize ambiguous aliases with a small OpenAI model."""
    if not items:
        return {}

    prompt_items = []
    for item in items:
        prompt_items.append(
            {
                "raw": item["raw"],
                "candidates": item.get("candidates", []),
            }
        )

    instructions = (
        "You normalize food ingredient aliases. "
        "Return strict JSON only. "
        "For each input item, choose the best canonical ingredient name. "
        "If the text is packaging noise, quantity text, marketing copy, or not an ingredient, return an empty canonical string and mark is_noise true. "
        "Keep canonical names short and ingredient-like."
    )
    payload = {
        "model": OPENAI_ALIAS_MODEL,
        "instructions": instructions,
        "input": (
            "Normalize these ingredient aliases and return a JSON object with an 'items' array. "
            "Each item must contain: raw, canonical, is_noise, confidence.\n"
            + json.dumps(prompt_items, ensure_ascii=True)
        ),
        "temperature": 0,
        "max_output_tokens": 700,
    }

    response = _post_json("/responses", payload)
    parsed = _extract_json_object(_extract_output_text(response))

    results: Dict[str, Dict] = {}
    for item in parsed.get("items", []):
        raw = str(item.get("raw") or "").strip()
        if not raw:
            continue
        results[raw] = {
            "canonical": str(item.get("canonical") or "").strip(),
            "is_noise": bool(item.get("is_noise")),
            "confidence": float(item.get("confidence") or 0),
            "source": "llm",
        }

    return results


def get_text_embeddings(texts: List[str]) -> List[List[float]]:
    """Return embedding vectors for a batch of texts."""
    if not texts:
        return []

    payload = {
        "model": OPENAI_EMBEDDING_MODEL,
        "input": texts,
    }
    response = _post_json("/embeddings", payload)
    data = sorted(response.get("data", []), key=lambda item: item.get("index", 0))
    vectors = [item.get("embedding", []) for item in data]

    if len(vectors) != len(texts):
        raise OpenAIUnavailableError("Embedding count mismatch")
    return vectors
