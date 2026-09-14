"""Published GenOS prompt as an indexing scope; no user token or stale cache."""
import json
import os
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class PromptScope:
    law_names: frozenset
    ministries: frozenset
    prompt_id: int
    revision_id: int

    def matches(self, raw):
        if not isinstance(raw, dict):
            raise ValueError("법령 JSON은 객체여야 합니다")
        basic = raw.get("basic_info") or {}
        if not isinstance(basic, dict):
            raise ValueError("basic_info는 객체여야 합니다")
        ministry = basic.get("소관부처") or basic.get("소관부처명")
        if isinstance(ministry, dict):
            ministry = ministry.get("content")
        name = raw.get("law_name")
        return ((isinstance(name, str) and name.strip() in self.law_names)
                or (isinstance(ministry, str) and ministry.strip() in self.ministries))


def parse_scope(payload, prompt_id):
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise ValueError("프롬프트 조회 실패: 정상 응답이 아닙니다")
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("prompt_id") != prompt_id or data.get("label") != "production":
        raise ValueError("프롬프트 ID 또는 운영 버전 응답이 일치하지 않습니다")
    revision = data.get("revision_id")
    if type(revision) is not int or revision <= 0:
        raise ValueError("프롬프트 리비전 ID가 올바르지 않습니다")
    body = data.get("prompt")
    if not isinstance(body, str):
        raise ValueError("프롬프트 본문이 없습니다")
    params = json.loads(body)
    if not isinstance(params, dict) or set(params) - {"law_names", "ministries"}:
        raise ValueError("목록은 law_names, ministries만 포함하는 JSON 객체여야 합니다")
    lists = []
    for key in ("law_names", "ministries"):
        items = params.get(key, [])
        if not isinstance(items, list) or any(not isinstance(x, str) or not x.strip() for x in items):
            raise ValueError(f"{key}는 빈 항목 없는 문자열 배열이어야 합니다")
        lists.append(frozenset(x.strip() for x in items))
    if not any(lists):
        raise ValueError("적재 대상 목록이 비어 있어 실행을 중단합니다")
    return PromptScope(*lists, prompt_id, revision)


def load_prompt_scope():
    # No new default for existing deployments: explicitly opt in to prompt 748.
    value = os.getenv("LAW_SCOPE_PROMPT_ID")
    if value is None:
        return None
    if not value.strip().isdigit() or int(value) <= 0:
        raise ValueError("LAW_SCOPE_PROMPT_ID는 양의 정수여야 합니다")
    prompt_id = int(value)
    base = (os.getenv("LLMOPS_ADMIN_API_URL") or "http://llmops-admin-api-service:8080").rstrip("/")
    query = urlencode({"prompt_id": prompt_id, "label": "production"})
    request = Request(f"{base}/prompt/runtime?{query}", headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"운영 프롬프트 조회 HTTP {exc.code}: 적재 중단") from None
    except (URLError, TimeoutError):
        raise RuntimeError("운영 프롬프트 연결 실패: 적재 중단") from None
    return parse_scope(payload, prompt_id)
