"SS 데이터 LLM 본선 추출 — 정규식 필터를 통과한 모든 대상군에 대해 1패스 LLM으로 핵심 수치/사전 후보를 추출\n\n본 모듈은 ss_extracted_rules_based 다운스트림에서 모든 '대상군'을 LLM으로 직접 처리합니다.\n정규식 단계는 질문/광고/비대상만 컬링하고, LLM이 본선에서 수치 및 제안을 생성합니다."
import json
import re
import os
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Tuple, Iterable, Optional

import dagster as dg
from dagster_openai import OpenAIResource

from .resources import PostgresResource
from .ss_data_sync import _normalize_carrier, CARRIER_ALLOWED

# =====================
# Module constants & Key Sets (top-level for discoverability)
# =====================
LOG_PREFIX = "[SS_EXTRACTED_LLM]"  # unified prefix for logs

# Core numeric keys commonly present in most deals
CORE_KEYS: Tuple[str, ...] = (
    "upfront",
    "plan_high_fee",
    "plan_high_months",
    "plan_after_fee",
    "plan_after_months",
)

# All integer-like fields eligible for merge (includes CORE_KEYS)
ALL_KEYS: Tuple[str, ...] = CORE_KEYS + (
    "mvno_tail_fee",
    "mvno_tail_months",
    "addons_monthly",
    "addons_months",
    "addons_count",
    "support_cash",
    "retention_line_months",
    "retention_plan_months",
    "retention_addons_months",
    "contract_months",
    "device_finance_total",
    "device_finance_months",
    "device_finance_monthly",
    "contract_support_amount",
)

# JSON container keys
JSON_KEYS: Tuple[str, ...] = ("addons_detail",)

# Simple string keys used in merge (non-numeric)
STR_KEYS: Tuple[str, ...] = ("model", "capacity", "carrier", "move_type")

# ===============
# Helper utilities (readability/consistency)
# ===============

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default

# --------- Global helper functions (inserted after _env_float) ---------
def _strip_html(s: str) -> str:
    """Lightweight HTML stripper used for comment bodies."""
    try:
        return re.sub(r"<[^>]+>", " ", s or "").replace("&nbsp;", " ").strip()
    except Exception:
        return (s or "").strip()

# Channel normalization helper
def _norm_channel(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if not s:
        return None
    # common misspellings / synonyms
    if s in {"unkwoun", "unkown", "unknown", "미상", "불명"}:
        return "unknown"
    if s in {"online", "on-line", "웹", "인터넷", "온라인"}:
        return "온라인"
    if s in {"offline", "off-line", "오프", "오프라인", "매장", "방문", "내방", "현장"}:
        return "오프라인"
    # if ambiguous, fall back to unknown
    return "unknown"

def _reason_inc(reasons: Dict[str, int], key: str) -> None:
    reasons[key] = reasons.get(key, 0) + 1

def _reason_push(samples: Dict[str, List[str]], key: str, value: str, cap: int = 5) -> None:
    lst = samples.get(key)
    if lst is not None and len(lst) < cap:
        lst.append(value)

def _pct(nums: List[float], p: float) -> float:
    if not nums:
        return 0.0
    xs = sorted(nums)
    k = max(0, min(len(xs) - 1, int(round((len(xs) - 1) * p))))
    return float(xs[k])

def _hist(nums: List[float], bins: List[float]) -> List[int]:
    counts = [0 for _ in range(len(bins) - 1)]
    for v in nums:
        for i in range(len(bins) - 1):
            if bins[i] <= v < bins[i + 1] or (i == len(bins) - 2 and v == bins[-1]):
                counts[i] += 1
                break
    return counts

def _build_system_suffix(allowed_carriers: List[str], allowed_models: List[str]) -> str:
    carriers_joined = " | ".join(allowed_carriers) if allowed_carriers else ""
    models_joined = " | ".join(allowed_models) if allowed_models else ""
    if not carriers_joined and not models_joined:
        return ""
    parts = ["\n\n[허용 목록]"]
    if carriers_joined:
        parts.append(f"- carrier: 아래 중 하나만 허용 → {carriers_joined}")
    if models_joined:
        parts.append(f"- model: 아래 중 하나만 허용 → {models_joined}")
    parts.append("\n[강화 규칙]\n- 'carrier'와 'model'은 반드시 위 목록에서 정확히 일치하는 문자열로만 기입합니다. 목록에 없거나 불명확하면 null. 별칭/임의 값 금지.")
    return "\n".join(parts)

def _load_allowed_lists(context: dg.AssetExecutionContext, is_postgres: PostgresResource) -> Tuple[List[str], List[str]]:
    allowed_carriers: List[str] = []
    allowed_models: List[str] = []
    try:
        with is_postgres.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM carriers ORDER BY name;")
                allowed_carriers = [row[0] for row in cur.fetchall()]
                cur.execute("SELECT name FROM phone_models ORDER BY name;")
                allowed_models = [row[0] for row in cur.fetchall()]
    except Exception as e:
        context.log.warning(f"Failed to load dictionary for prompt injection: {e}")
    return allowed_carriers, allowed_models

def _build_response_json_schema(allowed_carriers: List[str], allowed_models: List[str]) -> Dict[str, Any]:
    """OpenAI json_schema 응답 포맷에 사용할 스키마를 동적으로 생성."""
    def _str_or_null() -> Dict[str, Any]:
        return {"anyOf": [{"type": "string"}, {"type": "null"}]}

    def _int_or_null() -> Dict[str, Any]:
        return {"anyOf": [{"type": "integer"}, {"type": "null"}]}

    def _bool_or_null() -> Dict[str, Any]:
        return {"anyOf": [{"type": "boolean"}, {"type": "null"}]}

    def _enum_or_null(options: List[str]) -> Dict[str, Any]:
        return {"anyOf": [{"type": "string", "enum": options}, {"type": "null"}]}

    deal_props: Dict[str, Any] = {
        "model": {"type": "string"},
        "capacity": _str_or_null(),
        "carrier": {"type": "string"},
        "move_type": _str_or_null(),
        "upfront": _int_or_null(),
        "support_cash": _int_or_null(),
        "device_finance_total": _int_or_null(),
        "device_finance_months": _int_or_null(),
        "device_finance_monthly": _int_or_null(),
        "plan_high_fee": _int_or_null(),
        "plan_high_months": _int_or_null(),
        "plan_after_fee": _int_or_null(),
        "plan_after_months": _int_or_null(),
        "mvno_tail_fee": _int_or_null(),
        "mvno_tail_months": _int_or_null(),
        "addons_monthly": _int_or_null(),
        "addons_months": _int_or_null(),
        "retention_line_months": _int_or_null(),
        "retention_plan_months": _int_or_null(),
        "retention_addons_months": _int_or_null(),
        "contract_type": _str_or_null(),
        "contract_months": _int_or_null(),
        "contract_extra_support": _bool_or_null(),
        "contract_support_amount": _int_or_null(),
        "addons_detail": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {"type": "string"},
                    "fee": _int_or_null(),
                    "months": _int_or_null(),
                    "note": _str_or_null(),
                },
                "required": ["name"],
            },
        },
        "channel": _enum_or_null(["온라인", "오프라인", "unknown"]),
        "city": _str_or_null(),
        "store": _str_or_null(),
    }

    # 허용 목록이 있으면 enum으로 강제 (없으면 일반 문자열 허용)
    if allowed_carriers:
        deal_props["carrier"] = {"type": "string", "enum": allowed_carriers}
    if allowed_models:
        deal_props["model"] = {"type": "string", "enum": allowed_models}

    schema: Dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "deal": {
                "type": "object",
                "additionalProperties": False,
                "properties": deal_props,
                "required": ["model", "carrier"],
            }
        },
        "required": ["deal"],
    }
    return schema

def _call_llm_and_parse(*, client_res: OpenAIResource, context: dg.AssetExecutionContext, post_id: str, url: str, user_txt: str, system_suffix: str, json_schema: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str, bool, str]:
    """Call LLM and parse JSON. Returns (data, parse_err, fixed, raw_content)."""
    with client_res.get_client(context) as client:
        create_kwargs = dict(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": PROMPT + system_suffix},
                {"role": "user", "content": user_txt},
            ],
            temperature=LLM_TEMPERATURE,
        )
        if LLM_USE_JSON_MODE:
            if LLM_USE_JSON_SCHEMA and json_schema:
                create_kwargs["response_format"] = {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "ss_deal_schema",
                        "schema": json_schema,
                        "strict": True,
                    },
                }
            else:
                create_kwargs["response_format"] = {"type": "json_object"}
        if LLM_EXTRA_BODY:
            create_kwargs["extra_body"] = LLM_EXTRA_BODY
        resp = client.chat.completions.create(**create_kwargs)
        msg = resp.choices[0].message
        content = _message_content_to_text(msg)
        data, parse_err, fixed = _try_parse_json(content)
        return data, parse_err, fixed, content

# =====================
# 상수 & 타입
# =====================
PROMPT = (
    """
목적/역할
- 게시글과 OP(작성자) 댓글에서 휴대폰 거래 정보를 읽고, 주어진 **JSON Schema(엔진 강제)**에 맞춰 `deal` 객체를 **정확히 채웁니다**.
- 당신은 사고형(Thinking) 모델입니다. **모든 추론은 내부적으로만** 진행하고, **출력은 JSON 1개**만 내보냅니다.

입력 포맷
- [TITLE] 제목, [CONTENT] 본문, [META] 메타, [COMMENTS_OP] OP 본인 댓글(필요 시).
- 타인의 댓글은 사용하지 않습니다.

출력 요구사항
- 반드시 **JSON 객체 1개**만 출력합니다: 루트는 `{ "deal": { ... } }`.
- 스키마 이외의 키, 설명문, 코드펜스는 출력하지 않습니다.
- 불확실한 값은 `null`(또는 빈 배열)로 둡니다.

스키마 요약(참조용)
- 필수: `deal.model`(string), `deal.carrier`(string)
- 타입: 정수형은 모두 **integer**, 문자열은 string, 불리언은 boolean, 없으면 null
- 필드 목록 요약
  - model: string | capacity: string|null | carrier: string | move_type: string|null
  - upfront: integer|null | support_cash: integer|null
  - device_finance_total: integer|null | device_finance_months: integer|null | device_finance_monthly: integer|null
  - plan_high_fee: integer|null | plan_high_months: integer|null | plan_after_fee: integer|null | plan_after_months: integer|null
  - mvno_tail_fee: integer|null | mvno_tail_months: integer|null
  - addons_monthly: integer|null | addons_months: integer|null | addons_detail: array of {name, fee|null, months|null, note|null}
  - retention_line_months: integer|null | retention_plan_months: integer|null | retention_addons_months: integer|null
  - contract_type: string|null | contract_months: integer|null | contract_extra_support: boolean|null | contract_support_amount: integer|null
  - channel: '온라인'|'오프라인'|'unknown'|null | city: string|null | store: string|null
- **추가 키 금지**(additionalProperties=false). `model`/`carrier`는 시스템이 제공한 허용 목록과 **정확히 일치**(SYSTEM_SUFFIX 참고). 불명확하면 null.

작업 절차(내부 사고, 출력은 JSON만)
1) 단서 수집: 모델명/용량/통신사, 금액·개월·일수, 채널·도시·상호.
2) 매핑: 단서를 스키마의 적절한 필드에 대응.
3) 정규화/검증: 단위 변환, 중복/이중계산 제거, 비현실적 수치 검토.
4) 최종 JSON 생성: 스키마 키만 포함. 확실치 않은 값은 null.

해석 규칙(핵심)
- **식별자(필수)**: `model`, `carrier`는 정식 명칭으로. 허용 목록에 없거나 불명확하면 null.
- **정수화**: 금액/개월/일수는 모두 정수. 소수는 반올림.
- **unknown 처리**: 확신 없으면 null. placeholder 문자열 금지.
- **중복/이중계산 방지**
  - `upfront` = 내가 즉시 지불한 금액(현완/현/완납 등)
  - `support_cash` = 내가 받은 금액(차비/캐시백/페이백/현금지원 등)
  - 같은 금액을 둘 다에 넣지 않습니다.
- **할부 구분**
  - `device_finance_total`은 `할부/할부원금/원금/단말금액/기기값(할부 맥락)`이 **명시**될 때만.
  - `device_finance_months`/`device_finance_monthly`는 월납/개월 정보가 있을 때만.
- **요금제/부가**
  - `33요금제` → 33000원.
  - `N만 M개월`의 문맥이 부가에 가까우면 `addons_monthly`/`addons_months`.
  - 복수 부가는 `addons_detail` 배열에 `{name, fee, months, note}`로.
- **일수→개월 환산**
  - 30일 ≈ 1개월, 반올림. 예: 95일→3, 183일→6.
  - 대상: plan_high_months, plan_after_months, mvno_tail_months, retention_line_months, retention_plan_months, retention_addons_months, addons_months.
- **개월 인식 방어**
  - 단독 숫자(예: `1`)는 months로 보지 않음. `N개월/월/M+N/일수` 등 명시적 단위 필요.
- **비현실 방어**
  - months가 36을 크게 초과하면 원문 단위 재검토, 애매하면 null.
- **약어/은어**
  - `욕` = `요금제`(예: `115욕 6개월` → plan_high_fee=115000, plan_high_months=6)
- **금액 한국어 단위**
  - `6.6만`→66000, `6만6천`→66000. 단위가 없으면 금액으로 단정하지 않음.
- **채널 값 제한**: channel은 '온라인'/'오프라인'/'unknown' 중 하나로만 작성. 애매하면 'unknown'.

위치/채널 판단
- OP 본인 댓글을 사용할 때만 위치 근거로 인정: (a) 작성자가 OP, (b) 지명 명시, (c) 지명 인근의 거래 동사(내방/방문/개통/계약/구매/매장 등).
- 위 조건 충족 시 `city`=지명, `channel`=`오프라인`. 불충분 시 `city=null`, `channel=unknown`.

예시 A — 입력(축약)
[TITLE] 갤럭시 S24 256 KT 번호이동 조건 102만원
[CONTENT] 33요금제 3개월 후 4.7만, 부가 없음, 선약
[COMMENTS_OP] 서울 가산에서 내방 개통했습니다.
예시 A — 출력(JSON)
{"deal": {"model": "갤럭시 S24", "capacity": "256GB", "carrier": "KT", "move_type": "번호이동", "upfront": 1020000, "support_cash": null, "device_finance_total": null, "device_finance_months": null, "device_finance_monthly": null, "plan_high_fee": 33000, "plan_high_months": 3, "plan_after_fee": 47000, "plan_after_months": 6, "mvno_tail_fee": null, "mvno_tail_months": null, "addons_monthly": 0, "addons_months": null, "retention_line_months": null, "retention_plan_months": null, "retention_addons_months": null, "contract_type": "선택약정", "contract_months": 12, "contract_extra_support": null, "contract_support_amount": null, "addons_detail": [], "channel": "오프라인", "city": "서울", "store": null}}

예시 B — 입력(축약)
[TITLE] 아이폰 15 자급 128 자할 24개월 월 5.8만, 부가 0
[CONTENT] 통신사 미표기, 요금제 9.9 욕 3개월, 이후 알뜰 1.1만 3개월
예시 B — 출력(JSON)
{"deal": {"model": "아이폰 15", "capacity": "128GB", "carrier": null, "move_type": "자급", "upfront": null, "support_cash": null, "device_finance_total": null, "device_finance_months": 24, "device_finance_monthly": 58000, "plan_high_fee": 99000, "plan_high_months": 3, "plan_after_fee": 11000, "plan_after_months": 3, "mvno_tail_fee": null, "mvno_tail_months": null, "addons_monthly": 0, "addons_months": null, "retention_line_months": null, "retention_plan_months": null, "retention_addons_months": null, "contract_type": null, "contract_months": null, "contract_extra_support": null, "contract_support_amount": null, "addons_detail": [], "channel": "온라인", "city": null, "store": null}}
"""
)



# LLM 설정/로깅
# LLM_MODEL = "qwen/qwen3-4b-2507"
LLM_MODEL = "qwen/qwen3-4b-thinking-2507"
# LLM_MODEL = "openai/gpt-oss-20b"
# LLM_MODEL = "gemini-2.5-flash"
LLM_TEMPERATURE = 0
# Gemini(OpenAI 호환)에서는 'format' 필드를 허용하지 않음. (INVALID_ARGUMENT: Unknown name "format")
# JSON 강제 모드는 OpenAI 호환 파라미터 'response_format'을 사용 (가능 시).
LLM_USE_JSON_MODE = os.getenv("SS_LLM_JSON_MODE", "1") == "1"  # vLLM 등 일부 로컬 서버가 미지원이면 0으로
LLM_USE_JSON_SCHEMA = os.getenv("SS_LLM_JSON_SCHEMA", "1") == "1"  # JSON Schema 강제 모드 (OpenAI 'response_format': 'json_schema')
LLM_EXTRA_BODY: Optional[Dict[str, Any]] = None  # 호환성 위해 기본값은 None
LLM_DEBUG_SNIPPET = 800
LLM_LOG_FULL = os.getenv("SS_LLM_LOG_FULL", "1") == "1"
LLM_LOG_DIR = Path(os.getenv("SS_LLM_LOG_DIR", ".debug/llm"))

# JSON fix 정규식 미리 컴파일
_RE_TRIPLE_BACKTICKS = re.compile(r'^```(json)?\s*', re.DOTALL)
_RE_END_BACKTICKS = re.compile(r'```\s*$', re.DOTALL)
_RE_TRAILING_COMMAS = re.compile(r',(\s*[}\]])')

_RE_CTRL_CHARS = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')



# =====================
# 유틸 함수
# =====================
def _needs_llm(d: Dict[str, Any]) -> bool:
    """LLM 게이트: (사전 기반 prefilter 통과) AND (deal-candidate gate). Optional score threshold via env SS_DEAL_GATE_MIN_SCORE.
    또 하나의 사전 차단: advertorial_score가 높고(deafult>0.7) deal_candidate_score가 낮으면(default<0.5) 차단.
    """
    if not d.get("llm_prefilter_passed"):
        return False
    intent = d.get("intent")
    if intent and intent != "deal":
        return False
    deal_ok = bool(d.get("deal_candidate"))
    if not deal_ok:
        return False
    # optional score threshold
    try:
        min_score = float(os.getenv("SS_DEAL_GATE_MIN_SCORE", "0"))
    except Exception:
        min_score = 0.0
    score = d.get("deal_candidate_score")
    if isinstance(score, (int, float)) and score < min_score:
        return False
    # advertorial high & deal score low pre-block
    try:
        adv = float(d.get("advertorial_score") or 0.0)
    except Exception:
        adv = 0.0
    try:
        adv_high = float(os.getenv("SS_ADVERTORIAL_HIGH", "0.7") or 0.7)
    except Exception:
        adv_high = 0.7
    try:
        low_score = float(os.getenv("SS_DEAL_LOW_SCORE", "0.5") or 0.5)
    except Exception:
        low_score = 0.5
    try:
        ds = float(score) if score is not None else 0.0
    except Exception:
        ds = 0.0
    if adv > adv_high and ds < low_score:
        return False
    return True



def _build_user_text(d: Dict[str, Any]) -> str:
    title = (d.get("title") or "").strip()
    content = (d.get("content") or "").strip()
    author = (d.get("author") or "").strip()

    comments_text_op = (d.get("comments_text_op") or "").strip()
    comments_text_full = (d.get("comments_text") or "").strip()

    if not comments_text_op:
        comments_list = d.get("comments") or []
        if comments_list:
            op_parts: List[str] = []
            other_parts: List[str] = []
            for c in comments_list:
                cauthor = str(c.get("author") or "").strip()
                body = _strip_html(c.get("content") or c.get("raw") or c.get("html") or "")
                if body:
                    (op_parts if author and cauthor == author else other_parts).append(body)
                for r in (c.get("replies") or []):
                    rauthor = str(r.get("author") or "").strip()
                    rbody = _strip_html(r.get("content") or r.get("raw") or r.get("html") or "")
                    if rbody:
                        (op_parts if author and rauthor == author else other_parts).append(rbody)
            comments_text_op = "\n".join(op_parts)
            if not comments_text_full:
                comments_text_full = "\n".join(op_parts + other_parts)

    parts: List[str] = []
    if title:
        parts.append("[TITLE]\n" + title)
    if content:
        parts.append("[CONTENT]\n" + content)
    if author:
        parts.append("[META]\npost_author: " + author)
    if comments_text_op:
        parts.append("[COMMENTS_OP]\n" + comments_text_op)
    # Intentionally exclude other users' comments to avoid location leakage.
    return "\n\n".join(parts)


def _message_content_to_text(msg_obj: Any) -> str:
    """OpenAI 호환 SDK의 message.content를 str로 정규화"""
    # OpenAI SDK returns message objects with attribute .content (str or list)
    content = getattr(msg_obj, "content", msg_obj)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        texts: List[str] = []
        for part in content:
            if isinstance(part, dict) and "text" in part:
                texts.append(str(part.get("text") or ""))
            else:
                texts.append(str(part))
        return "\n".join(t for t in texts if t)
    if isinstance(content, dict):
        # 일부 SDK 변형 대응
        return str(content.get("content") or content)
    return str(content)


def _try_parse_json(raw: str) -> Tuple[Dict[str, Any], str, bool]:
    """JSON 파싱 시도. 실패 시 가벼운 정규화 후 재시도.
    Returns: (data, error_message, used_fix)
    """
    try:
        return json.loads(raw), "", False
    except Exception as e1:  # 첫 실패: 보정 로직 적용
        err1 = f"json.loads failed: {e1}"
        try:
            sub = raw.strip()
            # strip triple backticks 및 언어 힌트
            sub = _RE_TRIPLE_BACKTICKS.sub("", sub)
            sub = _RE_END_BACKTICKS.sub("", sub)
            # 바깥쪽 중괄호로 클리핑
            start = sub.find("{")
            end = sub.rfind("}")
            if start != -1 and end != -1 and end > start:
                sub = sub[start : end + 1]
            # 후행 콤마 제거
            sub = _RE_TRAILING_COMMAS.sub(r'\1', sub)
            # 탭/개행/CR 제외 제어문자 스크럽
            sub = _RE_CTRL_CHARS.sub(" ", sub)
            return json.loads(sub, strict=False), err1, True
        except Exception as e2:
            return {}, f"{err1}; fixup failed: {e2}", True


def _safe_to_int(v: Any) -> Optional[int]:
    """정수로 안전 변환. 숫자형만 허용, NaN/None은 None 반환."""
    if isinstance(v, bool):  # bool은 int로 간주하지 않음
        return None
    if isinstance(v, (int,)):
        return int(v)
    if isinstance(v, float):
        if v != v:  # NaN
            return None
        return int(round(v))
    return None



def _merge_deal_patch(base: Dict[str, Any], patch: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """Merge numeric/JSON/string fields from patch into base; return (merged, changed)."""
    merged = dict(base)
    changed = False

    # 1) numeric integer-like keys
    for k in ALL_KEYS:
        if merged.get(k) in (None, 0):
            iv = _safe_to_int(patch.get(k))
            if iv is not None:
                merged[k] = iv
                changed = True

    # 2) JSON container keys
    for k in JSON_KEYS:
        if not merged.get(k):
            v = patch.get(k)
            if v not in (None, "", [], {}):
                merged[k] = v
                changed = True

    # 3) simple string/bool keys (overwrite only if empty)
    SIMPLE_KEYS = ("contract_type", "channel", "city", "store", "contract_extra_support") + STR_KEYS
    for k in SIMPLE_KEYS:
        if merged.get(k) in (None, "") and (k in patch):
            merged[k] = patch.get(k)
            changed = True

    return merged, changed


def _dump_llm_debug(
    *,
    post_id: str,
    url: str,
    system_prompt: str,
    user_text: str,
    raw_content: str,
    parsed_keys: Iterable[str],
    parse_error: str,
    fixed: bool,
    parsed_data: Optional[Dict[str, Any]] = None,
    log_dir: Path = LLM_LOG_DIR,
) -> None:
    """옵션: LLM 호출 결과를 파일로 덤프 (감사/디버깅 용)"""
    if not LLM_LOG_FULL:
        return
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        key = f"{post_id}|{url}|{hashlib.md5(user_text.encode('utf-8')).hexdigest()}"
        fname = log_dir / f"llm_{hashlib.md5(key.encode('utf-8')).hexdigest()}.json"
        dump = {
            "post_id": post_id,
            "url": url,
            "system_prompt": system_prompt,
            "user_text": user_text,
            "raw_content": raw_content,
            "parsed_keys": list(parsed_keys),
            "parse_error": parse_error,
            "fixed": fixed,
            "parsed_data": parsed_data,
        }
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(dump, f, ensure_ascii=False, indent=2)
    except Exception:
        # 파일 로깅 실패는 워크플로우에 영향 주지 않음
        pass


# =====================
# Dagster Asset
# =====================

@dg.asset(
    name="ss_extracted_llm",
    group_name="SS",
    deps=["ss_extracted_rules_based"],
    description="LLM 추출(본선): DB 사전 필터를 통과한 대상군에 대해서만 LLM 1패스 추출",
)
def ss_extracted_llm(
    context,
    ss_extracted_rules_based: List[Dict[str, Any]],
    omen: OpenAIResource,
    is_postgres: PostgresResource,
):
    client_res = omen

    # --- Gate min score & score collectors for observability ---
    gate_min_score = _env_float("SS_DEAL_GATE_MIN_SCORE", 0.0)
    all_scores: List[float] = []
    called_scores: List[float] = []
    skipped_low_scores: List[float] = []  # skipped specifically due to low score gate

    # Gate detail counters (per-reason)
    gate_reasons_all = {
        "no_prefilter": 0,
        "intent_not_deal": 0,
        "deal_candidate_false": 0,
        "score_below_threshold": 0,
        "adv_high_and_low_score": 0,
    }
    gate_reasons_skipped = {
        "no_prefilter": 0,
        "intent_not_deal": 0,
        "deal_candidate_false": 0,
        "score_below_threshold": 0,
        "adv_high_and_low_score": 0,
    }
    # sample post_id collectors (cap at 5 each)
    gate_samples_all = {
        "no_prefilter": [],
        "intent_not_deal": [],
        "deal_candidate_false": [],
        "score_below_threshold": [],
        "adv_high_and_low_score": [],
    }
    gate_samples_skipped = {
        "no_prefilter": [],
        "intent_not_deal": [],
        "deal_candidate_false": [],
        "score_below_threshold": [],
        "adv_high_and_low_score": [],
    }
    # thresholds reused for gate evaluation
    gate_adv_high = _env_float("SS_ADVERTORIAL_HIGH", 0.7)
    gate_low_score = _env_float("SS_DEAL_LOW_SCORE", 0.5)

    # ---- Load DV-official carrier/model names for prompt injection ----
    allowed_carriers, allowed_models = _load_allowed_lists(context, is_postgres)
    SYSTEM_SUFFIX = _build_system_suffix(allowed_carriers, allowed_models)
    JSON_SCHEMA = _build_response_json_schema(allowed_carriers, allowed_models) if LLM_USE_JSON_SCHEMA else None

    out: List[Dict[str, Any]] = []
    llm_called = 0
    llm_effective = 0

    for d in ss_extracted_rules_based:
        # --- Score collection for observability ---
        try:
            s = float(d.get("deal_candidate_score") or 0.0)
        except Exception:
            s = 0.0
        all_scores.append(s)

        # --- Evaluate gate conditions for detailed counting ---
        prefilter_passed = bool(d.get("llm_prefilter_passed"))
        intent = d.get("intent")
        deal_ok = bool(d.get("deal_candidate"))
        try:
            adv_val = float(d.get("advertorial_score") or 0.0)
        except Exception:
            adv_val = 0.0
        no_prefilter = not prefilter_passed
        intent_not_deal = bool(intent and intent != "deal")
        deal_candidate_false = not deal_ok
        score_below_threshold = isinstance(s, (int, float)) and s < gate_min_score
        adv_high_and_low_score = (adv_val > gate_adv_high) and (s < gate_low_score)
        _pid = str(d.get("post_id") or "")
        _reasons = {
            "no_prefilter": no_prefilter,
            "intent_not_deal": intent_not_deal,
            "deal_candidate_false": deal_candidate_false,
            "score_below_threshold": score_below_threshold,
            "adv_high_and_low_score": adv_high_and_low_score,
        }
        for rk, active in _reasons.items():
            if active:
                _reason_inc(gate_reasons_all, rk)
                _reason_push(gate_samples_all, rk, _pid)

        # 최종 LLM 게이트: (carrier+model prefilter) AND (deal-candidate gate & optional score)
        if not _needs_llm(d):
            # Record explicit low-score skip if below threshold; other reasons (intent/advertorial/prefilter) are not counted here
            if s < gate_min_score:
                skipped_low_scores.append(s)
            _reasons = {
                "no_prefilter": no_prefilter,
                "intent_not_deal": intent_not_deal,
                "deal_candidate_false": deal_candidate_false,
                "score_below_threshold": score_below_threshold,
                "adv_high_and_low_score": adv_high_and_low_score,
            }
            for rk, active in _reasons.items():
                if active:
                    _reason_inc(gate_reasons_skipped, rk)
                    _reason_push(gate_samples_skipped, rk, _pid)
            d["llm_invoked"] = False
            out.append(d)
            continue

        # 대상군은 LLM 호출
        user_txt = _build_user_text(d)
        llm_called += 1

        post_id = str(d.get("post_id") or "")
        url = str(d.get("url") or "")

        try:
            data, parse_err, fixed, content = _call_llm_and_parse(
                client_res=client_res,
                context=context,
                post_id=post_id,
                url=url,
                user_txt=user_txt,
                system_suffix=SYSTEM_SUFFIX,
                json_schema=JSON_SCHEMA,
            )

            # Observability: log raw response snippet and parsed keys
            snippet = content[:LLM_DEBUG_SNIPPET]
            context.log.info(f"{LOG_PREFIX} raw_snippet post_id={post_id} :: {snippet}")
            try:
                context.log.info(f"{LOG_PREFIX} parsed_keys post_id={post_id} :: {list(data.keys())}")
            except Exception:
                pass

            if parse_err:
                context.log.warning(
                    (
                        f"{LOG_PREFIX} parse_json_failed post_id={post_id} :: {parse_err}\n"
                        f"raw[:{LLM_DEBUG_SNIPPET}]={content[:LLM_DEBUG_SNIPPET]}"
                    )
                )
            else:
                context.log.debug(
                    f"{LOG_PREFIX} response_ok post_id={post_id} fixed={fixed} keys={list(data.keys())}"
                )

            _dump_llm_debug(
                post_id=post_id,
                url=url,
                system_prompt=PROMPT + SYSTEM_SUFFIX,
                user_text=user_txt,
                raw_content=content,
                parsed_keys=list(data.keys()),
                parse_error=parse_err,
                fixed=fixed,
                parsed_data=data,
            )
        except Exception as e:
            context.log.warning(f"{LOG_PREFIX} extract_failed post_id={post_id} :: {e}")
            d["llm_invoked"] = True
            out.append(d)
            continue

        # 응답 모양 호환 처리
        deal_patch = data.get("deal", data) if isinstance(data, dict) else {}
        if not isinstance(deal_patch, dict):
            context.log.warning(
                f"{LOG_PREFIX} deal_object_type_error post_id={post_id} got={type(deal_patch).__name__} action=replaced_with_empty"
            )
            deal_patch = {}
        # normalize channel to enum {온라인, 오프라인, unknown}
        if isinstance(deal_patch, dict) and "channel" in deal_patch:
            deal_patch["channel"] = _norm_channel(deal_patch.get("channel"))

        merged, changed = _merge_deal_patch(d, deal_patch)
        merged["llm_invoked"] = True
        if changed:
            llm_effective += 1
        called_scores.append(s)

        out.append(merged)

    # === Early quarantine check (optional fast-fail) ===
    would_quarantine_called: List[Dict[str, Any]] = []
    would_quarantine_skipped: List[Dict[str, Any]] = []
    adv_high = _env_float("SS_ADVERTORIAL_HIGH", 0.7)
    low_score = _env_float("SS_DEAL_LOW_SCORE", 0.5)

    for dd in out:
        flags_list = dd.get("flags") or []
        adv = float(dd.get("advertorial_score") or 0.0)
        deal_score = float(dd.get("deal_candidate_score") or 0.0)
        carrier_f = _normalize_carrier(dd.get("carrier"))
        if not carrier_f and (str(dd.get("move_type") or "").startswith("자급")):
            carrier_f = "MVNO"
        reason = None
        if carrier_f not in CARRIER_ALLOWED:
            reason = "invalid_carrier"
        elif "non_deal" in flags_list:
            reason = "non_deal"
        elif adv > adv_high and deal_score < low_score:
            reason = "advertorial_high_low_deal_score"
        elif adv > adv_high:
            reason = "advertorial_high"
        if reason:
            entry = {
                "post_id": dd.get("post_id"),
                "url": dd.get("url"),
                "title": dd.get("title"),
                "reason": reason,
            }
            if bool(dd.get("llm_invoked")):
                would_quarantine_called.append(entry)
            else:
                would_quarantine_skipped.append(entry)

    if would_quarantine_called or would_quarantine_skipped:
        total_q = len(would_quarantine_called) + len(would_quarantine_skipped)
        fastfail = os.getenv("SS_FASTFAIL_ON_QUARANTINE", "0") == "1"
        meta_quar = {
            "quarantined_total": total_q,
            "quarantined_called_count": len(would_quarantine_called),
            "quarantined_skipped_count": len(would_quarantine_skipped),
            "examples_called": would_quarantine_called[:5],
            "examples_skipped": would_quarantine_skipped[:5],
        }
        if fastfail:
            raise dg.Failure(
                description=(
                    f"Quarantined {total_q} rows due to validation (split by LLM invocation). "
                    f"Set SS_FASTFAIL_ON_QUARANTINE=0 to switch back to soft mode."
                ),
                metadata=meta_quar,
            )
        else:
            # Soft mode: log warning and continue, attach metadata
            context.log.warning(
                f"{LOG_PREFIX} quarantine_soft total={total_q} called={len(would_quarantine_called)} skipped={len(would_quarantine_skipped)}"
            )
            context.add_output_metadata(meta_quar)

    # Build simple stats & histograms for deal_candidate_score
    bins = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]

    meta = {
        "total": len(out),
        "llm_called": llm_called,
        "llm_effective": llm_effective,
        "llm_patch_rate_effective": (llm_effective / llm_called) if llm_called else 0.0,
        "gate_reasons_all": gate_reasons_all,
        "gate_reasons_skipped": gate_reasons_skipped,
        "gate_samples_all": gate_samples_all,
        "gate_samples_skipped": gate_samples_skipped,
        "gate_min_score": gate_min_score,
        "deal_candidate_score": {
            "all": {
                "n": len(all_scores),
                "p50": _pct(all_scores, 0.5),
                "p25": _pct(all_scores, 0.25),
                "p75": _pct(all_scores, 0.75),
                "min": min(all_scores) if all_scores else 0.0,
                "max": max(all_scores) if all_scores else 0.0,
                "hist_0_2": _hist(all_scores, bins),
            },
            "called": {
                "n": len(called_scores),
                "p50": _pct(called_scores, 0.5),
                "p25": _pct(called_scores, 0.25),
                "p75": _pct(called_scores, 0.75),
                "min": min(called_scores) if called_scores else 0.0,
                "max": max(called_scores) if called_scores else 0.0,
                "hist_0_2": _hist(called_scores, bins),
            },
            "skipped_low": {
                "n": len(skipped_low_scores),
                "min": min(skipped_low_scores) if skipped_low_scores else 0.0,
                "max": max(skipped_low_scores) if skipped_low_scores else 0.0,
            },
        },
    }
    # Prepare small dashboard samples (max 5)
    try:
        meta["samples"] = [
            {
                "post_id": dd.get("post_id"),
                "url": dd.get("url"),
                "model": dd.get("model"),
                "deal_candidate_score": dd.get("deal_candidate_score"),
                "upfront": dd.get("upfront"),
                "plan_high_fee": dd.get("plan_high_fee"),
                "plan_high_months": dd.get("plan_high_months"),
            }
            for dd in out[:5]
        ]
    except Exception:
        pass
    context.add_output_metadata(meta)
    return out