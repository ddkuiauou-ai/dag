"""
SS 데이터 파이프라인(Dagster) — Ppomppu 휴대폰 JSON → 정규화 → (규칙 필터) → LLM 본선 추출 → UPSERT → 집계

본 모듈은 변경된 스펙(v2): 정규식 단계는 질문/광고/비대상만 컬링하고, 대상군은 전부 LLM이 관장합니다.
격리 큐는 사용하지 않습니다.
"""

import json
import os
import re
import hashlib
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from psycopg2.extras import execute_values, Json

from .resources import PostgresResource
from .schedules import EAGER
from pathlib import Path
import unicodedata
from functools import lru_cache


# =============================
# Helpers
# =============================

_WS_RE = re.compile(r"\s+")

# === Text normalization (A/B keys) ===
_ALLOW_CH = re.compile(r"[0-9a-zA-Z가-힣\+ ]")  # keep digits/latin/korean/+/space

def _normalize_keys(s: Optional[str]) -> Tuple[str, str]:
    """Return (Key-A, Key-B) where:
    - Key-A: spaces collapsed to single spaces, punctuation removed (except '+'), unicode normalized, casefolded
    - Key-B: Key-A with spaces removed
    """
    if not s:
        return "", ""
    # Unicode normalize: compatibility then composition
    s = unicodedata.normalize("NFKC", s)
    s = unicodedata.normalize("NFC", s)
    s = s.casefold()
    # Keep only allowlist; others -> space
    kept = []
    for ch in s:
        kept.append(ch if _ALLOW_CH.match(ch) else " ")
    s = "".join(kept)
    # collapse spaces
    s = _WS_RE.sub(" ", s).strip()
    key_a = s
    key_b = s.replace(" ", "")
    return key_a, key_b


def _md5_hex(s: str) -> str:
    return hashlib.md5((s or "").encode("utf-8")).hexdigest()


def _text(s: Optional[str]) -> str:
    if not s:
        return ""
    return _WS_RE.sub(" ", s).strip()


def _strip_html(s: Optional[str]) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    return _text(s)


def _detect_channel(text: str) -> str:
    t = text.lower()
    # If any city/province keyword exists, treat as offline (strong signal)
    if any(kw in t for kw in CITY_KEYWORDS):
        return "오프라인"
    if any(k in t for k in ["쿠팡", "링크", "네이버", "온라인", "url", "http"]):
        return "온라인"
    if any(k in t for k in ["성지", "대리점", "방문", "매장", "예약"]):
        return "오프라인"
    return "unknown"


CITY_KEYWORDS = [
    "서울", "경기", "인천", "부산", "대구", "대전", "광주", "울산", "세종",
    "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주",
]

# Carrier normalization constants & function
CARRIER_ALLOWED = {"SKT", "KT", "LGU+", "MVNO"}
def _normalize_carrier(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    t = str(raw).strip().upper()
    # Common Koreans / aliases
    # MVNO / 자급제 / 알뜰
    if any(k in t for k in ["MVNO", "알뜰", "자급"]):
        return "MVNO"
    # LG U+ variants
    if any(k in t for k in ["LGU+", "LG U+", "엘지", "유플", "U+","LGUPLUS", "LG"]):
        return "LGU+"
    # SKT variants
    if any(k in t for k in ["SKT", "SK TELECOM", "에스케이", "SK "]):
        return "SKT"
    # KT variants
    if any(k in t for k in ["KT", "케이티"]):
        return "KT"
    # If already one of allowed (case-insensitive)
    if t in {x.upper() for x in CARRIER_ALLOWED}:
        return t
    return None


def _extract_city(text: str) -> Optional[str]:
    for kw in CITY_KEYWORDS:
        if kw in text:
            return kw
    return None



POSITIVE_TOKENS = [
    "현완", "현금완", "할부", "할부원금", "요금제", "유지비", "부가",
    "공시", "선약", "성지", "시세", "조건", "개통", "택배", "가격",
    "번이", "기변", "자급",
]
NEGATIVE_TOKENS = [
    "될까요", "일까요", "어떨까요", "추천", "질문", "후기", "고민", "고민중", "갈지", "할지", "기다릴지",
    "액정필름", "강화유리", "케이스", "유심", "화이트리스트", "설정", "수리", "AS", "해지방어", "뉴비",
    "문의", "상담", "알려", "정보좀", "액정", "액정파손", "복구", "데이터", "사진", "터치불가", "서비스센터",
    "사망"
]
_PRICE_RE = re.compile(r"(\d{1,3}(?:\.\d)?\s*만|\d{4,7}\s*원)")
_QUESTIONY_RE = re.compile(r"[?]|(겠|까요|할지|갈지|일지|어떨지|있었나요|이었나요|알려주세요|어떤지)")
_DISCUSSION_RE = re.compile(r"(고민|고민중|생각|조언|추천|문의|상담|정보좀|모르겠네요)")

 # === Dictionary cache & regex pattern builder ===
class _DictPatterns:
    __slots__ = ("term_to_name", "terms_sorted", "pattern")
    def __init__(self, term_to_name: Dict[str, str], terms_sorted: List[str], pattern: re.Pattern):
        self.term_to_name = term_to_name
        self.terms_sorted = terms_sorted
        self.pattern = pattern


def _build_patterns(rows: List[Tuple[str, List[str]]]) -> _DictPatterns:
    term_to_name: Dict[str, str] = {}
    for name, aliases in rows:
        nm = str(name or "").strip()
        if not nm:
            continue
        # normalize canonical name too
        _, nm_b = _normalize_keys(nm)
        if nm_b:
            term_to_name[nm_b] = nm
        for alias in (aliases or []):
            a = str(alias or "").strip()
            if not a:
                continue
            _, b = _normalize_keys(a)
            if b:
                term_to_name[b] = nm
    # sort by length desc to prefer specific terms
    terms_sorted = sorted(term_to_name.keys(), key=len, reverse=True)
    # compile a single regex alternation on normalized (Key-B) terms
    # use re.escape to avoid special chars; join with '|'
    if terms_sorted:
        pattern = re.compile("|".join(re.escape(t) for t in terms_sorted))
    else:
        pattern = re.compile(r"$a")  # never matches
    return _DictPatterns(term_to_name, terms_sorted, pattern)

# --- Build patterns from pre-normalized Key-B aliases ---
def _build_patterns_from_keyb(rows: List[Tuple[str, List[str]]]) -> _DictPatterns:
    """Build patterns when aliases are already normalized to Key-B (spaces removed).
    Ensures canonical name's Key-B is included as well.
    """
    term_to_name: Dict[str, str] = {}
    for name, aliases_b in rows:
        nm = str(name or "").strip()
        if not nm:
            continue
        # include canonical name's Key-B
        _, nm_b = _normalize_keys(nm)
        if nm_b:
            term_to_name[nm_b] = nm
        for b in (aliases_b or []):
            b = str(b or "").strip()
            if b:
                term_to_name[b] = nm
    terms_sorted = sorted(term_to_name.keys(), key=len, reverse=True)
    pattern = re.compile("|".join(re.escape(t) for t in terms_sorted)) if terms_sorted else re.compile(r"$a")
    return _DictPatterns(term_to_name, terms_sorted, pattern)

@lru_cache(maxsize=1)
def _load_dictionary_patterns_cached(carriers_key: str = "__v1__") -> Tuple[_DictPatterns, _DictPatterns]:
    """LRU-cached loader. Key param allows invalidation if needed."""
    raise RuntimeError("This function must be called with a DB connection via _load_dictionary_patterns(context, is_postgres)")

def _load_dictionary_patterns(context: dg.AssetExecutionContext, ss_postgres: PostgresResource) -> Tuple[_DictPatterns, _DictPatterns]:
    # Use cached value if available
    try:
        carriers_pat, models_pat = _load_dictionary_patterns_cached()
        return carriers_pat, models_pat
    except Exception:
        pass
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, aliases_keyb FROM carriers")
            carriers_raw = cur.fetchall()
            cur.execute("SELECT name, aliases_keyb FROM phone_models")
            models_raw = cur.fetchall()
    carriers_pat = _build_patterns_from_keyb(carriers_raw)
    models_pat = _build_patterns_from_keyb(models_raw)
    # seed cache
    try:
        _load_dictionary_patterns_cached.cache_clear()  # type: ignore
        _load_dictionary_patterns_cached()  # warm with default key
    except Exception:
        pass
    # monkey-patch the cached function to return our freshly built objects
    def _cached_impl(carriers_key: str = "__v1__"):
        return carriers_pat, models_pat
    globals()["_load_dictionary_patterns_cached"] = lru_cache(maxsize=1)(_cached_impl)  # type: ignore
    return carriers_pat, models_pat

def _detect_with_patterns(text: str, pat: _DictPatterns) -> Tuple[Optional[str], List[str]]:
    """Detect first match and list all unique matches based on normalized Key-B."""
    if not text:
        return None, []
    _, key_b = _normalize_keys(text)
    if not key_b:
        return None, []
    hits = set()
    for m in pat.pattern.finditer(key_b):
        term = m.group(0)
        name = pat.term_to_name.get(term)
        if name:
            hits.add(name)
    first = None
    if hits:
        # preserve priority by terms_sorted order mapped to names
        for t in pat.terms_sorted:
            n = pat.term_to_name[t]
            if n in hits:
                first = n
                break
    return first, sorted(hits)

# 특정 모델이 캐리어를 암시하는 특수 규칙
SPECIAL_MODEL_TO_CARRIER: Dict[str, str] = {
    "갤럭시 퀀텀 6": "SKT",  # SKT 전용 공급 모델
}


def _detect_deal_candidate(text: str) -> tuple[bool, Optional[str], float]:
    t = text or ""
    # price / token signals
    has_price = bool(_PRICE_RE.search(t))
    pos_hits = sum(1 for tok in POSITIVE_TOKENS if tok in t)
    neg_hits = sum(1 for tok in NEGATIVE_TOKENS if tok in t)

    # questiony / discussion cues
    is_questiony = bool(_QUESTIONY_RE.search(t)) or bool(_DISCUSSION_RE.search(t))

    # If it's clearly a question/discussion and there is no concrete price anchor, treat as non-deal early.
    if is_questiony and not has_price:
        score = max(0.0, min(1.0, 0.1 + min(pos_hits, 3) * 0.05 - min(neg_hits, 3) * 0.2))
        return False, "non_deal_question", score

    # naive scoring (kept but slightly more conservative without price)
    score = 0.0
    if has_price:
        score += 0.5
    # cap positive at 3 hits to avoid runaway; weight lowered when no price
    score += (0.15 if has_price else 0.08) * min(pos_hits, 3)
    score -= 0.2 * min(neg_hits, 3)

    # clamp 0..1
    score = max(0.0, min(1.0, score))

    if has_price or pos_hits > 0:
        return True, None, score
    if neg_hits > 0 or is_questiony:
        return False, "non_deal_unclear", score
    return False, "non_deal_unclear", score


def _detect_move_type(text: str) -> Optional[str]:
    if "번이" in text:
        return "번호이동"
    if "기변" in text or "기기변" in text:
        return "기기변경"
    if "자급" in text:
        return "자급"
    return None


def _detect_contract(text: str) -> Optional[str]:
    if "공시" in text:
        return "공시지원"
    if "선약" in text or "선택약정" in text:
        return "선택약정"
    if "무약정" in text:
        return "무약정"
    return None


def _detect_payment(text: str) -> Optional[str]:
    if "현완" in text or "현금완" in text:
        return "현금완납"
    if "할부" in text:
        return "할부"
    return None


# Intent classifier
def _classify_intent(text: str) -> str:
    """Cheap intent classifier: 'deal' | 'question' | 'advertorial' | 'discussion' | 'unclear'"""
    txt = text or ""
    adv = _advertorial_score(txt)
    has_price = bool(_PRICE_RE.search(txt))
    questiony = bool(_QUESTIONY_RE.search(txt)) or bool(_DISCUSSION_RE.search(txt))
    pos_hits = sum(1 for tok in POSITIVE_TOKENS if tok in txt)

    if adv >= float(os.getenv("SS_ADVERTORIAL_HIGH", "0.7") or 0.7):
        return "advertorial"
    if questiony and not has_price:
        return "question"
    if has_price or pos_hits >= 2:
        return "deal"
    return "unclear"


def _split_scenarios(text: str) -> List[str]:
    """Split multi-scenario posts into segments if markers like '두번째', '둘째', '2안' exist.
    Returns at least one segment (original text if no split markers).
    """
    if not text:
        return [""]
    # Common markers indicating second scenario
    second_markers = [r"^\s*두번째", r"^\s*둘째", r"^\s*2\.?\s*안", r"^\s*2\s*번째"]
    pattern = re.compile("|".join(second_markers), flags=re.I | re.M)
    m = pattern.search(text)
    if not m:
        return [text]
    cut = m.start()
    first = text[:cut].strip()
    second = text[cut:].strip()
    out = [s for s in [first, second] if s]
    return out or [text]


def _tco_total(row: Dict[str, Any]) -> int:
    v = 0
    def add(x, m):
        nonlocal v
        if x is not None and m is not None:
            v += int(x) * int(m)
    v += int(row.get("upfront") or 0)
    add(row.get("plan_high_fee"), row.get("plan_high_months"))
    add(row.get("plan_after_fee"), row.get("plan_after_months"))
    add(row.get("mvno_tail_fee"), row.get("mvno_tail_months"))
    add(row.get("addons_monthly"), row.get("addons_months"))
    v += int(row.get("device_finance_total") or 0)
    return v


def _advertorial_score(text: str) -> float:
    t = text
    score = 0.0
    if any(k in t for k in ["제휴카드", "카드", "상담", "예약", "가입"]):
        score += 0.2
    if any(k in t for k in ["네이버", "링크", "클릭", "카페 가입"]):
        score += 0.3
    if any(k in t for k in ["문의", "톡", "연락"]):
        score += 0.2
    return max(0.0, min(1.0, score))


@dataclass
class DealExtract:
    post_id: Optional[str]
    url: Optional[str]
    model: str
    capacity: Optional[str]
    carrier: str
    move_type: Optional[str]
    contract: Optional[str]
    payment: Optional[str]
    channel: Optional[str]
    city: Optional[str]
    upfront: int
    plan_high_fee: Optional[int] = None
    plan_high_months: Optional[int] = None
    plan_after_fee: Optional[int] = None
    plan_after_months: Optional[int] = None
    mvno_tail_fee: Optional[int] = None
    mvno_tail_months: Optional[int] = None
    addons_monthly: Optional[int] = None
    addons_months: Optional[int] = None
    addons_count: Optional[int] = None
    advertorial_score: Optional[float] = None
    flags: Optional[List[str]] = None


_CAPACITY_RE_UNITS = re.compile(r"\b(64|128|256|512|1024)\s*(GB|G|기가|TB|테라)\b", re.IGNORECASE)
_CAPACITY_RE_BARE = re.compile(r"\b(64|128|256|512|1024)(?!\d)")

def _extract_capacity(text: str) -> Optional[str]:
    """Extract storage capacity like 128/256/512GB or 1TB from free text."""
    if not text:
        return None
    # 1) With units
    m = _CAPACITY_RE_UNITS.search(text)
    if m:
        num = int(m.group(1))
        unit = m.group(2).upper()
        if unit in {"TB", "테라"}:
            return "1TB" if num == 1024 else f"{num}TB"
        return "1TB" if num == 1024 else f"{num}GB"
    # 2) Bare common capacities
    m2 = _CAPACITY_RE_BARE.search(text)
    if m2:
        num = int(m2.group(1))
        return "1TB" if num == 1024 else f"{num}GB"
    return None



# =============================
# SS data directory & partitions
# =============================

SS_DATA_DIR = Path("/Users/craigchoi/silla/dag/ssdata")
SS_FILES = dg.DynamicPartitionsDefinition(name="ss_crawl_files")

@dg.observable_source_asset(
    name="ss_crawl_sources",
    group_name="SS",
    partitions_def=SS_FILES,
    automation_condition=EAGER,
    description="/Users/craigchoi/silla/dag/ssdata 폴더에 떨어지는 *.json 파일을 관찰합니다. 파일명(확장자 제외)을 파티션 키로 등록하고, 파일 해시를 데이터 버전으로 기록합니다."
)
def ss_crawl_sources(context: dg.OpExecutionContext):
    # Ensure directory exists
    try:
        SS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    files = sorted([p for p in SS_DATA_DIR.glob("*.json") if p.is_file()])
    partition_keys = [p.stem for p in files]

    if partition_keys:
        # Register (idempotently) dynamic partitions for discovered files
        context.instance.add_dynamic_partitions(SS_FILES.name, partition_keys)

    # Compute data versions per partition using file hash (mtime fallback)
    versions: dict[str, dg.DataVersion] = {}
    for p in files:
        try:
            h = hashlib.md5(p.read_bytes()).hexdigest()
        except Exception:
            h = f"mtime:{int(p.stat().st_mtime)}"
        versions[p.stem] = dg.DataVersion(h)

    return dg.DataVersionsByPartition(versions)

# =============================
# Assets
# =============================


@dg.asset(
    name="ss_raw_posts",
    group_name="SS",
    kinds={"source"},
    automation_condition=EAGER,
    partitions_def=SS_FILES,
    deps=[ss_crawl_sources],
    description="SSDATA 폴더의 각 JSON 파일(크롤러별 1파일)을 파티션 단위로 읽어 posts 리스트를 반환합니다."
)
def ss_raw_posts(context: dg.AssetExecutionContext):
    # Partition key == filename stem
    part = context.partition_key
    if not part:
        raise dg.Failure(description="파티션 키가 없습니다. 먼저 ss_crawl_sources 관찰을 실행해 파일 파티션을 등록하세요.")

    # Resolve file path (prefer .json)
    candidate = SS_DATA_DIR / f"{part}.json"
    if not candidate.exists():
        raise dg.Failure(description=f"파티션 '{part}'에 해당하는 파일을 찾을 수 없습니다: {candidate}")

    try:
        with open(candidate, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise dg.Failure(description=f"JSON 읽기 실패: {e}")

    # Support either [{meta,posts}] or {meta,posts}
    block = data[0] if isinstance(data, list) else data
    posts = block.get("posts", []) or []
    meta = block.get("meta", {})

    # basic duplicate diagnostics by post_id
    post_ids = [str(p.get("post_id") or "") for p in posts]
    unique_ids = set([pid for pid in post_ids if pid])
    dup_count = max(0, len(post_ids) - len(unique_ids))

    context.add_output_metadata({
        "site": meta.get("site"),
        "board": meta.get("board"),
        "total_posts": len(posts),
        "unique_post_ids": len(unique_ids),
        "duplicate_post_ids_in_file": dup_count,
        "partition": part,
        "file_path": str(candidate),
    })
    return posts


@dg.asset(
    name="ss_bootstrap_schema",
    group_name="SS",
    automation_condition=EAGER,
    description="필요한 테이블/인덱스/뷰 생성 (idempotent)"
)
def ss_bootstrap_schema(context: dg.AssetExecutionContext, ss_postgres: PostgresResource):
    ddl = [
        # === Drop existing objects for clean slate ===
        "DROP VIEW IF EXISTS api_reports_daily_latest_json CASCADE;",
        "DROP VIEW IF EXISTS api_reports_daily_json CASCADE;",
        "DROP VIEW IF EXISTS api_deals_json CASCADE;",
        "DROP TABLE IF EXISTS aggregates_daily CASCADE;",
        "DROP TABLE IF EXISTS deals CASCADE;",
        
        # === deals table ===
        """
        CREATE TABLE IF NOT EXISTS deals (
            id BIGSERIAL PRIMARY KEY,
            deal_hash VARCHAR(32) NOT NULL,
            post_id TEXT,
            url TEXT,
            source_posted_at DATE,
            model TEXT NOT NULL,
            capacity TEXT,
            carrier TEXT NOT NULL,
            move_type TEXT,
            contract TEXT,
            payment TEXT,
            channel TEXT,
            city TEXT,
            upfront INTEGER NOT NULL DEFAULT 0,
            plan_high_fee INTEGER,
            plan_high_months INTEGER,
            plan_after_fee INTEGER,
            plan_after_months INTEGER,
            mvno_tail_fee INTEGER,
            mvno_tail_months INTEGER,
            addons_monthly INTEGER,
            addons_months INTEGER,
            addons_count INTEGER,
            device_finance_total INTEGER,
            device_finance_months INTEGER,
            device_finance_monthly INTEGER,
            support_cash INTEGER,
            retention_line_months INTEGER,
            retention_plan_months INTEGER,
            retention_addons_months INTEGER,
            contract_type TEXT,
            contract_months INTEGER,
            contract_extra_support BOOLEAN,
            contract_support_amount INTEGER,
            addons_detail JSONB,
            store TEXT,
            advertorial_score DOUBLE PRECISION,
            flags JSONB NOT NULL DEFAULT '[]'::jsonb,
            badges JSONB NOT NULL DEFAULT '[]'::jsonb,
            parsed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            tco_total INTEGER,
            tco_net INTEGER
            , CONSTRAINT chk_deals_carrier CHECK (carrier IN ('SKT','KT','LGU+','MVNO'))
            , CONSTRAINT chk_deals_channel CHECK (channel IS NULL OR channel IN ('온라인','오프라인','unknown'))
        );
        """,
        # === deals indexes ===
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_deals_deal_hash ON deals(deal_hash);",
        "CREATE INDEX IF NOT EXISTS ix_deals_model_tco_parsed ON deals(model, tco_total, parsed_at);",
        "CREATE INDEX IF NOT EXISTS ix_deals_city ON deals(city);",
        "CREATE INDEX IF NOT EXISTS ix_deals_carrier ON deals(carrier);",
        "CREATE INDEX IF NOT EXISTS ix_deals_model_capacity_parsed ON deals(model, capacity, parsed_at);",
        "CREATE INDEX IF NOT EXISTS ix_deals_model_capacity_tco_parsed ON deals(model, capacity, tco_total, parsed_at);",
        "CREATE INDEX IF NOT EXISTS ix_deals_parsed_at ON deals(parsed_at);",
        "CREATE INDEX IF NOT EXISTS ix_deals_source_posted_at ON deals(source_posted_at);",
        
        # === aggregates_daily table ===
        """
        CREATE TABLE IF NOT EXISTS aggregates_daily (
            model TEXT NOT NULL,
            capacity TEXT,
            ts DATE NOT NULL,
            min INTEGER,
            p25 INTEGER,
            median INTEGER,
            p75 INTEGER,
            max INTEGER,
            avg INTEGER,
            n INTEGER NOT NULL,
            PRIMARY KEY(model, capacity, ts)
        );
        """,
        # === API views ===
        """
        CREATE OR REPLACE VIEW api_deals_json AS
        SELECT
          jsonb_build_object(
            'id', d.id,
            'post_id', d.post_id,
            'url', d.url,
            'source_posted_at', d.source_posted_at,
            'model', d.model,
            'capacity', d.capacity,
            'carrier', d.carrier,
            'move_type', d.move_type,
            'contract', d.contract,
            'contract_type', d.contract_type,
            'contract_months', d.contract_months,
            'contract_extra_support', d.contract_extra_support,
            'payment', d.payment,
            'channel', d.channel,
            'city', d.city,
            'upfront', COALESCE(d.upfront,0),
            'plan_high_fee', d.plan_high_fee,
            'plan_high_months', d.plan_high_months,
            'plan_after_fee', d.plan_after_fee,
            'plan_after_months', d.plan_after_months,
            'mvno_tail_fee', d.mvno_tail_fee,
            'mvno_tail_months', d.mvno_tail_months,
            'addons_monthly', d.addons_monthly,
            'addons_months', d.addons_months,
            'addons_detail', COALESCE(d.addons_detail, '[]'::jsonb),
            'addons_count', d.addons_count,
            'device_finance_total', d.device_finance_total,
            'device_finance_months', d.device_finance_months,
            'device_finance_monthly', d.device_finance_monthly,
            'support_cash', d.support_cash,
            'store', d.store,
            'advertorial_score', d.advertorial_score,
            'flags', COALESCE(d.flags, '[]'::jsonb),
            'badges', COALESCE(d.badges, '[]'::jsonb),
            'parsed_at', d.parsed_at,
            'tco_total', d.tco_total,
            'tco_monthly_24m', CASE WHEN d.tco_total IS NULL THEN NULL ELSE (d.tco_total + 23) / 24 END,
            'tco_net', d.tco_net,
            'tco_net_monthly_24m', CASE WHEN d.tco_net IS NULL THEN NULL ELSE (d.tco_net + 23) / 24 END,
            'retention_line_months', d.retention_line_months,
            'retention_plan_months', d.retention_plan_months,
            'retention_addons_months', d.retention_addons_months,
            'contract_support_amount', d.contract_support_amount
          ) AS deal
        FROM deals d;
        """,
     """
        CREATE OR REPLACE VIEW api_reports_daily_json AS
        SELECT jsonb_build_object(
            'model', a.model,
            'capacity', a.capacity,
            'ts', a.ts,
            'min', a.min,
            'p25', a.p25,
            'median', a.median,
            'p75', a.p75,
            'max', a.max,
            'avg', a.avg,
            'n', a.n
        ) AS report
        FROM aggregates_daily a;
        """,
        """
        CREATE OR REPLACE VIEW api_reports_daily_latest_json AS
        WITH latest AS (
          SELECT model, capacity, MAX(ts) AS ts
            FROM aggregates_daily
           GROUP BY model, capacity
        )
        SELECT jsonb_build_object(
            'model', a.model,
            'capacity', a.capacity,
            'ts', a.ts,
            'min', a.min,
            'p25', a.p25,
            'median', a.median,
            'p75', a.p75,
            'max', a.max,
            'avg', a.avg,
            'n', a.n
        ) AS report
        FROM aggregates_daily a
        JOIN latest l
          ON a.model = l.model AND a.capacity = l.capacity AND a.ts = l.ts;
        """,
    ]
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for stmt in ddl:
                cur.execute(stmt)
            conn.commit()
    context.add_output_metadata({"status": "ok"})
    return True

def _extract_source_posted_at(p: dict) -> Optional[str]:
    """Extract post date from source JSON strictly.

    We only trust the post-level ISO8601 `timestamp` field coming from the crawler,
    interpret it as UTC ("Z"), and convert to Asia/Seoul calendar date (YYYY-MM-DD).
    No other keys or fallbacks are considered here.
    """
    from datetime import datetime
    try:
        raw = p.get("timestamp")
        if raw is None or str(raw).strip() == "":
            return None
        s = str(raw).strip()
        # Normalize "Z" to "+00:00" so datetime.fromisoformat can parse it
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Parse as aware datetime
        dt = datetime.fromisoformat(s)
        # Convert to KST and return date part
        try:
            from zoneinfo import ZoneInfo  # Python 3.9+
        except Exception:
            # If zoneinfo isn't available for some reason, treat as UTC date
            return dt.date().isoformat()
        kst = dt.astimezone(ZoneInfo("Asia/Seoul"))
        return kst.date().isoformat()
    except Exception:
        return None
       

@dg.asset(
    name="ss_flush_all",
    group_name="SS",
    automation_condition=EAGER,
    description="DANGER: SS 관련 테이블을 모두 비웁니다(TRUNCATE ... RESTART IDENTITY). SS_DANGEROUS_FLUSH=1 환경변수 필요."
)
def ss_flush_all(context: dg.AssetExecutionContext, ss_postgres: PostgresResource):
    """Dangerous flush for SS project tables.

    Behavior
    - Requires env SS_DANGEROUS_FLUSH=1. Otherwise raises Failure to prevent accidents.
    - Truncates data tables and restarts identity sequences.
    - Views are left untouched.
    - Also flushes dictionary tables managed by SS Bootstrap Dictionary Schema: carriers, phone_models.
    """
    if os.getenv("SS_DANGEROUS_FLUSH", "1") != "1":
        raise dg.Failure(
            description=(
                "Refusing to flush tables. Set SS_DANGEROUS_FLUSH=1 to confirm destructive operation."
            )
        )

    tables = [
        "deals",
        "aggregates_daily",
        "carriers",
        "phone_models",
    ]

    sql = ";\n".join([f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE" for t in tables]) + ";"

    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
    context.add_output_metadata({
        "flushed_tables": tables,
    })
    return {"status": "ok", "flushed_tables": tables}

@dg.asset(
    name="ss_normalized_posts",
    group_name="SS",
    automation_condition=EAGER,
    deps=[ss_raw_posts],
    description="텍스트 정규화, 채널/도시 등 1차 신호 도출"
)
def ss_normalized_posts(context: dg.AssetExecutionContext, ss_raw_posts):
    # ss_raw_posts may be a list (single partition) or a dict of {partition_key: list} when fan-in occurs.
    raw_list: List[dict] = []
    if isinstance(ss_raw_posts, dict):
        for _pk, items in (ss_raw_posts or {}).items():
            if isinstance(items, list):
                raw_list.extend(items)
    elif isinstance(ss_raw_posts, list):
        raw_list = ss_raw_posts
    else:
        raw_list = []

    out: List[dict] = []
    missing_dates = 0
    zero_comments = 0
    for p in raw_list:
        post_author = str(p.get("author") or "").strip()
        title = _text(p.get("title"))
        content = _text(p.get("content"))
        comments = p.get("comments") or []
        # Separate OP-authored comments vs others (include first-level replies)
        op_comments_parts: List[str] = []
        other_comments_parts: List[str] = []
        for c in comments:
            cauthor = str(c.get("author") or "").strip()
            body = _strip_html(c.get("content"))
            if body:
                if post_author and cauthor == post_author:
                    op_comments_parts.append(body)
                else:
                    other_comments_parts.append(body)
            for r in (c.get("replies") or []):
                rauthor = str(r.get("author") or "").strip()
                rbody = _strip_html(r.get("content"))
                if rbody:
                    if post_author and rauthor == post_author:
                        op_comments_parts.append(rbody)
                    else:
                        other_comments_parts.append(rbody)
        comments_text_op = " \n ".join([t for t in op_comments_parts if t])
        comments_text_others = " \n ".join([t for t in other_comments_parts if t])
        comments_text = " \n ".join([t for t in [comments_text_op, comments_text_others] if t])
        comments_count = len(comments)
        if comments_count == 0:
            zero_comments += 1
        # Prioritize body (title+content) for city/channel; fallback to OP-only comments
        body_text = "\n".join([t for t in [title, content] if t])
        op_text = comments_text_op
        city_body = _extract_city(body_text)
        city_op = _extract_city(op_text)
        channel_body = _detect_channel(body_text)
        channel_op = "오프라인" if city_op else None
        resolved_city = city_body or city_op
        resolved_channel = channel_body or channel_op or "unknown"
        # Downstream text uses body + OP comments only (no others to avoid leakage)
        text = "\n".join([t for t in [title, content, comments_text_op] if t])
        source_posted_at = _extract_source_posted_at(p)
        if source_posted_at is None:
            missing_dates += 1
        base = {
            "post_id": p.get("post_id"),
            "url": p.get("url"),
            "author": post_author,
            "title": title,
            "content": content,
            "comments_text": comments_text,         # full (OP + others)
            "comments_text_op": comments_text_op,   # OP-only
            "text": text,                           # body + OP-only
            "channel": resolved_channel,
            "city": resolved_city,
            "move_type": _detect_move_type(body_text),
            "contract": _detect_contract(body_text),
            "payment": _detect_payment(body_text),
            "source_posted_at": source_posted_at,
            "comments_count": comments_count,
        }
        intent = _classify_intent(text)
        # deal intent
        deal_cand, nondeal_reason, deal_score = _detect_deal_candidate(text)
        # If our coarse intent classifier says non-deal/question/advertorial, force deal_cand to False.
        if intent in ("question", "advertorial", "discussion"):
            deal_cand = False
            if nondeal_reason is None:
                nondeal_reason = "intent_" + intent
        base["deal_candidate"] = deal_cand
        base["deal_candidate_score"] = deal_score
        base["intent"] = intent
        if not deal_cand:
            base["nondeal_reason"] = nondeal_reason
        out.append(base)

    # Optional dedupe by (post_id, url) to suppress upstream overlaps across partitions
    do_dedupe = os.getenv("SS_DEDUPE_POSTS", "1") == "1"
    deduped = 0
    if do_dedupe and out:
        seen_keys = set()
        filtered: List[dict] = []
        for r in out:
            key = (r.get("post_id"), r.get("url"))
            if key in seen_keys:
                deduped += 1
                continue
            seen_keys.add(key)
            filtered.append(r)
        out = filtered
    context.add_output_metadata({
        "n": len(out),
        "deduped": deduped,
        "deal_candidates": sum(1 for r in out if r.get("deal_candidate")),
        "non_deal": sum(1 for r in out if r.get("deal_candidate") is False),
        "missing_source_posted_at": missing_dates,
        "zero_comments": zero_comments,
    })
    return out


@dg.asset(
    name="ss_extracted_rules_based",
    group_name="SS",
    automation_condition=EAGER,
    deps=[ss_normalized_posts, "ss_seed_dictionary_data"],
    description="DB 사전 기반 필터링: LLM 대상 선정 및 메타 신호 생성"
)
def ss_extracted_rules_based(context: dg.AssetExecutionContext, ss_normalized_posts: List[dict], is_postgres: PostgresResource):
    carriers_pat, models_pat = _load_dictionary_patterns(context, is_postgres)

    results: List[dict] = []
    skipped_no_comments = 0
    for row in ss_normalized_posts:
        # Skip sending to LLM when a post has zero comments
        try:
            if int(row.get("comments_count") or 0) == 0:
                # Mark as skipped for observability and continue
                row["llm_prefilter_passed"] = False
                row_flags = row.get("flags") or []
                row_flags.append("no_comments_skip_llm")
                row["flags"] = row_flags
                context.log.info(
                    f"Skip LLM (post_id: {row.get('post_id')}) due to zero comments"
                )
                skipped_no_comments += 1
                continue
        except Exception:
            pass
        full_text = row["text"]

        # LLM Prefilter Logic
        detected_carrier_name, carrier_all = _detect_with_patterns(full_text, carriers_pat)
        detected_model_name, model_all = _detect_with_patterns(full_text, models_pat)
        has_carrier = detected_carrier_name is not None
        has_model = detected_model_name is not None

        # 특수 케이스: 모델이 특정 캐리어를 암시하면 캐리어 자동 설정
        auto_carrier: Optional[str] = None
        if not has_carrier and has_model and detected_model_name in SPECIAL_MODEL_TO_CARRIER:
            auto_carrier = SPECIAL_MODEL_TO_CARRIER[detected_model_name]
            has_carrier = True  # 프리필터에서 캐리어 존재로 간주

        row["llm_prefilter_passed"] = has_carrier and has_model
        row["carrier_auto_assigned"] = auto_carrier
        row["dict_matches"] = {
            "carriers": carrier_all,
            "models": model_all,
        }

        if not row["llm_prefilter_passed"]:
            fail_reasons = []
            if not has_carrier:
                fail_reasons.append("no carrier found")
            if not has_model:
                fail_reasons.append("no model found")
            reason = ", ".join(fail_reasons)

            title = row.get("title", "")
            content_snippet = (row.get("content", "") or "")[:200]

            context.log.info(
                f"Prefilter FAIL (post_id: {row.get('post_id')}): {reason}\n"
                f"Title: {title}\n"
                f"Content: {content_snippet}..."
            )

        # record gate info for observability
        row["gate_has_carrier"] = has_carrier
        row["gate_has_model"] = has_model
        row["gate_deal_score"] = row.get("deal_candidate_score")

        segments = _split_scenarios(full_text)

        for seg_text in segments:
            text = seg_text

            chosen_model, seg_models_all = _detect_with_patterns(seg_text, models_pat)
            if not chosen_model:
                chosen_model = "미상"
            seg_carrier, seg_carriers_all = _detect_with_patterns(seg_text, carriers_pat)
            # normalize carrier aliases (e.g., 자급제/알뜰 → MVNO)
            seg_carrier = _normalize_carrier(seg_carrier) or _normalize_carrier(row.get("carrier"))
            # if still missing but move_type is 자급, default MVNO
            if not seg_carrier and (row.get("move_type") or "").startswith("자급"):
                seg_carrier = "MVNO"
            # final fallback to None (let gate decide quarantine)
            # 특수 케이스 적용: 모델이 캐리어를 암시하면 덮어쓰기
            if chosen_model in SPECIAL_MODEL_TO_CARRIER:
                seg_carrier = SPECIAL_MODEL_TO_CARRIER[chosen_model]
            capacity = _extract_capacity(text)

            deal = DealExtract(
                post_id=row.get("post_id"),
                url=row.get("url"),
                model=chosen_model,
                capacity=capacity,
                carrier=(seg_carrier or "MVNO") if (row.get("move_type") or "").startswith("자급") else seg_carrier,
                move_type=row.get("move_type"),
                contract=row.get("contract"),
                payment=row.get("payment"),
                channel=row.get("channel"),
                city=row.get("city"),
                upfront=0,
                advertorial_score=_advertorial_score(text),
                flags=[],
            )

            if row.get("carrier_auto_assigned"):
                deal.flags.append(f"carrier_auto_{row['carrier_auto_assigned']}")

            if row.get("intent") and row.get("intent") != "deal":
                deal.flags.append("non_deal")
                deal.flags.append("intent_" + str(row.get("intent")))

            if row.get("deal_candidate") is False:
                deal.flags.append("non_deal")

            if float(deal.advertorial_score or 0.0) > 0.7:
                deal.flags.append("advertorial_high")

            # combined preblock observability: high advertorial & low deal score
            try:
                adv_high = float(os.getenv("SS_ADVERTORIAL_HIGH", "0.7") or 0.7)
            except Exception:
                adv_high = 0.7
            try:
                low_score = float(os.getenv("SS_DEAL_LOW_SCORE", "0.5") or 0.5)
            except Exception:
                low_score = 0.5
            try:
                ds = float(row.get("deal_candidate_score") or 0.0)
            except Exception:
                ds = 0.0
            if float(deal.advertorial_score or 0.0) > adv_high and ds < low_score:
                deal.flags.append("preblock_advertorial_low_deal_score")

            cnt_m = re.search(r"부가\S*\s*(\d{1,2})\s*개", text)
            if cnt_m:
                try:
                    cnt = int(cnt_m.group(1))
                    deal.addons_count = cnt
                    if deal.carrier in {"KT", "LGU+"} and cnt > 3:
                        deal.flags.append("addons_count_suspicious")
                except Exception:
                    pass

            # conflict diagnostics: multiple distinct matches
            if seg_models_all and len(set(seg_models_all)) > 1:
                if deal.flags is None:
                    deal.flags = []
                deal.flags.append("dict_conflict_model")
            if seg_carriers_all and len(set(seg_carriers_all)) > 1:
                if deal.flags is None:
                    deal.flags = []
                deal.flags.append("dict_conflict_carrier")

            merged = asdict(deal)
            merged.update(row) # Carry over fields from normalized_posts
            results.append(merged)

    context.add_output_metadata({
        "n": len(results),
        "passed_prefilter": sum(1 for r in results if r.get("llm_prefilter_passed")),
        "filtered_non_deal": sum(1 for r in results if isinstance(r.get("flags"), list) and ("non_deal" in r.get("flags"))),
        "advertorial_high": sum(1 for r in results if isinstance(r.get("flags"), list) and ("advertorial_high" in r.get("flags"))),
        "conflict_models": sum(1 for r in results if isinstance(r.get("flags"), list) and ("dict_conflict_model" in r.get("flags"))),
        "conflict_carriers": sum(1 for r in results if isinstance(r.get("flags"), list) and ("dict_conflict_carrier" in r.get("flags"))),
        "skipped_no_comments": skipped_no_comments,
    })
    return results


def _gate_and_build_rows(data: List[dict]):
    good_rows: List[tuple] = []
    quarantine_rows: List[dict] = []
    seen: Dict[str, List[Dict[str, Optional[str]]]] = {}
    for d in data:
        if not d.get("llm_prefilter_passed"):
            continue

        model_f = d.get("model") or "미상"
        carrier_f = _normalize_carrier(d.get("carrier"))
        # 자급제 move_type이면 MVNO로 보정
        if not carrier_f and (d.get("move_type") or "").startswith("자급"):
            carrier_f = "MVNO"
        # If still invalid, quarantine this row by marking a skip_reason later
        if carrier_f not in CARRIER_ALLOWED:
            carrier_f = None
        capacity_f = d.get("capacity") or None
        hash_src = [
            str(d.get("post_id") or ""),
            str(d.get("url") or ""),
            str(model_f or ""),
            str(carrier_f or ""),
            str(d.get("move_type") or ""),
            str(d.get("contract") or ""),
            str(d.get("payment") or ""),
            str(d.get("upfront") or 0),
            str(d.get("plan_high_fee") or 0),
            str(d.get("plan_high_months") or 0),
            str(d.get("support_cash") or 0),
            str(capacity_f or ""),
            str(d.get("device_finance_total") or 0),
        ]
        hash_src_str = "|".join(hash_src)
        deal_hash = _md5_hex(hash_src_str)
        tco_total = _tco_total(d)
        tco_net = tco_total - int(d.get("support_cash") or 0)
        row_tuple = (
            (
                deal_hash,
                d.get("post_id"),
                d.get("url"),
                d.get("source_posted_at"),
                model_f,
                capacity_f,
                carrier_f,
                d.get("move_type"),
                d.get("contract"),
                d.get("payment"),
                d.get("channel"),
                d.get("city"),
                int(d.get("upfront") or 0),
                d.get("plan_high_fee"),
                d.get("plan_high_months"),
                d.get("plan_after_fee"),
                d.get("plan_after_months"),
                d.get("mvno_tail_fee"),
                d.get("mvno_tail_months"),
                d.get("addons_monthly"),
                d.get("addons_months"),
                d.get("addons_count"),
                d.get("device_finance_total"),
                d.get("device_finance_months"),
                d.get("device_finance_monthly"),
                d.get("support_cash"),
                d.get("retention_line_months"),
                d.get("retention_plan_months"),
                d.get("retention_addons_months"),
                d.get("contract_type"),
                d.get("contract_months"),
                d.get("contract_extra_support"),
                d.get("contract_support_amount"),
                Json(d.get("addons_detail") or []),
                d.get("store"),
                d.get("advertorial_score"),
                Json(d.get("flags") or []),
                tco_total,
                tco_net,
            )
        )

        info = {"post_id": str(d.get("post_id") or ""), "url": str(d.get("url") or ""), "title": str(d.get("title") or "")}
        skip_reason = None
        seen.setdefault(deal_hash, []).append(info)

        flags_list = d.get("flags") or []
        adv = float(d.get("advertorial_score") or 0.0)
        deal_score = float(d.get("deal_candidate_score") or 0.0)
        adv_high = float(os.getenv("SS_ADVERTORIAL_HIGH", "0.7") or 0.7)
        low_score = float(os.getenv("SS_DEAL_LOW_SCORE", "0.5") or 0.5)
        # Enforce carrier constraint before INSERT
        if not carrier_f:
            skip_reason = "invalid_carrier"
        elif "non_deal" in flags_list:
            skip_reason = "non_deal"
        elif adv > adv_high and deal_score < low_score:
            skip_reason = "advertorial_high_low_deal_score"
        elif adv > adv_high:
            skip_reason = "advertorial_high"

        if skip_reason:
            quarantine_rows.append({
                "deal_hash": deal_hash,
                "post_id": info["post_id"],
                "url": info["url"],
                "title": info["title"],
                "reason": skip_reason
            })
            continue

        good_rows.append(row_tuple)

    duplicate_report = {h: lst for h, lst in seen.items() if len(lst) > 1}
    return good_rows, quarantine_rows, duplicate_report


def _dedupe_by_hash(rows: List[tuple]) -> List[tuple]:
    seen = set()
    out: List[tuple] = []
    for t in rows:
        h = t[0]
        if h in seen:
            continue
        seen.add(h)
        out.append(t)
    return out


@dg.asset(
    name="ss_deals_upsert",
    group_name="SS",
    deps=[ss_bootstrap_schema, "ss_extracted_llm"],
    description="LLM 파이프라인을 통과한 최종 Deal 후보군을 deals 테이블에 UPSERT 합니다."
)
def ss_deals_upsert(context: dg.AssetExecutionContext, ss_postgres: PostgresResource, ss_extracted_llm: List[dict]):
    total_candidates = len(ss_extracted_llm or [])
    good_rows, quarantine_rows, duplicate_report = _gate_and_build_rows(ss_extracted_llm)
    good_rows = _dedupe_by_hash(good_rows)

    # FastFail on quarantine if requested
    fastfail = os.getenv("SS_FASTFAIL_ON_QUARANTINE", "0") == "1"
    if fastfail and quarantine_rows:
        # Surface a concise failure with first few examples
        examples = quarantine_rows[:5]
        raise dg.Failure(
            description=(
                f"Quarantined {len(quarantine_rows)} rows due to validation (e.g., invalid carrier/non_deal/advertorial). "
                f"Enable SS_FASTFAIL_ON_QUARANTINE=0 to skip failing the run."
            ),
            metadata={
                "quarantined_count": len(quarantine_rows),
                "examples": examples,
            },
        )

    inserted_or_updated = 0
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            if good_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO deals (
                        deal_hash, post_id, url, source_posted_at, model, capacity, carrier, move_type, contract, payment,
                        channel, city, upfront, plan_high_fee, plan_high_months, plan_after_fee, plan_after_months,
                        mvno_tail_fee, mvno_tail_months, addons_monthly, addons_months, addons_count,
                        device_finance_total, device_finance_months, device_finance_monthly,
                        support_cash, retention_line_months, retention_plan_months, retention_addons_months,
                        contract_type, contract_months, contract_extra_support, contract_support_amount, addons_detail, store,
                        advertorial_score, flags, tco_total, tco_net
                    ) VALUES %s
                    ON CONFLICT (deal_hash) DO UPDATE SET
                        url=EXCLUDED.url,
                        source_posted_at=EXCLUDED.source_posted_at,
                        model=EXCLUDED.model,
                        capacity=EXCLUDED.capacity,
                        carrier=EXCLUDED.carrier,
                        move_type=EXCLUDED.move_type,
                        contract=EXCLUDED.contract,
                        payment=EXCLUDED.payment,
                        channel=EXCLUDED.channel,
                        city=EXCLUDED.city,
                        upfront=EXCLUDED.upfront,
                        plan_high_fee=EXCLUDED.plan_high_fee,
                        plan_high_months=EXCLUDED.plan_high_months,
                        plan_after_fee=EXCLUDED.plan_after_fee,
                        plan_after_months=EXCLUDED.plan_after_months,
                        mvno_tail_fee=EXCLUDED.mvno_tail_fee,
                        mvno_tail_months=EXCLUDED.mvno_tail_months,
                        addons_monthly=EXCLUDED.addons_monthly,
                        addons_months=EXCLUDED.addons_months,
                        addons_count=EXCLUDED.addons_count,
                        device_finance_total=EXCLUDED.device_finance_total,
                        device_finance_months=EXCLUDED.device_finance_months,
                        device_finance_monthly=EXCLUDED.device_finance_monthly,
                        support_cash=EXCLUDED.support_cash,
                        retention_line_months=EXCLUDED.retention_line_months,
                        retention_plan_months=EXCLUDED.retention_plan_months,
                        retention_addons_months=EXCLUDED.retention_addons_months,
                        contract_type=EXCLUDED.contract_type,
                        contract_months=EXCLUDED.contract_months,
                        contract_extra_support=EXCLUDED.contract_extra_support,
                        contract_support_amount=EXCLUDED.contract_support_amount,
                        addons_detail=EXCLUDED.addons_detail,
                        store=EXCLUDED.store,
                        advertorial_score=EXCLUDED.advertorial_score,
                        flags=EXCLUDED.flags,
                        parsed_at=NOW(),
                        tco_total=EXCLUDED.tco_total,
                        tco_net=EXCLUDED.tco_net
                    """,
                    good_rows,
                    page_size=500,
                )
                inserted_or_updated = len(good_rows)
            conn.commit()
    passed = len(good_rows)
    quarantined = len(quarantine_rows)
    pass_rate = (passed / total_candidates) if total_candidates else 0.0
    quarantine_rate = (quarantined / total_candidates) if total_candidates else 0.0

    if duplicate_report:
        examples = []
        for h, arr in list(duplicate_report.items())[:5]:
            examples.append({"hash": h, "items": arr[:3]})
        context.log.warning(f"Duplicate deal_hash in batch (llm): count={len(duplicate_report)} examples={examples}")
    context.add_output_metadata({
        "candidates": total_candidates,
        "passed": passed,
        "quarantined": quarantined,
        "pass_rate": pass_rate,
        "quarantine_rate": quarantine_rate,
        "upserted": inserted_or_updated,
        "duplicates_in_batch": len(duplicate_report),
    })
    return inserted_or_updated


@dg.asset(
    name="ss_aggregates_daily",
    group_name="SS",
    automation_condition=EAGER,
    deps=[ss_bootstrap_schema, ss_deals_upsert],
    description="모델/용량별 7일 롤링 min/median/max/n 계산 및 저장"
)
def ss_aggregates_daily(context: dg.AssetExecutionContext, ss_postgres: PostgresResource):
    sql = """
    WITH recent AS (
      SELECT *
        FROM deals d
       WHERE d.source_posted_at IS NOT NULL
         AND d.source_posted_at >= (CURRENT_DATE - INTERVAL '7 days')
         AND d.tco_total IS NOT NULL
    ),
    cap_gb AS (
      SELECT r.model,
             r.capacity,
             r.source_posted_at::date AS ts,
             CASE
               WHEN r.capacity IS NULL THEN NULL
               WHEN r.capacity ~* 'tb' THEN (NULLIF(regexp_replace(r.capacity, '[^0-9]', '', 'g'), '')::INT) * 1024
               ELSE NULLIF(regexp_replace(r.capacity, '[^0-9]', '', 'g'), '')::INT
             END AS cap_gb,
             r.tco_total
        FROM recent r
    ),
    -- NEW: compute each model's smallest capacity from all historical data (not just the recent window)
    all_caps AS (
      SELECT d.model,
             CASE
               WHEN d.capacity ~* 'tb' THEN (NULLIF(regexp_replace(d.capacity, '[^0-9]', '', 'g'), '')::INT) * 1024
               ELSE NULLIF(regexp_replace(d.capacity, '[^0-9]', '', 'g'), '')::INT
             END AS cap_gb
        FROM deals d
       WHERE d.capacity IS NOT NULL
    ),
    min_cap AS (
      SELECT model, MIN(cap_gb) AS min_gb
        FROM all_caps
       GROUP BY model
    ),
    normalized AS (
      SELECT c.model,
             CASE
               WHEN COALESCE(c.cap_gb, m.min_gb) IS NULL THEN '미상'
               WHEN COALESCE(c.cap_gb, m.min_gb) >= 1024 AND COALESCE(c.cap_gb, m.min_gb) % 1024 = 0
                 THEN (COALESCE(c.cap_gb, m.min_gb) / 1024)::TEXT || 'TB'
               ELSE COALESCE(c.cap_gb, m.min_gb)::TEXT || 'GB'
             END AS capacity_norm,
             c.ts,
             c.tco_total
        FROM cap_gb c
        LEFT JOIN min_cap m USING (model)
    )
    INSERT INTO aggregates_daily (model, capacity, ts, min, p25, median, p75, max, avg, n)
    SELECT n.model,
           n.capacity_norm,
           n.ts,
           MIN(n.tco_total) AS min,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY n.tco_total) AS p25,
           PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY n.tco_total) AS median,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY n.tco_total) AS p75,
           MAX(n.tco_total) AS max,
           CAST(AVG(n.tco_total) AS INTEGER) AS avg,
           COUNT(*) AS n
      FROM normalized n
  GROUP BY n.model, n.capacity_norm, n.ts
    ON CONFLICT (model, capacity, ts) DO UPDATE SET
      min=EXCLUDED.min,
      p25=EXCLUDED.p25,
      median=EXCLUDED.median,
      p75=EXCLUDED.p75,
      max=EXCLUDED.max,
      avg=EXCLUDED.avg,
      n=EXCLUDED.n;
    """
    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
    context.add_output_metadata({"status": "ok"})
    return True
