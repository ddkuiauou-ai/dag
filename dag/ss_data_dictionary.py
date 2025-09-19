from typing import Dict, List, Tuple, Optional

import dagster as dg
from psycopg2.extras import execute_values, Json

import re
import unicodedata

from .resources import PostgresResource

# ---------------------------------------------
# Dictionary data (readable, centralized, deduped on load)
# ---------------------------------------------
# --- Normalization helpers (must mirror ss_data_sync) ---
_WS_RE = re.compile(r"\s+")
_ALLOW_CH = re.compile(r"[0-9a-zA-Z가-힣\+ ]")  # keep digits/latin/korean/+/space

def _normalize_keys(s: Optional[str]) -> Tuple[str, str]:
    if not s:
        return "", ""
    s = unicodedata.normalize("NFKC", s)
    s = unicodedata.normalize("NFC", s)
    s = s.casefold()
    kept = []
    for ch in s:
        kept.append(ch if _ALLOW_CH.match(ch) else " ")
    s = "".join(kept)
    s = _WS_RE.sub(" ", s).strip()
    key_a = s
    key_b = s.replace(" ", "")
    return key_a, key_b


CARRIERS: Dict[str, List[str]] = {
    # 대표 표기 우선, 중복/공백/동일 Key-B는 제거
    "SKT": ["SKT", "SK", "에스케이", "스크", "ㅅㅋ"],
    "KT": ["KT", "케이티", "크트", "ㅋㅌ"],
    "LGU+": [
        "LGU+",   # canonical with +
        "LG U+",  # spaced with +
        "U+",
        "LGU",
        "LG U",
        "유플",
        "유플러스",
        "엘지유플러스",
        "엘지",
        "LG"
    ],
    "MVNO": ["MVNO", "알뜰폰", "알뜰"],
    "자급제": ["자급제", "자급"],
}

MODELS: Dict[str, List[str]] = {
    # iPhone 17 family
    "아이폰 17 프로": ["아이폰 17 프로", "아이폰17프로", "17프로", "IPHONE 17 PRO", "아17프로"],
    "아이폰 17 에어": ["아이폰 17 에어", "아이폰17에어", "17에어", "아17에어"],
    "아이폰 17": ["아이폰 17", "아이폰17", "17일반", "아17"],

    # iPhone 16/15/14 family
    "아이폰 16 프로 맥스": ["아이폰 16 프로 맥스", "16 프맥", "IPHONE 16 PRO MAX"],
    "아이폰 16 프로": ["아이폰 16 프로", "IPHONE 16 PRO"],
    "아이폰 16": ["아이폰 16", "IPHONE 16"],
    "아이폰 15 프로 맥스": ["아이폰 15 프로 맥스", "15 프맥", "IPHONE 15 PRO MAX"],
    "아이폰 15 프로": ["아이폰 15 프로", "IPHONE 15 PRO"],
    "아이폰 15 플러스": ["아이폰 15 플러스", "IPHONE 15 PLUS"],
    "아이폰 15": ["아이폰 15", "IPHONE 15"],
    "아이폰 14 플러스": ["아이폰 14 플러스", "IPHONE 14 PLUS"],
    "아이폰 14 프로 맥스": ["아이폰 14 프로 맥스", "IPHONE 14 PRO MAX"],
    "아이폰 14 프로": ["아이폰 14 프로", "IPHONE 14 PRO"],
    "아이폰 14": ["아이폰 14", "IPHONE 14"],
    "아이폰 SE (3세대)": ["아이폰 SE 3", "SE3", "IPHONE SE 3"],

    # Galaxy S25/S24
    "갤럭시 S25 울트라": ["S25 울트라", "S25U", "GALAXY S25 ULTRA"],
    "갤럭시 S25": ["GALAXY S25", "S25"],
    "갤럭시 S25+": ["S25+", "S25 PLUS"],
    "갤럭시 S25 엣지": ["S25 엣지", "S25 EDGE", "S25+ EDGE"],
    "갤럭시 S24 울트라": ["S24 울트라", "S24U", "GALAXY S24 ULTRA"],
    "갤럭시 S24": ["GALAXY S24", "S24"],
    "갤럭시 S24+": ["S24+", "S24 PLUS"],
    "갤럭시 S23 FE": ["S23 FE", "GALAXY S23 FE", "FE S23"],

    # Galaxy Z series
    "갤럭시 Z 플립 7": ["Z 플립 7", "Z FLIP 7", "플립7", "플7"],
    "갤럭시 Z 폴드 7": ["Z 폴드 7", "Z FOLD 7", "폴드7", "폴7"],
    "갤럭시 Z 플립 6": ["Z 플립 6", "Z FLIP 6", "플립6", "플6"],
    "갤럭시 Z 폴드 6": ["Z 폴드 6", "Z FOLD 6", "폴드6", "폴6"],

    # Galaxy A series
    "갤럭시 A15": ["A15"],
    "갤럭시 A16": ["A16"],
    "갤럭시 A36": ["A36"],
    "갤럭시 A54": ["A54"],
    "갤럭시 A55": ["A55"],

    # Others
    "홍미노트 13 프로": ["홍미노트 13 프로", "REDMI NOTE 13 PRO"],
    "픽셀 8 프로": ["픽셀 8 프로", "PIXEL 8 PRO"],
    "픽셀 8": ["픽셀 8", "PIXEL 8"],
    "갤럭시 퀀텀 6": ["퀀텀 6", "퀀6", "퀀텀6", "QUANTUM 6"],
    "IM-100": ["IM-100"],
}


def _prepare_seed_rows(mapping: Dict[str, List[str]]) -> List[Tuple[str, Json, Json]]:
    """Return rows as (name, Json(aliases), Json(aliases_keyb)).

    - 입력 별칭을 정규화(Key-B) 후 **Key-B 기준으로 1개 대표 표기**만 남깁니다.
    - 대표 표기는 최초로 등장한 원문 별칭을 사용하고, 정식명(name)도 반드시 포함합니다.
    - 결과:
      * aliases: 사람이 읽을 대표 표기(원문) 목록 (중복 Key-B 제거됨)
      * aliases_keyb: aliases와 1:1 대응되는 Key-B 목록
    """
    rows: List[Tuple[str, Json, Json]] = []
    for name, aliases in mapping.items():
        # 1) canonical name
        name = str(name or "").strip()
        _, name_b = _normalize_keys(name)

        # 2) build keyB → representative alias map
        rep_map: Dict[str, str] = {}
        if name_b:
            rep_map.setdefault(name_b, name)  # prefer canonical as representative when first

        seen_raw = set()
        for a in aliases:
            a_raw = str(a or "").strip()
            if not a_raw or a_raw in seen_raw:
                continue
            seen_raw.add(a_raw)
            _, b = _normalize_keys(a_raw)
            if not b:
                continue
            # keep only the first seen alias for this Key-B (compaction)
            rep_map.setdefault(b, a_raw)

        # 3) materialize aligned lists (aliases ↔ aliases_keyb)
        #    keep stable order: canonical first (if exists), then others by insertion order
        aliases_keyb: List[str] = []
        aliases_rep: List[str] = []
        # ensure canonical first if present
        if name_b and name_b in rep_map:
            aliases_keyb.append(name_b)
            aliases_rep.append(rep_map[name_b])
        for b, rep in rep_map.items():
            if b == name_b:
                continue
            aliases_keyb.append(b)
            aliases_rep.append(rep)

        rows.append((name, Json(aliases_rep), Json(aliases_keyb)))
    return rows


@dg.asset(
    name="ss_bootstrap_dictionary_schema",
    group_name="SS_Dictionary",
    description="통신사 및 모델 사전을 위한 테이블/인덱스를 초기화(드롭 후 재생성)합니다. (파괴적 작업)",
)
def ss_bootstrap_dictionary_schema(
    context: dg.AssetExecutionContext, ss_postgres: PostgresResource
):
    """Idempotently ensures the required tables and indexes exist."""

    ddl = [
        # destructive reset: drop then create
        "DROP TABLE IF EXISTS carriers CASCADE;",
        "DROP TABLE IF EXISTS phone_models CASCADE;",
        # carriers
        """
        CREATE TABLE carriers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            aliases JSONB NOT NULL DEFAULT '[]'::jsonb,
            aliases_keyb JSONB NOT NULL DEFAULT '[]'::jsonb,
            CONSTRAINT carriers_aliases_is_array CHECK (jsonb_typeof(aliases) = 'array'),
            CONSTRAINT carriers_aliases_keyb_is_array CHECK (jsonb_typeof(aliases_keyb) = 'array')
        );
        """,
        # phone_models
        """
        CREATE TABLE phone_models (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            aliases JSONB NOT NULL DEFAULT '[]'::jsonb,
            aliases_keyb JSONB NOT NULL DEFAULT '[]'::jsonb,
            CONSTRAINT phone_models_aliases_is_array CHECK (jsonb_typeof(aliases) = 'array'),
            CONSTRAINT phone_models_aliases_keyb_is_array CHECK (jsonb_typeof(aliases_keyb) = 'array')
        );
        """,
        # indexes (fresh create)
        # NOTE: btree index on name is implicitly created by UNIQUE(name), so we DO NOT create a separate ix_*_name.
        # GIN indexes to accelerate alias containment queries
        "CREATE INDEX ix_carriers_aliases_gin ON carriers USING gin (aliases);",
        "CREATE INDEX ix_phone_models_aliases_gin ON phone_models USING gin (aliases);",
        "CREATE INDEX ix_carriers_aliases_keyb_gin ON carriers USING gin (aliases_keyb);",
        "CREATE INDEX ix_phone_models_aliases_keyb_gin ON phone_models USING gin (aliases_keyb);",
    ]

    with is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            for stmt in ddl:
                cur.execute(stmt)
        conn.commit()

    context.add_output_metadata(
        {
            "status": "ok",
            "tables": ["carriers", "phone_models"],
            "indexes": [
                "ix_carriers_aliases_gin",
                "ix_phone_models_aliases_gin",
                "ix_carriers_aliases_keyb_gin",
                "ix_phone_models_aliases_keyb_gin",
            ],
        }
    )
    return True


@dg.asset(
    name="ss_seed_dictionary_data",
    group_name="SS_Dictionary",
    deps=[ss_bootstrap_dictionary_schema],
    description="통신사 및 모델 사전에 초기/갱신 데이터를 업서트합니다.",
)
def ss_seed_dictionary_data(
    context: dg.AssetExecutionContext, ss_postgres: PostgresResource
):
    """Seed (insert or update) carriers and phone models in one transaction.

    - 데이터는 상단 상수(CARRIERS/MODELS)에서 관리합니다.
    - 중복/공백이 제거된 별칭 배열을 JSONB로 저장합니다.
    """

    carrier_rows = _prepare_seed_rows(CARRIERS)
    model_rows = _prepare_seed_rows(MODELS)

    with ss_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # Seed carriers
            execute_values(
                cur,
                (
                    "INSERT INTO carriers (name, aliases, aliases_keyb) VALUES %s "
                    "ON CONFLICT (name) DO UPDATE SET aliases = EXCLUDED.aliases, aliases_keyb = EXCLUDED.aliases_keyb"
                ),
                carrier_rows,
            )
            # Seed models
            execute_values(
                cur,
                (
                    "INSERT INTO phone_models (name, aliases, aliases_keyb) VALUES %s "
                    "ON CONFLICT (name) DO UPDATE SET aliases = EXCLUDED.aliases, aliases_keyb = EXCLUDED.aliases_keyb"
                ),
                model_rows,
            )
        conn.commit()

    total_aliases = sum(len(r[1].adapted) for r in carrier_rows) + sum(len(r[1].adapted) for r in model_rows)
    total_aliases_keyb = sum(len(r[2].adapted) for r in carrier_rows) + sum(len(r[2].adapted) for r in model_rows)

    context.add_output_metadata(
        {
            "status": "ok",
            "seeded_carriers": len(carrier_rows),
            "seeded_models": len(model_rows),
            "aliases_compacted_count": total_aliases,
            "aliases_keyb_count": total_aliases_keyb,
        }
    )
    return True