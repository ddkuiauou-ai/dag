# --- Standard library ---
import base64
import os
import json as _json
import traceback as _tb
from datetime import datetime as _dt
from urllib.parse import urlsplit
from dataclasses import dataclass
from typing import Set, List, Dict, Any, Optional, Tuple

# --- Third-party ---
import requests
from psycopg2.extras import Json as PgJson
import dagster as dg
from dagster_openai import OpenAIResource

# --- Local ---
from .resources import PostgresResource
from .schedules import ON3

# --- VLMConfig dataclass for config bundle ---
@dataclass(frozen=True)
class VLMConfig:
    target_format: str
    force_jpeg: bool
    image_max_bytes: int
    jpeg_quality: int
    png_color_threshold: int
    model: str
    preview_prompt: int
    preview_resp: int
    strict_json: bool
    route_ocr_when_ui_like: bool
    route_ocr_url_keywords: List[str]
    # OCR routing/bonus/thresholds (configurable via ENV)
    ocr_score_min: float
    ocr_text_density_min: float
    ocr_aspect_min: float
    ocr_ui_like_bonus: float
    ocr_kw_bonus: float
    ocr_png_bonus: float
    ocr_png_better_bonus: float
    heavy_text_density_min: float
    heavy_aspect_min: float
    montage_aspect_min: float
    montage_td_min: float
    montage_td_max: float
    montage_bypass_heavy: bool
    talltext_enable: bool
    talltext_aspect_min: float
    talltext_td_min: float

    @staticmethod
    def from_env() -> "VLMConfig":
        return VLMConfig(
            target_format=os.getenv("VLM_TARGET_FORMAT", "auto").lower(),
            force_jpeg=(os.getenv("VLM_FORCE_JPEG", "1").lower() not in ("0", "false", "no")),
            image_max_bytes=int(os.getenv("VLM_IMAGE_MAX_BYTES", "6291456")),
            jpeg_quality=int(os.getenv("VLM_JPEG_QUALITY", "90")),
            png_color_threshold=int(os.getenv("VLM_PNG_COLOR_THRESHOLD", "4096")),
            model=os.getenv("VLM_MODEL", "qwen/qwen2.5-vl-7b"),
            preview_prompt=int(os.getenv("VLM_LOG_PREVIEW_PROMPT", "800")),
            preview_resp=int(os.getenv("VLM_LOG_PREVIEW_RESP", "1200")),
            strict_json=(os.getenv("VLM_STRICT_JSON", "1").lower() not in ("0", "false", "no")),
            route_ocr_when_ui_like=(os.getenv("VLM_ROUTE_OCR_UI_LIKE", "1").lower() not in ("0", "false", "no")),
            route_ocr_url_keywords=[x.strip().lower() for x in os.getenv("VLM_ROUTE_OCR_URL_KEYWORDS", "screenshot,capture,screen,notice,announcement,receipt,invoice,statement,news,article,post,blog,comment,web,webpage,login,signup,policy,terms,alert,dialog,modal,tab,toolbar,menu,button,form,table,chart,graph,code,terminal,console").split(",") if x.strip()],
            ocr_score_min=float(os.getenv("VLM_ROUTE_OCR_SCORE_MIN", "2.0")),
            ocr_text_density_min=float(os.getenv("VLM_ROUTE_OCR_TD_MIN", "0.22")),
            ocr_aspect_min=float(os.getenv("VLM_ROUTE_OCR_ASPECT_MIN", "1.25")),
            ocr_ui_like_bonus=float(os.getenv("VLM_ROUTE_OCR_UI_BONUS", "0.8")),
            ocr_kw_bonus=float(os.getenv("VLM_ROUTE_OCR_KW_BONUS", "0.6")),
            ocr_png_bonus=float(os.getenv("VLM_ROUTE_OCR_PNG_BONUS", "0.4")),
            ocr_png_better_bonus=float(os.getenv("VLM_ROUTE_OCR_PNG_BETTER_BONUS", "0.6")),
            heavy_text_density_min=float(os.getenv("VLM_ROUTE_OCR_HEAVY_TD_MIN", "0.20")),
            heavy_aspect_min=float(os.getenv("VLM_ROUTE_OCR_HEAVY_ASPECT_MIN", "1.40")),
            montage_aspect_min=float(os.getenv("VLM_ROUTE_MONTAGE_ASPECT_MIN", "2.2")),
            montage_td_min=float(os.getenv("VLM_ROUTE_MONTAGE_TD_MIN", "0.06")),
            montage_td_max=float(os.getenv("VLM_ROUTE_MONTAGE_TD_MAX", "0.30")),
            montage_bypass_heavy=(os.getenv("VLM_ROUTE_MONTAGE_BYPASS_HEAVY", "1").lower() not in ("0", "false", "no")),
            talltext_enable=(os.getenv("VLM_ROUTE_TALLTEXT_ENABLE", "1").lower() not in ("0", "false", "no")),
            talltext_aspect_min=float(os.getenv("VLM_ROUTE_TALLTEXT_ASPECT_MIN", "2.8")),
            talltext_td_min=float(os.getenv("VLM_ROUTE_TALLTEXT_TD_MIN", "0.10")),
        )

# --- Specialized prompts for routing ---
VLM_SYSTEM_OCR_MSG = {
    "role": "system",
    "content": (
        """
당신은 Qwen2.5‑VL 기반의 **OCR/스크린샷 전용 분석기**입니다. 최종 출력은 **오직 하나의 JSON 객체**이며, **한국어**로 작성합니다. **추측 금지** — 화면에 보이는 정보만 사용하세요.

[목표]
- 화면의 **실제 텍스트를 가능한 한 그대로 재현**합니다. 요약/의역/추가 설명 금지.

[출력]
- **JSON 한 개만 출력.** 다른 텍스트/설명/코드펜스/주석(**`//`, `/* */`, `#`**) 금지. **트레일링 콤마 금지.**
- 필수 키: **caption, labels, ocr_lines, safety, objects, colors**

[대상] 스크린샷/UI/웹페이지/문서 사진 중심
- 우선순위: **ocr ▶ caption ▶ labels ▶ colors(≤3) ▶ objects(옵션)**

[ocr_lines — 생성 규칙]
1) **순서**: 좌→우, 상→하. 다중 패널/탭/창은 각 줄 앞에 `"panel1: …"`, `"panel2: …"`처럼 표시.
2) **줄 정의**: 논리적 문장/구 단위로 구성. 긴 문장은 **구두점/띄어쓰기 경계**에서 160자 이내로 분할.
3) **하이픈 줄바꿈 복원**: 줄 끝 `-` 로 끊긴 단어는 다음 줄과 **붙여 복원**(하이픈 제거).
4) **보존 원칙**: 대소문자, 숫자, 구두점, 통화기호, 단위, 슬래시/괄호, 날짜·시간·전화번호·이메일·URL을 **원문 그대로** 유지.
5) **불필요 기호 제거**: 글머리표(•, ·, *, -), 장식용 이모지/아이콘은 제거하되 **본문 텍스트는 유지**.
6) **공백 규칙**: 연속 공백은 1칸으로 축약, 탭은 공백으로 치환, 좌우 공백 제거.
7) **중복 억제**: 헤더/풋터/네비게이션/공유/쿠키 배너 등 반복 UI 문구는 **한 번만** 기록.
8) **표/코드 처리**:
   - 표는 **행 단위**로 `"table: <헤더/행내용>"` 형태 한 줄씩.
   - 코드/명령은 `"code: <내용>"` 한 줄씩. 자동 줄바꿈으로 잘린 경우 논리적으로 이어붙여 160자 이내 유지.
9) **언어**: 화면의 **원문 언어 그대로**. 번역/로마자화/의역 금지.
10) **불확실성 처리**: 글자가 애매하면 **생략**. 추측/자유 보정 금지.
11) **저가치 라인 컷**: 단일 글자/숫자/이모지, 빈 메뉴 항목, 스켈레톤 자리표시자, 광고/쿠키 수락 버튼, 구분선 등 **의미 없는** 요소는 제외.
12) **최대치**: 전체 **200줄**, 각 줄 **160자**.

[caption]
- 어떤 화면인지 1~2문장(약 60~140자). 페이지 종류/맥락만 간략히.

[labels]
- UI 요소/플랫폼/기능 중심 명사(예: 스크린샷, 댓글, 알림, 로그인, 가격표, 표, 버튼). **최대 20개.**

[colors]
- **최대 3개만.** `{name, hex, ratio}` 객체 배열. 색상 정보가 무의미하면 빈 배열 허용.

[objects]
- 필요 시에만(아이콘, 로고, 프로필사진 등). 없으면 빈 배열. **최대 20개.**

[safety]
- `{"nsfw":0~1,"violence":0~1}` 각 값은 **소수점 3자리**. 보이는 근거 있을 때만 상승.

[형식 강제]
- 키는 정확히: `caption`, `labels`, `ocr_lines`, `safety`, `objects`, `colors`.
- 값 타입: caption=string, labels=string[], ocr_lines=string[], safety=object, objects=object[], colors=object[].
- **JSON 이외의 어떤 텍스트도 출력하지 마세요.**
        """
    ),
}

VLM_SYSTEM_PHOTO_MSG = {
    "role": "system",
    "content": (
        """
당신은 Qwen2.5‑VL 기반의 **실세계 사진 전용 분석기**입니다. 출력은 **오직 하나의 JSON 객체**이며, **한국어**로 작성합니다. **과도한 추측은 피하되**, 사진 맥락상 합리적 추정은 문자열 내에 **"(추정)"** 표시로 허용합니다.

[출력]
- **JSON 한 개만 출력.** 다른 텍스트/설명/코드펜스/주석(**`//`, `/* */`, `#`**) 금지. **트레일링 콤마 금지.**
- 필수 키: **caption, labels, ocr_lines, safety, objects, colors**

[대상] 실세계 사진/풍경/인물
- 우선순위: **caption ▶ objects ▶ labels ▶ colors ▶ ocr**

[caption]
- 장면을 **사실적으로 묘사**하되, **조명/분위기/시간대/구도/카메라 앵글·렌즈 느낌** 등 **사진적 속성**을 포함해 **1~3문장**으로 작성합니다(길이 자율).
- 확신이 낮은 요소는 `(추정)`을 덧붙입니다. 인물 신원/민감 정보는 포함하지 않습니다.

[objects]
- 상위 사물/인물/장소 **3~12개 권장**(최대 20). `{name, count, confidence}`.
- `name`은 **단수 일반명사**(예: person, car, tree). 유사 항목은 통합.
- `count`는 추정 가능하며 정수. `confidence`∈[0,1], 소수점 3자리 권장.

[labels]
- 장면 키워드 + **스타일/구도/촬영속성** 혼합을 권장(예: 접사, 광각, 역광, 보케, 장노출, 파노라마, 실내, 야간, 비오는 날, 네온, 필름룩).
- **8~20개 권장**, 중복/동의어는 합치고 과도한 세분화는 피함.

[colors]
- **주조색/보조색/포인트색 위주 3~5개 권장.** `{name, hex, ratio}`. ratio는 [0,1], 합이 1을 넘지 않아야 함. 불명확하면 빈 배열.

[ocr_lines]
- **프레임 스택/긴 몽타주(세로로 매우 길고 여러 장면이 이어진 이미지)**: 전체 전사는 금지. **서로 다른 핵심 자막 3~10줄만 샘플링**, 장면 순서를 대략 유지하고 반복 자막은 제거.
- 표지판/간판/인쇄물 텍스트가 보일 때만 사용. 없으면 빈 배열.
    - **순서**: 좌→우, 상→하.
    - **보존**: 대소문자/숫자/구두점/단위/URL/이메일/전화번호 **원문 그대로**. 번역 금지.
    - **하이픈 줄바꿈 복원**: 줄 끝 `-` 연결.
    - **잡음/중복 억제**: 장식 아이콘·이모지·반복 메뉴 제외, 동일 줄 1회.
    - **길이**: 각 줄 최대 160자.

[safety]
- `{"nsfw":0~1,"violence":0~1}` 각 값은 **소수점 3자리**. 보이는 근거 있을 때만 상승.

[형식 강제]
- 키는 정확히: `caption`, `labels`, `ocr_lines`, `safety`, `objects`, `colors`.
- 값 타입: caption=string, labels=string[], ocr_lines=string[], safety=object, objects=object[], colors=object[].
- **JSON 이외의 어떤 텍스트도 출력하지 마세요.**
        """
    ),
}

def VLM_USER_TMPL(data_url: str) -> List[Dict[str, Any]]:
    """Build user content array for multimodal VLM calls.
    Accepts a data URL and returns the list payload expected by OpenAI-compatible
    chat.completions (image-only user message).
    """
    return [
        {"type": "image_url", "image_url": {"url": data_url}},
    ]


# --- VLM response parsing & normalization helpers ---
CONTROL_TRANSLATION_TABLE = {i: " " for i in range(0x00, 0x20)}
CONTROL_TRANSLATION_TABLE[ord("\n")] = " | "
CONTROL_TRANSLATION_TABLE[ord("\r")] = " "

@dataclass(frozen=True)
class VLMResult:
    caption: Optional[str]
    labels: List[str]
    ocr_lines: List[str]          # ← 변경 (ocr_text 제거)
    safety: Dict[str, float]
    objects: List[Dict[str, Any]]
    colors: List[Dict[str, Any]]

def _sanitize_model_text(s: str) -> str:
    t = str(s or "")
    return t.translate(CONTROL_TRANSLATION_TABLE).strip()

def _normalize_labels(labels_in) -> List[str]:
    if isinstance(labels_in, str):
        candidates = [x.strip() for x in labels_in.split(",")]
    elif isinstance(labels_in, list):
        candidates = [x.strip() for x in labels_in if isinstance(x, str)]
    else:
        candidates = []
    return list(dict.fromkeys(x for x in candidates if x))[:20]

def _normalize_ocr_lines(ocr_in) -> List[str]:
    MAX_LINES = 200
    MAX_LINE_LEN = 160
    def _clean(s: str) -> str:
        return _sanitize_model_text(s)[:MAX_LINE_LEN]
    lines: List[str] = []
    if isinstance(ocr_in, list):
        for it in ocr_in:
            if isinstance(it, str):
                t = _clean(it)
                if t:
                    lines.append(t)
    elif isinstance(ocr_in, str):
        parts = [p.strip() for p in ocr_in.split("|")]
        for p in parts:
            t = _clean(p)
            if t:
                lines.append(t)
    seen = set(); out = []
    for s in lines:
        if s not in seen:
            out.append(s); seen.add(s)
        if len(out) >= MAX_LINES:
            break
    return out

def _clamp01(v):
    try:
        f = float(v)
    except Exception:
        return None
    return 0.0 if f < 0 else 1.0 if f > 1 else f

def _normalize_objects(objs_in) -> List[Dict[str, Any]]:
    from collections import defaultdict as _dd
    norm_objs: List[Dict[str, Any]] = []
    if isinstance(objs_in, list):
        tmp = []
        for it in objs_in:
            if isinstance(it, str):
                name = it.strip().lower()
                if name:
                    tmp.append({"name": name, "count": 1})
            elif isinstance(it, dict):
                name = str(it.get("name", "")).strip().lower()
                if not name:
                    continue
                try:
                    cnt = int(it.get("count")) if it.get("count") is not None else 1
                except Exception:
                    cnt = 1
                conf = it.get("confidence")
                try:
                    conf = float(conf)
                    conf = 0.0 if conf < 0 else 1.0 if conf > 1 else conf
                except Exception:
                    conf = None
                d = {"name": name, "count": cnt}
                if conf is not None:
                    d["confidence"] = conf
                tmp.append(d)
        agg = _dd(lambda: {"count": 0, "conf_sum": 0.0, "conf_n": 0})
        for d in tmp:
            k = d["name"]
            a = agg[k]
            a["count"] += max(1, int(d.get("count") or 1))
            if "confidence" in d:
                a["conf_sum"] += float(d["confidence"])
                a["conf_n"] += 1
        for name, v in agg.items():
            out = {"name": name, "count": v["count"]}
            if v["conf_n"] > 0:
                out["confidence"] = round(v["conf_sum"] / v["conf_n"], 3)
            norm_objs.append(out)
        norm_objs = sorted(norm_objs, key=lambda x: (-x.get("count", 0), x.get("name", "")))[:20]
    return norm_objs

def _normalize_colors(cols_in) -> List[Dict[str, Any]]:
    from collections import defaultdict as _dd
    norm_cols: List[Dict[str, Any]] = []
    if isinstance(cols_in, list):
        tmp = []
        for it in cols_in:
            if isinstance(it, str):
                name = it.strip().lower()
                if name:
                    tmp.append({"name": name, "ratio": 0.0})
            elif isinstance(it, dict):
                name = str(it.get("name", "")).strip().lower()
                hexv = str(it.get("hex", "")).strip().lower()
                ratio = it.get("ratio")
                try:
                    ratio = float(ratio)
                    ratio = 0.0 if ratio < 0 else 1.0 if ratio > 1 else ratio
                except Exception:
                    ratio = 0.0
                if name or hexv:
                    tmp.append({"name": name or None, "hex": hexv or None, "ratio": ratio})
        agg = _dd(lambda: {"name": None, "hex": None, "ratio": 0.0})
        for d in tmp:
            key = d.get("hex") or d.get("name") or ""
            a = agg[key]
            if d.get("name"):
                a["name"] = d.get("name")
            if d.get("hex"):
                a["hex"] = d.get("hex")
            a["ratio"] += float(d.get("ratio") or 0.0)
        for _key, v in agg.items():
            out = {"name": v.get("name"), "hex": v.get("hex"), "ratio": round(v.get("ratio", 0.0), 3)}
            norm_cols.append(out)
        # Clamp to top 3 colors for OCR usage (was 10)
        norm_cols = sorted(norm_cols, key=lambda x: -x.get("ratio", 0.0))[:3]
    return norm_cols
def _preprocess_for_ocr(im, mode: str):
    """
    Preprocess PIL image for OCR.
    - mode: "heavy" or "light"
    - Returns processed PIL Image
    """
    from PIL import Image, ImageEnhance, ImageFilter, ImageOps
    try:
        import cv2
    except ImportError:
        raise ImportError("OpenCV (cv2) is required for OCR preprocessing but not installed.")
    import numpy as np
    im_proc = im
    try:
        # Always work in RGB for consistency
        if im_proc.mode not in ("RGB", "L"):
            im_proc = im_proc.convert("RGB")
    except Exception:
        pass
    # Heavy preprocessing for dense text/screenshot
    if mode == "heavy":
        # Deskew
        try:
            arr = np.array(im_proc.convert("L"))
            coords = np.column_stack(np.where(arr > 0))
            if coords.size > 0:
                angle = 0.0
                try:
                    rect = cv2.minAreaRect(coords)
                    angle = rect[-1]
                    if angle < -45:
                        angle = -(90 + angle)
                    else:
                        angle = -angle
                    (h, w) = arr.shape
                    M = cv2.getRotationMatrix2D((w // 2, h // 2), angle, 1.0)
                    arr_rot = cv2.warpAffine(arr, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
                    im_proc = Image.fromarray(arr_rot).convert("RGB")
                except Exception:
                    pass
        except Exception:
            pass
        # Upsample if too small (<1600-2200 max dim)
        try:
            max_dim = max(im_proc.size)
            if max_dim < 1600:
                scale = 1600.0 / max_dim
                newsize = (int(im_proc.size[0]*scale), int(im_proc.size[1]*scale))
                im_proc = im_proc.resize(newsize, Image.BICUBIC)
        except Exception:
            pass
        # CLAHE on L channel
        try:
            arr = np.array(im_proc.convert("LAB"))
            l, a, b = arr[:,:,0], arr[:,:,1], arr[:,:,2]
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
            l2 = clahe.apply(l)
            arr2 = np.stack([l2,a,b], axis=2)
            im_proc = Image.fromarray(arr2, mode="LAB").convert("RGB")
        except Exception:
            try:
                im_proc = ImageOps.autocontrast(im_proc)
            except Exception:
                pass
        # Unsharp mask (PIL)
        try:
            im_proc = im_proc.filter(ImageFilter.UnsharpMask(radius=2, percent=120, threshold=3))
        except Exception:
            pass
        # Adaptive threshold (binarization)
        try:
            arr = np.array(im_proc.convert("L"))
            th = cv2.adaptiveThreshold(arr, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 11)
            im_proc = Image.fromarray(th).convert("RGB")
        except Exception:
            pass
    # Light preprocessing for photo
    elif mode == "light":
        # Light autocontrast
        try:
            im_proc = ImageOps.autocontrast(im_proc)
        except Exception:
            pass
        # Light unsharp
        try:
            im_proc = im_proc.filter(ImageFilter.UnsharpMask(radius=1, percent=80, threshold=3))
        except Exception:
            pass
    return im_proc

def _parse_and_normalize_vlm(content: str, job_ctx: Dict[str, Any], context, preview_len: int) -> VLMResult:
    import json as _json
    if not isinstance(content, str) or not content.strip():
        raise dg.Failure(description="VLM JSON 파싱 실패: empty content")

    # 로그만 찍고 원문은 손대지 않음
    _log(context, "info", "[VLM][RESP]", _trim(content, preview_len), preview_len)

    # Strict first; if it fails, strip code fences/markdown and slice the first JSON object
    try:
        obj = _json.loads(content)
    except Exception as e1:
        s = str(content or "").strip()
        # --- 1) Remove code fences ```...``` (with optional language tag) ---
        if s.startswith("```"):
            nl = s.find("\n")
            if nl != -1:
                s = s[nl+1:]
            if s.endswith("```"):
                s = s[:-3]
            s = s.strip()
        # --- 2) If still not a pure JSON object, slice from the first '{' to the last '}' ---
        if not s.startswith("{"):
            l = s.find("{")
            r = s.rfind("}")
            if l != -1 and r != -1 and r > l:
                s = s[l:r+1].strip()
        # Final attempt
        try:
            obj = _json.loads(s)
        except Exception as e2:
            raise dg.Failure(description=f"VLM JSON 파싱 실패: {type(e2).__name__}: {e2}")

    if not isinstance(obj, dict):
        raise dg.Failure(description="VLM JSON 파싱 실패: root is not an object")

    # 필수 키 강제
    required_keys = {"caption", "labels", "ocr_lines", "safety", "objects", "colors"}
    missing = [k for k in required_keys if k not in obj]
    if missing:
        raise dg.Failure(description=f"VLM JSON 포맷 오류: missing keys: {', '.join(missing)}")

    ocr_lines = _normalize_ocr_lines(obj.get("ocr_lines"))

    # 최소 정규화(가독성 위주, 가공 최소화)
    caption_val = obj.get("caption")
    caption = _sanitize_model_text(str(caption_val))[:200] if caption_val is not None else None

    labels = _normalize_labels(obj.get("labels"))


    safety_raw = obj.get("safety") or {}
    safety: Dict[str, float] = {}
    if isinstance(safety_raw, dict):
        maybe = {k: _clamp01(safety_raw.get(k)) for k in ("nsfw", "violence")}
        safety = {k: round(v, 3) for k, v in maybe.items() if v is not None}

    norm_objs = _normalize_objects(obj.get("objects"))
    norm_cols = _normalize_colors(obj.get("colors"))

    parsed_preview = _trim(_json.dumps(
    {"caption": caption, "labels": labels, "ocr_lines": len(ocr_lines), "safety": safety}, ensure_ascii=False), preview_len)
    _log(context, "debug", "[VLM][PARSED]", parsed_preview, preview_len)

    return VLMResult(
        caption=caption,
        labels=labels,
        ocr_lines=ocr_lines,
        safety=safety,
        objects=norm_objs,
        colors=norm_cols,
    )

# === Module-level utilities: logging, JSON, image handling, validation ===

def _trim(s: object, n: int = 800) -> str:
    if s is None:
        return ""
    t = str(s)
    return t if len(t) <= n else t[:n] + "…(truncated)"

def _log(context, level: str, tag: str, payload: object, preview_len: int = 800):
    from contextlib import suppress
    import json as _json
    with suppress(Exception):
        logger = getattr(context.log, level, None)
        if not callable(logger):
            return
        if isinstance(payload, str):
            msg = _trim(payload, preview_len)
        else:
            try:
                msg = _trim(_json.dumps(payload, ensure_ascii=False, default=str), preview_len)
            except Exception:
                msg = _trim(str(payload), preview_len)
        logger(f"{tag} " + msg)

def _exc_dict(e):
    try:
        return {"type": type(e).__name__, "msg": str(e), "trace": _tb.format_exc()}
    except Exception:
        return {"type": type(e).__name__, "msg": str(e)}

def _sniff_image_mime(raw: bytes) -> str:
    try:
        if not raw: return ""
        if raw.startswith(b"\xff\xd8\xff"): return "image/jpeg"
        if raw.startswith(b"\x89PNG\r\n\x1a\n"): return "image/png"
        if len(raw) >= 12 and raw[0:4] == b"RIFF" and raw[8:12] == b"WEBP": return "image/webp"
        if raw.startswith(b"GIF8"): return "image/gif"
        head = raw[:32]
        if b"ftypavif" in head: return "image/avif"
        if (b"ftypmif1" in head) or (b"ftypheic" in head) or (b"ftypheif" in head) or (b"ftyphevc" in head): return "image/heic"
        return ""
    except Exception:
        return ""

def _is_ui_like_image_pil(im, png_color_threshold: int) -> bool:
    try:
        bands = getattr(im, "getbands", lambda: tuple())()
        if "A" in bands or im.mode in ("P", "LA"):
            return True
        from PIL import Image
        w, h = im.size
        if max(w, h) > 512:
            scale = 512.0 / float(max(w, h))
            size = (max(1, int(w * scale)), max(1, int(h * scale)))
            im_small = im.convert("RGB").resize(size, Image.BILINEAR)
        else:
            im_small = im.convert("RGB")
        colors = im_small.getcolors(maxcolors=65536)
        if colors is None:
            return False
        uniq = len(colors)
        return uniq <= png_color_threshold
    except Exception:
        return False

# --- Lightweight text/edge density estimator for PIL images ---
def _estimate_text_density_pil(im) -> float:
    """
    Rough text/edge density estimator in [0,1] using only PIL:
    - Convert to grayscale
    - Downscale longest side to 512
    - Edge detection (FIND_EDGES)
    - Binarize via 85th percentile threshold
    - Return white-pixel ratio
    """
    try:
        from PIL import Image, ImageFilter
        w, h = im.size
        if max(w, h) > 512:
            scale = 512.0 / float(max(w, h))
            size = (max(1, int(w * scale)), max(1, int(h * scale)))
            g = im.convert("L").resize(size, Image.BILINEAR)
        else:
            g = im.convert("L")
        edges = g.filter(ImageFilter.FIND_EDGES)
        hist = edges.histogram()
        total = sum(hist)
        if total <= 0:
            return 0.0
        cum = 0
        thr = 255
        target = total * 0.85
        for i, c in enumerate(hist):
            cum += c
            if cum >= target:
                thr = i
                break
        bw = edges.point(lambda x, t=thr: 255 if x >= t else 0)
        white = sum(1 for px in bw.getdata() if px == 255)
        return round(white / (bw.size[0] * bw.size[1]), 3)
    except Exception:
        return 0.0

def _decide_target_format(orig_mime: str, im, target_mode: str, force_jpeg: bool, png_color_threshold: int) -> str:
    mode = (target_mode or "auto").lower()
    if mode in ("jpeg", "jpg"): return "JPEG"
    if mode == "png": return "PNG"
    if orig_mime in ("image/png", "image/gif"): return "PNG"
    try:
        if _is_ui_like_image_pil(im, png_color_threshold): return "PNG"
    except Exception:
        pass
    if force_jpeg and mode == "auto": return "JPEG"
    return "JPEG"

def _img_to_data_url(url: str, cfg: VLMConfig):
    dbg = {"url": url}

    def _load_image_exif_safe(raw_bytes: bytes):
        from PIL import Image, ImageOps
        import io
        im = Image.open(io.BytesIO(raw_bytes))
        try: im.seek(0)
        except Exception: pass
        try: im = ImageOps.exif_transpose(im)
        except Exception: pass
        return im

    def _to_png_bytes(im):
        import io
        if im.mode not in ("RGBA", "LA", "P"):
            im = im.convert("RGB")
        buf = io.BytesIO(); im.save(buf, format="PNG", optimize=True)
        return buf.getvalue()

    def _to_jpeg_bytes(im, quality: int):
        import io
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        buf = io.BytesIO(); im.save(buf, format="JPEG", quality=quality, optimize=True)
        return buf.getvalue()

    def _convert_to_png_or_jpeg(raw_bytes: bytes, mime_in: str):
        info = {}
        try:
            im = _load_image_exif_safe(raw_bytes)
            # Additional lightweight signals for router
            try:
                # Text/edge density (0..1)
                info["text_density"] = _estimate_text_density_pil(im)
            except Exception:
                pass
            try:
                # Compare compressibility on a small RGB sample
                from PIL import Image
                import io
                sample = im
                if max(im.size) > 512:
                    scale = 512.0 / float(max(im.size))
                    sample = im.convert("RGB").resize((max(1, int(im.size[0] * scale)), max(1, int(im.size[1] * scale))), Image.BILINEAR)
                else:
                    sample = im.convert("RGB")
                bufp = io.BytesIO(); sample.save(bufp, format="PNG", optimize=True)
                len_png = bufp.tell()
                bufj = io.BytesIO(); sample.save(bufj, format="JPEG", quality=int(cfg.jpeg_quality), optimize=True)
                len_jpg = bufj.tell()
                info["png_vs_jpeg"] = {"png": len_png, "jpeg": len_jpg, "png_better": bool(len_png < len_jpg * 0.9)}
            except Exception:
                pass
            try:
                info["width"], info["height"] = im.size
            except Exception:
                pass
            ui_like = _is_ui_like_image_pil(im, cfg.png_color_threshold)
            info["ui_like"] = bool(ui_like)
            decided = _decide_target_format(mime_in, im, cfg.target_format, cfg.force_jpeg, cfg.png_color_threshold)
            if decided == "PNG":
                out = _to_png_bytes(im); info["target_format"] = "png"
                if len(out) > int(cfg.image_max_bytes):
                    out = _to_jpeg_bytes(im, int(cfg.jpeg_quality))
                    info["fallback_due_to_size"] = True; info["fallback_from"] = "image/png"; mime_out = "image/jpeg"
                else:
                    mime_out = "image/png"
            else:
                out = _to_jpeg_bytes(im, int(cfg.jpeg_quality)); info["target_format"] = "jpeg"; mime_out = "image/jpeg"
            info["converted_from"] = mime_in; info["bytes_after"] = len(out)
            return out, mime_out, info
        except Exception as e:
            info["convert_error"] = f"{type(e).__name__}: {e}"
            return raw_bytes, mime_in, info

    if not url:
        dbg["error"] = "empty url"; return "", dbg

    if url.startswith("data:"):
        dbg["mode"] = "data-url"
        head_body = url.split(",", 1)
        if len(head_body) == 2:
            try:
                head = head_body[0]; b64 = head_body[1]
                mime_hdr = head[5:].split(";", 1)[0].lower()
                dbg["mime"] = mime_hdr; dbg["raw_b64_len"] = len(b64)
            except Exception:
                pass
        return url.strip(), dbg

    try:
        u = urlsplit(url); ref = f"{u.scheme}://{u.netloc}/"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/avif,image/png,image/jpeg;q=0.9,*/*;q=0.8",
            "Referer": ref,
            "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
        }
        r = requests.get(url, timeout=10, headers=headers, allow_redirects=True)
        dbg["status"] = getattr(r, "status_code", None)
        dbg["content_type"] = r.headers.get("Content-Type") if hasattr(r, "headers") else None
        r.raise_for_status()

        raw = r.content or b""
        dbg["fetched_bytes"] = len(raw)
        header_mime = (r.headers.get("Content-Type") or "").split(";", 1)[0].lower()
        sniffed = _sniff_image_mime(raw)
        mime = sniffed or (header_mime if header_mime.startswith("image/") else "")

        dbg["header_mime"] = header_mime or None
        dbg["sniffed_mime"] = sniffed or None

        if not mime or not mime.startswith("image/") or len(raw) == 0:
            dbg["error"] = f"non-image or empty payload (mime={header_mime}, bytes={len(raw)})"
            return "", dbg

        # --- Early router feature extraction (independent of conversion) ---
        try:
            im_probe = _load_image_exif_safe(raw)
            try:
                dbg["width"], dbg["height"] = im_probe.size
            except Exception:
                pass
            try:
                dbg["ui_like"] = bool(_is_ui_like_image_pil(im_probe, cfg.png_color_threshold))
            except Exception:
                pass
            try:
                dbg["text_density"] = _estimate_text_density_pil(im_probe)
            except Exception:
                pass
            try:
                from PIL import Image
                import io
                sample = im_probe
                if max(im_probe.size) > 512:
                    scale = 512.0 / float(max(im_probe.size))
                    sample = im_probe.convert("RGB").resize(
                        (max(1, int(im_probe.size[0] * scale)), max(1, int(im_probe.size[1] * scale))),
                        Image.BILINEAR
                    )
                else:
                    sample = im_probe.convert("RGB")
                bufp = io.BytesIO(); sample.save(bufp, format="PNG", optimize=True)
                len_png = bufp.tell()
                bufj = io.BytesIO(); sample.save(bufj, format="JPEG", quality=int(cfg.jpeg_quality), optimize=True)
                len_jpg = bufj.tell()
                dbg["png_vs_jpeg"] = {"png": len_png, "jpeg": len_jpg, "png_better": bool(len_png < len_jpg * 0.9)}
            except Exception:
                pass
            # --- OCR-oriented preprocessing ---
            heavy = False
            try:
                width, height = im_probe.size
                aspect = height / width if width > 0 else 0.0
                td = dbg.get("text_density", 0.0)
                heavy = (td >= 0.14 and aspect >= 1.3)
                # Ensure router_features exists and set heavy_pass
                if "router_features" not in dbg or not isinstance(dbg["router_features"], dict):
                    dbg["router_features"] = {}
                dbg["router_features"]["heavy_pass"] = heavy
                if heavy:
                    im_proc = _preprocess_for_ocr(im_probe, "heavy")
                else:
                    im_proc = _preprocess_for_ocr(im_probe, "light")
            except Exception:
                im_proc = im_probe
                # still set heavy flag if possible
                try:
                    width, height = im_probe.size
                    aspect = height / width if width > 0 else 0.0
                    td = dbg.get("text_density", 0.0)
                    heavy = (td >= 0.14 and aspect >= 1.3)
                    if "router_features" not in dbg or not isinstance(dbg["router_features"], dict):
                        dbg["router_features"] = {}
                    dbg["router_features"]["heavy_pass"] = heavy
                except Exception:
                    pass
            # Use im_proc for all downstream signals
            try:
                dbg["width"], dbg["height"] = im_proc.size
            except Exception:
                pass
            try:
                dbg["ui_like"] = bool(_is_ui_like_image_pil(im_proc, cfg.png_color_threshold))
            except Exception:
                pass
            try:
                dbg["text_density"] = _estimate_text_density_pil(im_proc)
            except Exception:
                pass
            try:
                from PIL import Image
                import io
                sample = im_proc
                if max(im_proc.size) > 512:
                    scale = 512.0 / float(max(im_proc.size))
                    sample = im_proc.convert("RGB").resize(
                        (max(1, int(im_proc.size[0] * scale)), max(1, int(im_proc.size[1] * scale))),
                        Image.BILINEAR
                    )
                else:
                    sample = im_proc.convert("RGB")
                bufp = io.BytesIO(); sample.save(bufp, format="PNG", optimize=True)
                len_png = bufp.tell()
                bufj = io.BytesIO(); sample.save(bufj, format="JPEG", quality=int(cfg.jpeg_quality), optimize=True)
                len_jpg = bufj.tell()
                dbg["png_vs_jpeg"] = {"png": len_png, "jpeg": len_jpg, "png_better": bool(len_png < len_jpg * 0.9)}
            except Exception:
                pass
        except Exception:
            pass

        converted = False
        need_convert = (
            mime in ("image/webp", "image/avif", "image/heic", "image/heif", "image/gif")
            or cfg.target_format in ("png", "jpeg", "jpg")
            or mime not in ("image/png", "image/jpeg")
        )
        if need_convert:
            raw, mime, conv_info = _convert_to_png_or_jpeg(raw, mime); dbg.update(conv_info)
            converted = True if conv_info.get("target_format") else False

        if mime not in ("image/png", "image/jpeg"):
            try:
                im2 = _load_image_exif_safe(raw)
                raw = _to_jpeg_bytes(im2, int(cfg.jpeg_quality))
                dbg["force_convert_from"] = mime; mime = "image/jpeg"
                dbg["target_format"] = "jpeg"; dbg["bytes_after"] = len(raw)
            except Exception as e:
                dbg["error"] = f"unsupported final mime {mime}: {type(e).__name__}: {e}"
                return "", dbg

        # Use im_proc for the final conversion (if available), else fallback to im_probe or reload
        try:
            im_out = im_proc
        except Exception:
            try:
                im_out = im_probe
            except Exception:
                from PIL import Image
                import io
                im_out = Image.open(io.BytesIO(raw))
        # Determine heavy flag
        heavy = False
        try:
            rf = dbg.get("router_features", {})
            if isinstance(rf, dict):
                heavy = bool(rf.get("heavy_pass", False))
        except Exception:
            pass
        # If heavy, prefer PNG, but fallback to JPEG if too large
        output_mime = mime
        fallback_due_to_size = False
        fallback_from = None
        target_format = None
        try:
            if heavy:
                # Try PNG first
                out = _to_png_bytes(im_out)
                target_format = "png"
                output_mime = "image/png"
                if len(out) > int(cfg.image_max_bytes):
                    out = _to_jpeg_bytes(im_out, int(cfg.jpeg_quality))
                    fallback_due_to_size = True
                    fallback_from = "image/png"
                    target_format = "jpeg"
                    output_mime = "image/jpeg"
            else:
                if mime == "image/png":
                    out = _to_png_bytes(im_out)
                    target_format = "png"
                    output_mime = "image/png"
                else:
                    out = _to_jpeg_bytes(im_out, int(cfg.jpeg_quality))
                    target_format = "jpeg"
                    output_mime = "image/jpeg"
        except Exception:
            # fallback to raw
            out = raw
        # Set debug info for heavy path/fallback
        dbg["target_format"] = target_format
        if fallback_due_to_size:
            dbg["fallback_due_to_size"] = True
            dbg["fallback_from"] = fallback_from
        dbg["mime"] = output_mime
        b64 = base64.b64encode(out).decode("ascii")
        dbg["raw_b64_len"] = len(b64); dbg["converted"] = converted
        return f"data:{output_mime};base64,{b64}", dbg
    except Exception as e:
        dbg["error"] = f"{type(e).__name__}: {e}"
        return "", dbg

def _extract_error_from_response(r):
    try:
        if hasattr(r, "model_dump"): d = r.model_dump()
        elif isinstance(r, dict): d = r
        else: d = {}
        return d.get("error") or d.get("Error")
    except Exception:
        return None

def _resp_preview(r, max_len: int = 1200) -> str:
    try:
        if hasattr(r, "model_dump_json"):
            return str(r.model_dump_json())[:max_len]
    except Exception:
        pass
    try:
        if hasattr(r, "model_dump"):
            import json as _json
            return _trim(_json.dumps(r.model_dump(), ensure_ascii=False), max_len)
    except Exception:
        pass
    try:
        import json as _json
        return _trim(_json.dumps(r, default=str, ensure_ascii=False), max_len)
    except Exception:
        return _trim(str(r), max_len)

def _validate_data_url(s: str):
    import base64
    if not isinstance(s, str) or not s:
        return False, "not a string"
    if not s.startswith("data:"):
        return False, "not a data URL"
    head_body = s.split(",", 1)
    if len(head_body) != 2:
        return False, "no comma separator"
    head, b64 = head_body[0], head_body[1].strip()
    if ";base64" not in head:
        return False, "missing ;base64 flag"
    mime = head[5:].split(";", 1)[0] if head.startswith("data:") else ""
    if not b64:
        return False, "empty base64 payload"
    try:
        raw = base64.b64decode(b64, validate=True)
    except Exception as e:
        return False, f"base64 decode error: {e}"
    meta = {"mime": mime or "", "b64_len": len(b64), "byte_len": len(raw)}
    return (len(raw) > 0), meta

def _make_response_format(cfg: VLMConfig):
    """
    Build response_format compatible with current model/endpoint.
    - If strict_json: use json_schema (strongest server-side enforcement)
    - Else: use text (client-side parsing/validation)
    """
    if cfg.strict_json:
        # Minimal schema that matches our parser's required keys & shapes
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "VLMOutput",
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["caption", "labels", "ocr_lines", "safety", "objects", "colors"],
                    "properties": {
                        "caption": {"type": "string"},
                        "labels": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 20
                        },
                        "ocr_lines": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "maxLength": 160
                            },
                            "maxItems": 200
                        },
                        "safety": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "nsfw": {"type": "number"},
                                "violence": {"type": "number"},
                            },
                        },
                        "objects": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "name": {"type": "string"},
                                    "count": {"type": "integer"},
                                    "confidence": {"type": "number"},
                                },
                            },
                            "maxItems": 20
                        },
                        "colors": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": True,
                                "properties": {
                                    "name": {"type": ["string", "null"]},
                                    "hex": {"type": ["string", "null"]},
                                    "ratio": {"type": "number"},
                                },
                            },
                            "maxItems": 10
                        },
                    },
                },
            },
        }
    else:
        return {"type": "text"}
        
def _select_vlm_prompt(fetch_dbg: Dict[str, Any], cfg: VLMConfig) -> Tuple[Dict[str, Any], str]:
    """
    Decide OCR vs PHOTO with cheap signals captured during fetch/convert.
    All thresholds/weights are now configurable via ENV:
      - VLM_ROUTE_OCR_SCORE_MIN
      - VLM_ROUTE_OCR_TD_MIN
      - VLM_ROUTE_OCR_ASPECT_MIN
      - VLM_ROUTE_OCR_UI_BONUS
      - VLM_ROUTE_OCR_KW_BONUS
      - VLM_ROUTE_OCR_PNG_BONUS
      - VLM_ROUTE_OCR_PNG_BETTER_BONUS
      - VLM_ROUTE_OCR_HEAVY_TD_MIN
      - VLM_ROUTE_OCR_HEAVY_ASPECT_MIN
      - VLM_ROUTE_MONTAGE_ASPECT_MIN
      - VLM_ROUTE_MONTAGE_TD_MIN
      - VLM_ROUTE_MONTAGE_TD_MAX
      - VLM_ROUTE_MONTAGE_BYPASS_HEAVY
      - VLM_ROUTE_TALLTEXT_ENABLE
      - VLM_ROUTE_TALLTEXT_ASPECT_MIN
      - VLM_ROUTE_TALLTEXT_TD_MIN
    Signals (each contributes to score):
      - ui_like (binary, +cfg.ocr_ui_like_bonus)
      - URL keyword hit (binary, +cfg.ocr_kw_bonus)
      - MIME == image/png (soft, +cfg.ocr_png_bonus)
      - PNG compresses noticeably better than JPEG on sample (binary, +cfg.ocr_png_better_bonus)
    OCR if cfg.route_ocr_when_ui_like and gate and score >= cfg.ocr_score_min.
    """
    url = str(fetch_dbg.get("url") or "").lower()
    kw_hit = False
    try:
        for kw in (cfg.route_ocr_url_keywords or []):
            if kw and kw in url:
                kw_hit = True
                break
    except Exception:
        kw_hit = False
    ui_like = bool(fetch_dbg.get("ui_like"))
    mime = str(fetch_dbg.get("mime") or "").lower()
    # optional features
    try:
        td = float(fetch_dbg.get("text_density") or 0.0)
        text_density = td if 0.0 <= td <= 1.0 else 0.0
    except Exception:
        text_density = 0.0
    try:
        cmp = fetch_dbg.get("png_vs_jpeg") or {}
        png_better = bool(cmp.get("png_better"))
    except Exception:
        png_better = False
    # aspect ratio
    aspect = None
    try:
        width = fetch_dbg.get("width")
        height = fetch_dbg.get("height")
        if width and height and width > 0:
            aspect = height / width
        else:
            aspect = None
    except Exception:
        aspect = None

    # Fast path: very tall UI-like text screenshot -> OCR
    talltext_candidate = (
        bool(cfg.talltext_enable)
        and aspect is not None
        and aspect >= cfg.talltext_aspect_min
        and ui_like
        and (text_density >= cfg.talltext_td_min)
    )
    if talltext_candidate:
        if "router_features" not in fetch_dbg or not isinstance(fetch_dbg.get("router_features"), dict):
            fetch_dbg["router_features"] = {}
        fetch_dbg["router_features"].update({
            "talltext": True,
            "heavy_pass": False,
        })
        return VLM_SYSTEM_OCR_MSG, "ocr"
    
    # Detect tall subtitle montage (stacked frames with repeating subtitles)
    montage_candidate = (
        aspect is not None
        and aspect >= cfg.montage_aspect_min
        and (cfg.montage_td_min <= text_density <= cfg.montage_td_max)
        and (not ui_like)
    )

    if montage_candidate and cfg.montage_bypass_heavy:
        if "router_features" not in fetch_dbg or not isinstance(fetch_dbg.get("router_features"), dict):
            fetch_dbg["router_features"] = {}
        fetch_dbg["router_features"].update({
            "montage": True,
            "heavy_pass": False,
        })
        return VLM_SYSTEM_PHOTO_MSG, "photo"
    # Early heavy route: configurable thresholds
    if aspect is not None and text_density >= cfg.heavy_text_density_min and aspect >= cfg.heavy_aspect_min:
        # Ensure router_features exists and set heavy_pass
        if "router_features" not in fetch_dbg or not isinstance(fetch_dbg["router_features"], dict):
            fetch_dbg["router_features"] = {}
        fetch_dbg["router_features"]["heavy_pass"] = True
        return VLM_SYSTEM_OCR_MSG, "ocr"
    # score and gates (all configurable)
    score = 0.0
    if ui_like: score += cfg.ocr_ui_like_bonus
    if kw_hit: score += cfg.ocr_kw_bonus
    if mime == "image/png": score += cfg.ocr_png_bonus
    if png_better: score += cfg.ocr_png_better_bonus
    text_gate = (text_density >= cfg.ocr_text_density_min)
    aspect_gate = (aspect is not None and aspect >= cfg.ocr_aspect_min)
    gate = text_gate and (ui_like or kw_hit or aspect_gate)
    # expose features for logging/upstream inspection
    fetch_dbg["router_features"] = {
        "ui_like": ui_like,
        "kw_hit": kw_hit,
        "mime": mime,
        "png_better": png_better,
        "text_density": round(text_density, 3),
        "score": round(score, 2),
        "heavy_pass": False,
        "talltext": talltext_candidate,
        "montage": montage_candidate,
        "score_components": {
            "ui_like": ui_like,
            "kw_hit": kw_hit,
            "mime_png": (mime == "image/png"),
            "png_better": png_better,
        },
        "text_gate": text_gate,
        "aspect_gate": aspect_gate,
        "gate": gate,
        "thresholds": {
            "score_min": cfg.ocr_score_min,
            "td_min": cfg.ocr_text_density_min,
            "aspect_min": cfg.ocr_aspect_min,
            "montage_aspect_min": cfg.montage_aspect_min,
            "montage_td_min": cfg.montage_td_min,
            "montage_td_max": cfg.montage_td_max,
            "talltext_aspect_min": cfg.talltext_aspect_min,
            "talltext_td_min": cfg.talltext_td_min,
        },
    }
    # Final decision: require both a text density gate and a minimal score
    if cfg.route_ocr_when_ui_like and gate and score >= cfg.ocr_score_min:
        return VLM_SYSTEM_OCR_MSG, "ocr"
    return VLM_SYSTEM_PHOTO_MSG, "photo"

# --- SQL constants (module-level to avoid redefining on each run) -----------
SQL_PICK_AND_LOCK_JOBS = """
WITH ranked AS (
  SELECT
    j2.post_id,
    j2.url_hash,
    j2.priority,
    j2.next_attempt_at,
    ROW_NUMBER() OVER (PARTITION BY j2.post_id ORDER BY j2.url_hash ASC)  AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY j2.post_id ORDER BY j2.url_hash DESC) AS rn_desc
  FROM media_enrichment_jobs j2
  WHERE j2.status = 'queued'
    AND j2.next_attempt_at <= NOW()
),
-- Pick at most two images per post (first/last by url_hash) among queued & due, no dependency on post_images
candidates AS (
  SELECT j2.post_id, j2.url_hash
    FROM media_enrichment_jobs j2
    JOIN ranked r
      ON r.post_id = j2.post_id
     AND r.url_hash = j2.url_hash
   WHERE (r.rn_asc = 1 OR r.rn_desc = 1)
   ORDER BY j2.priority, j2.next_attempt_at
   FOR UPDATE OF j2 SKIP LOCKED
   LIMIT %s
)
UPDATE media_enrichment_jobs j
   SET status    = 'processing',
       locked_at = NOW(),
       locked_by = %s,
       updated_at= NOW()
  FROM candidates c
 WHERE j.post_id = c.post_id
   AND j.url_hash = c.url_hash
RETURNING j.post_id, j.url_hash, j.image_url
"""

SQL_UPSERT_POST_IMAGE_ENRICHMENT = """
INSERT INTO post_image_enrichment
  (post_id, url_hash, image_url, model, version, caption, labels, ocr_lines, safety, objects, colors, embedding, enriched_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NOW())
ON CONFLICT (post_id, url_hash) DO UPDATE SET
  caption   = EXCLUDED.caption,
  labels    = EXCLUDED.labels,
  ocr_lines = EXCLUDED.ocr_lines,
  safety    = EXCLUDED.safety,
  objects   = EXCLUDED.objects,
  colors    = EXCLUDED.colors,
  enriched_at = NOW()
"""

SQL_MARK_JOB_DONE = """
UPDATE media_enrichment_jobs
   SET status='done',
       updated_at=NOW(),
       finished_at=NOW()
 WHERE post_id=%s AND url_hash=%s
"""

SQL_MARK_JOB_ERROR = """
UPDATE media_enrichment_jobs
   SET status='error',
       attempts = COALESCE(attempts,0) + 1,
       last_error = %s,
       next_attempt_at = NULL,
       finished_at = NOW(),
       updated_at = NOW(),
       locked_at = NULL,
       locked_by = NULL
 WHERE post_id=%s AND url_hash=%s
"""

SQL_ROLLUP_AFFECTED_POSTS = """
WITH changed AS (
  SELECT UNNEST(%s::text[]) AS post_id
),
caps AS (
  SELECT
    pie.post_id,
    NULLIF(STRING_AGG(NULLIF(TRIM(pie.caption), ''), ' | ' ORDER BY pie.enriched_at DESC, pie.url_hash), '') AS image_summary
  FROM post_image_enrichment AS pie
  JOIN changed USING (post_id)
  GROUP BY pie.post_id
),
labels_exploded AS (
  SELECT
    pie.post_id,
    TRIM(LOWER(jsonb_array_elements_text(COALESCE(pie.labels, '[]'::jsonb)))) AS label
  FROM post_image_enrichment AS pie
  JOIN changed USING (post_id)
),
kw AS (
  SELECT
    post_id,
    COALESCE(jsonb_agg(DISTINCT label) FILTER (WHERE label <> ''), '[]'::jsonb) AS image_keywords
  FROM labels_exploded
  GROUP BY post_id
),
ocr_raw AS (
  SELECT
    pie.post_id,
    TRIM(elt::text, '\"') AS phrase
  FROM post_image_enrichment AS pie
  JOIN changed USING (post_id)
  CROSS JOIN LATERAL jsonb_array_elements(COALESCE(pie.ocr_lines, '[]'::jsonb)) AS elt
),
ocr_norm AS (
  SELECT
    post_id,
    TRIM(phrase) AS phrase
  FROM ocr_raw
  WHERE TRIM(phrase) <> ''
),
ocr_ranked AS (
  SELECT
    post_id, phrase, COUNT(*) AS cnt,
    ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY COUNT(*) DESC, phrase ASC) AS rn
  FROM ocr_norm
  GROUP BY post_id, phrase
),
ocr_agg AS (
  SELECT
    post_id,
    CASE
      WHEN COUNT(*) = 0 THEN NULL
      ELSE STRING_AGG(phrase, ' | ' ORDER BY rn)
    END AS image_ocr
  FROM ocr_ranked
  WHERE rn <= 5
  GROUP BY post_id
),
safety AS (
  SELECT
    pie.post_id,
    AVG(CASE WHEN jsonb_typeof(pie.safety->'nsfw') = 'number' THEN (pie.safety->>'nsfw')::float END)     AS nsfw_avg,
    MAX(CASE WHEN jsonb_typeof(pie.safety->'nsfw') = 'number' THEN (pie.safety->>'nsfw')::float END)     AS nsfw_max,
    AVG(CASE WHEN jsonb_typeof(pie.safety->'violence') = 'number' THEN (pie.safety->>'violence')::float END) AS violence_avg,
    MAX(CASE WHEN jsonb_typeof(pie.safety->'violence') = 'number' THEN (pie.safety->>'violence')::float END) AS violence_max
  FROM post_image_enrichment AS pie
  JOIN changed USING (post_id)
  GROUP BY pie.post_id
),
safety_json AS (
  SELECT
    post_id,
    jsonb_build_object(
      'nsfw',     jsonb_build_object('avg', COALESCE(nsfw_avg, 0), 'max', COALESCE(nsfw_max, 0)),
      'violence', jsonb_build_object('avg', COALESCE(violence_avg, 0), 'max', COALESCE(violence_max, 0))
    ) AS image_safety
  FROM safety
),
objs_exploded AS (
  SELECT
    pie.post_id,
    TRIM(LOWER(COALESCE((elem->>'name')::text, ''))) AS name,
    GREATEST(1, COALESCE((elem->>'count')::int, 1)) AS cnt,
    NULLIF((elem->>'confidence')::float, NULL) AS conf
  FROM (
    SELECT * FROM post_image_enrichment AS pie
    WHERE pie.objects IS NOT NULL AND pie.objects <> '[]'::jsonb
  ) AS pie
  JOIN changed USING (post_id)
  CROSS JOIN LATERAL jsonb_array_elements(pie.objects) AS elem
),
objs_norm AS (
  SELECT post_id, name, cnt, conf FROM objs_exploded WHERE name <> ''
),
objs_grouped AS (
  SELECT
    post_id,
    name,
    SUM(cnt) AS sum_cnt,
    ROUND(AVG(conf)::numeric, 3) AS avg_conf
  FROM objs_norm
  GROUP BY post_id, name
),
objs_ranked AS (
  SELECT
    post_id,
    name,
    sum_cnt,
    avg_conf,
    row_number() OVER (PARTITION BY post_id ORDER BY sum_cnt DESC, name ASC) AS rn
  FROM objs_grouped
),
objs_agg AS (
  SELECT
    post_id,
    jsonb_agg(
      jsonb_strip_nulls(
        jsonb_build_object(
          'name', name,
          'count', sum_cnt,
          'confidence', avg_conf
        )
      )
      ORDER BY sum_cnt DESC, name ASC
    ) AS image_objects
  FROM objs_ranked
  WHERE rn <= 20
  GROUP BY post_id
),
cols_exploded AS (
  SELECT
    pie.post_id,
    NULLIF(TRIM(LOWER(elem->>'name')), '') AS name,
    NULLIF(TRIM(LOWER(elem->>'hex')), '')  AS hex,
    GREATEST(0.0, LEAST(1.0, COALESCE((elem->>'ratio')::float, 0))) AS ratio
  FROM (
    SELECT * FROM post_image_enrichment AS pie
    WHERE pie.colors IS NOT NULL AND pie.colors <> '[]'::jsonb
  ) AS pie
  JOIN changed USING (post_id)
  CROSS JOIN LATERAL jsonb_array_elements(pie.colors) AS elem
),
cols_keyed AS (
  SELECT post_id,
         COALESCE(hex, name, '') AS key,
         name, hex, ratio
  FROM cols_exploded
  WHERE COALESCE(hex, name, '') <> ''
),
cols_grouped AS (
  SELECT
    post_id,
    MAX(name) AS name,
    MAX(hex) AS hex,
    SUM(ratio) AS sum_ratio
  FROM cols_keyed
  GROUP BY post_id, key
),
cols_ranked AS (
  SELECT
    post_id,
    name,
    hex,
    sum_ratio,
    row_number() OVER (PARTITION BY post_id ORDER BY sum_ratio DESC, name NULLS LAST) AS rn
  FROM cols_grouped
),
cols_agg AS (
  SELECT
    post_id,
    jsonb_agg(
      jsonb_strip_nulls(
        jsonb_build_object(
          'name', name,
          'hex', hex,
          'ratio', ROUND(sum_ratio::numeric, 3)
        )
      )
      ORDER BY sum_ratio DESC
    ) AS image_colors
  FROM cols_ranked
  WHERE rn <= 10
  GROUP BY post_id
)
INSERT INTO post_enrichment (post_id, image_summary, image_keywords, image_ocr, image_safety, image_objects, image_colors, enriched_at)
SELECT
  p.post_id,
  c.image_summary,
  k.image_keywords,
  o.image_ocr,
  s.image_safety,
  COALESCE(ob.image_objects, '[]'::jsonb),
  COALESCE(co.image_colors, '[]'::jsonb),
  NOW()
FROM changed AS p
LEFT JOIN caps         AS c  USING (post_id)
LEFT JOIN kw           AS k  USING (post_id)
LEFT JOIN ocr_agg      AS o  USING (post_id)
LEFT JOIN safety_json  AS s  USING (post_id)
LEFT JOIN objs_agg     AS ob USING (post_id)
LEFT JOIN cols_agg     AS co USING (post_id)
ON CONFLICT (post_id) DO UPDATE SET
  image_summary = COALESCE(EXCLUDED.image_summary, post_enrichment.image_summary),
  image_keywords = COALESCE(EXCLUDED.image_keywords, post_enrichment.image_keywords),
  image_ocr = COALESCE(EXCLUDED.image_ocr, post_enrichment.image_ocr),
  image_safety = COALESCE(EXCLUDED.image_safety, post_enrichment.image_safety),
  image_objects = COALESCE(EXCLUDED.image_objects, post_enrichment.image_objects),
  image_colors = COALESCE(EXCLUDED.image_colors, post_enrichment.image_colors),
  enriched_at = NOW()
WHERE post_enrichment.image_summary IS DISTINCT FROM EXCLUDED.image_summary
   OR post_enrichment.image_keywords IS DISTINCT FROM EXCLUDED.image_keywords
   OR post_enrichment.image_ocr IS DISTINCT FROM EXCLUDED.image_ocr
   OR post_enrichment.image_safety IS DISTINCT FROM EXCLUDED.image_safety
   OR post_enrichment.image_objects IS DISTINCT FROM EXCLUDED.image_objects
   OR post_enrichment.image_colors IS DISTINCT FROM EXCLUDED.image_colors;
"""

def _process_one_vlm_job(
    context,
    cur,
    client,
    cfg: VLMConfig,
    post_id: str,
    url_hash: str,
    image_url: str,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Process a single VLM job end-to-end. Returns (ok, preview, changed_post_id).
    Side effects: writes enrichment rows and marks job done/error.
    """
    t0 = _dt.now()
    stage = "start"
    job_ctx = {"post_id": post_id, "url_hash": url_hash}
    preview_len = cfg.preview_resp
    try:
        stage = "img_fetch"
        data_url, fetch_dbg = _img_to_data_url(image_url, cfg)
        _log(context, "info", "[VLM][IMGFETCH]", fetch_dbg, preview_len)

        mime = fetch_dbg.get("mime") or ""

        ok, meta = _validate_data_url(data_url)
        if not ok:
            _log(context, "warning", "[VLM][IMG] invalid data URL:", meta, preview_len)
            raise dg.Failure(description=f"VLM requires data URL; {meta}")
        else:
            _log(context, "debug", "[VLM][IMGCHK]", meta, preview_len)

        prompt_user = VLM_USER_TMPL(data_url)
        system_msg, route = _select_vlm_prompt(fetch_dbg, cfg)
        messages = [
            system_msg,
            {"role": "user", "content": prompt_user},
        ]

        max_toks = 2000 if route == "photo" else 10000
        fmt = _make_response_format(cfg)
        req_log = {
            "model": cfg.model,
            "post_id": post_id,
            "url_hash": url_hash,
            "image_mode": "data-url",
            "mime": mime,
            "image_url": image_url,
            "prompt_preview": _trim(_json.dumps(prompt_user, ensure_ascii=False), cfg.preview_prompt),
            "route": route,
            "router_features": fetch_dbg.get("router_features"),
            "max_tokens": max_toks,
            "response_format_type": (fmt.get("type") if isinstance(fmt, dict) else str(fmt)),
            "strict_json": bool(getattr(cfg, "strict_json", False)),
        }
        _log(context, "info", "[VLM][REQ]", req_log, preview_len)

        stage = "openai_call"
        resp = client.chat.completions.create(
            model=cfg.model,
            temperature=0,
            response_format=fmt,
            messages=messages,
            max_tokens=max_toks,
        )

        if not getattr(resp, "choices", None):
            err_obj = _extract_error_from_response(resp)
            preview = _resp_preview(resp, preview_len)
            _log(context, "warning", "[VLM][ERRRESP]", {"error": err_obj, "raw": preview}, preview_len)
            raise dg.Failure(description="VLM 응답 오류: choices missing or empty")

        stage = "parse_json"
        try:
            content = resp.choices[0].message.content if resp and resp.choices else None
        except Exception:
            content = None

        result = _parse_and_normalize_vlm(str(content or ""), job_ctx, context, preview_len)

        caption   = result.caption
        labels    = result.labels
        ocr_lines = result.ocr_lines
        safety    = result.safety
        norm_objs = result.objects
        norm_cols = result.colors

        if (not ocr_lines) and any(x in (labels or []) for x in ["뉴스","신문","기사","블로그","커뮤니티","글","캡처","웹 문서"]):
            _log(context, "warning", "[VLM][OCR-MISSING]", {**job_ctx, "labels": labels}, preview_len)
        ocr_total_len = sum(len(x) for x in (ocr_lines or []))
        if ocr_total_len > 20000:
            _log(context, "warning", "[VLM][OCR-LONG]", {**job_ctx, "len": ocr_total_len}, preview_len)

        stage = "db_upsert"
        cur.execute(
            SQL_UPSERT_POST_IMAGE_ENRICHMENT,
            (post_id, url_hash, image_url, cfg.model, 2, caption, PgJson(labels), PgJson(ocr_lines), PgJson(safety), PgJson(norm_objs), PgJson(norm_cols))
        )
        _log(context, "debug", "[VLM][UPSERT]", {**job_ctx,
            "caption_len": len(caption or ""),
            "labels": len(labels),
            "ocr_lines": len(ocr_lines or []),
            "objects": len(norm_objs),
            "colors": len(norm_cols),
            "rowcount": getattr(cur, "rowcount", None)
        }, preview_len)

        stage = "db_mark_done"
        cur.execute(SQL_MARK_JOB_DONE, (post_id, url_hash))
        dur_ms = int((_dt.now() - t0).total_seconds() * 1000)
        _log(context, "info", "[VLM][DONE]", {**job_ctx, "duration_ms": dur_ms}, preview_len)

        preview_str = _trim(str(content or ""), preview_len)
        return True, preview_str, post_id

    except Exception as e:
        _log(context, "error", "[VLM][EXC]", {**job_ctx, "stage": stage, "error": _exc_dict(e)}, preview_len)
        cur.execute(
            SQL_MARK_JOB_ERROR,
            (f"{stage}: {type(e).__name__}: {str(e)[:400]}", post_id, url_hash)
        )
        return False, None, None

# VLM 처리 워커: media_enrichment_jobs 큐를 SKIP LOCKED로 소비하여 per-image 분석 결과를 저장
@dg.asset(
    name="vlm_worker_asset",
    group_name="IS",
    automation_condition=ON3,
    tags={
        "data_tier": "gold",
        "table": "post_image_enrichment"
    },
    description="VLM 처리 워커 media_enrichment_jobs 큐를 SKIP LOCKED로 소비하여 per image 분석 결과를 저장"
)
def vlm_worker_asset(
    context,
    is_postgres: PostgresResource,
    qwen25: OpenAIResource,
    promote_p0_from_frontpage_asset,  # 프론트 노출 글의 VLM 잡 승격과 시각적 연결
):
    """
    - 주기(2분)마다 queued 잡을 소량 배치로 가져와 처리
    - 실패: Fast-Fail 처리(재시도/백오프 없음) — DB에는 status='error'로 기록
    - 결과는 post_image_enrichment에 upsert
    - 모델 응답은 JSON만 허용(파서 안전)
    - 엔진: 기본 Qwen2.5-VL-7B-Instruct (ENV VLM_MODEL로 변경 가능)
    - 호출: OpenAI/Compatible Chat Completions SDK(Resource), 이미지는 data:URL(base64)로 전달
    """
    # --- Runtime & worker params ---
    # BATCH = 4. #omen
    BATCH = int(os.getenv("VLM_BATCH", "33"))  # 3080ti; override with env VLM_BATCH
    WORKER_NAME = "vlm_worker"

    # --- Config (ENV bundle) ---
    cfg = VLMConfig.from_env()

    with qwen25.get_client(context) as client, is_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 1) 잡 잠금(배치)
            _log(context, "info", "[VLM][CFG]", {
                "TARGET_FORMAT": cfg.target_format,
                "FORCE_JPEG": cfg.force_jpeg,
                "IMAGE_MAX_BYTES": cfg.image_max_bytes,
                "JPEG_QUALITY": cfg.jpeg_quality,
                "PNG_COLOR_THRESHOLD": cfg.png_color_threshold,
                "MODEL": cfg.model,
                "PREVIEW_PROMPT": cfg.preview_prompt,
                "PREVIEW_RESP": cfg.preview_resp,
                "STRICT_JSON": cfg.strict_json,
                "ROUTE_OCR_SCORE_MIN": cfg.ocr_score_min,
                "ROUTE_OCR_TD_MIN": cfg.ocr_text_density_min,
                "ROUTE_OCR_ASPECT_MIN": cfg.ocr_aspect_min,
                "ROUTE_OCR_UI_BONUS": cfg.ocr_ui_like_bonus,
                "ROUTE_OCR_KW_BONUS": cfg.ocr_kw_bonus,
                "ROUTE_OCR_PNG_BONUS": cfg.ocr_png_bonus,
                "ROUTE_OCR_PNG_BETTER_BONUS": cfg.ocr_png_better_bonus,
                "ROUTE_OCR_HEAVY_TD_MIN": cfg.heavy_text_density_min,
                "ROUTE_OCR_HEAVY_ASPECT_MIN": cfg.heavy_aspect_min,
                "ROUTE_MONTAGE_ASPECT_MIN": cfg.montage_aspect_min,
                "ROUTE_MONTAGE_TD_MIN": cfg.montage_td_min,
                "ROUTE_MONTAGE_TD_MAX": cfg.montage_td_max,
                "ROUTE_MONTAGE_BYPASS_HEAVY": cfg.montage_bypass_heavy,
                "ROUTE_TALLTEXT_ENABLE": cfg.talltext_enable,
                "ROUTE_TALLTEXT_ASPECT_MIN": cfg.talltext_aspect_min,
                "ROUTE_TALLTEXT_TD_MIN": cfg.talltext_td_min,
            }, cfg.preview_resp)
            cur.execute(SQL_PICK_AND_LOCK_JOBS, (BATCH, WORKER_NAME))
            jobs = cur.fetchall()
            _log(context, "info", "[VLM][PICKED]", {"count": len(jobs), "sample": jobs[:5]}, cfg.preview_resp)
            if not jobs:
                conn.commit()
                context.add_output_metadata({
                    "picked": 0,
                    "done": 0,
                    "failed": 0,
                    "rolled_up_post_ids": [],
                    "vlm_resp_previews": [],
                })
                return {"picked": 0, "done": 0, "failed": 0}

            processed = 0; failed = 0
            resp_previews = []
            changed_post_ids: Set[str] = set()
            for post_id, url_hash, image_url in jobs:
                ok, preview, changed_id = _process_one_vlm_job(context, cur, client, cfg, post_id, url_hash, image_url)
                if ok:
                    processed += 1
                    if changed_id:
                        changed_post_ids.add(changed_id)
                    if preview:
                        resp_previews.append({"post_id": post_id, "url_hash": url_hash, "resp": preview})
                else:
                    failed += 1

            # 6.5) Incremental rollup for affected posts (Option A)
            if changed_post_ids:
                cur.execute(SQL_ROLLUP_AFFECTED_POSTS, (list(changed_post_ids),))
            conn.commit()
            # Emit a compact run summary (subset shown in Dagster asset metadata)
            context.add_output_metadata({
                "picked": len(jobs),
                "done": processed,
                "failed": failed,
                "rolled_up_post_ids": sorted(list(changed_post_ids))[:50],
                "vlm_resp_previews": resp_previews[:5],
            })
            if failed > 0:
                raise dg.Failure(
                    description=f"[vlm_worker_asset] 작업 일부 실패: {failed}/{len(jobs)}",
                    metadata={
                        "picked": len(jobs),
                        "done": processed,
                        "failed": failed,
                        "rolled_up_post_ids": sorted(list(changed_post_ids))[:50],
                        "vlm_resp_previews": resp_previews[:5],
                    },
                )
            return {"picked": len(jobs), "done": processed, "failed": failed}
            