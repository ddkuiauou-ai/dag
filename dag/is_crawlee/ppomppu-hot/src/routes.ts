import {
    createPlaywrightRouter,
    Dataset,
    KeyValueStore,
} from "crawlee";
import { createHash } from "node:crypto";
import * as cheerio from 'cheerio';
import iconv from 'iconv-lite';

/****
 * ---------------------------------------------------------------------------
 * PPOMPPU Hot Board Router (std-optimized)
 *
 * 역할
 *  • 게시판별 **셀렉터/규칙**을 모아 Playwright 라우터(LIST/DETAIL)를 제공합니다.
 *  • HTML 구조가 비슷한 게시판은 `BoardConfig`만 교체하면 동일 라우터를 재사용합니다.
 *
 *
 * 라우터 개요
 *  1) LIST: 목록 페이지에서 상세 URL을 수집하고 페이지네이션을 진행합니다.
 *     - incremental: 이전 실행의 마지막 postId까지 도달하면 중단
 *     - full: 목록상의 시간 힌트가 cutoff 이전이면 중단
 *  2) DETAIL: 상세 페이지에서 메타/본문/이미지/임베드/댓글을 수집합니다.
 *     - 본문 내부의 상대경로를 절대경로로 치환
 *     - YouTube/X(트위터) 임베드 메타 수집
 *     - 댓글 트리를 평면화하여 개수 산출(최종 개수 계산은 main.ts)
 *
 * 주요 유틸 요약
 *  • `parseKstToUtcIso` : 사이트에서 사용하는 다양한 KST 시간을 **엄격한 규칙**으로 파싱
 *  • `absolutizeContentUrls` : 본문 내 src/href/srcset를 **절대 URL**로 치환
 *  • `getYouTubeInfo` : oEmbed + 썸네일 HEAD 체크로 제목/썸네일을 빠르게 확보(LRU+KV 캐시)
 *
 * 변경 이력(리팩토링)
 *  • 사용되지 않는 `extractInt` 유틸 제거(출력/동작에 영향 없음)
 *  • 함수/섹션 주석을 JSDoc 형태로 보강하여 읽기 흐름 개선
 * ---------------------------------------------------------------------------
 */

// ===[ 고정 상수 ]============================================================
const HASH_LEN_POST_ID = 16;
const KST_OFFSET_MIN = 9 * 60; // KST(UTC+9)
const YT_ID_RE = /(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})/

// ===[ 타입 정의 ]============================================================
/** 댓글 구조 */
export type Comment = {
    id: string;
    author: string | null;
    content: string | null;
    timestamp: string; // UTC ISO
    isReply: boolean;
    replies: Comment[];
    likeCount?: number;
    dislikeCount?: number;
    reactionCount?: number;
    // 추가 메타
    raw?: string | null;
    avatar?: string | null;
    html?: string | null;
    depth?: number;
    parentId?: string | null;
};

/** 임베드 메타 */
export type Embedded = {
    type: string; // youtube, twitter, iframe, etc.
    url: string;
    provider?: string | null;
    videoId?: string | null;
    thumbnail?: string | null;
    title?: string | null;
    description?: string | null;
    mimeType?: string | null; // MIME 타입 (예: video/mp4, application/pdf 등)
};

/** 게시판 셀렉터/규칙 */
export type BoardConfig = {
    SITE_NAME: string;
    BOARD_NAME: string;
    STATE_STORE_NAME: string; // KeyValueStore 이름
    baseListUrl: string; // 목록 첫 진입 URL
    postUrlAllow?: (u: URL, base: URL, cfg: BoardConfig) => boolean; // 상세 URL 허용 규칙(선택)
    list: {
        container: string; // 목록 래퍼
        item: string;      // 각 게시물 아이템
        postLink: string;  // 상세 링크 셀렉터
        timestampHint?: string; // 목록에서 사용되는 시간 텍스트 힌트 (옵션)
        skipNotice?: string;    // 공지 스킵
        skipPromo?: string;     // 홍보/광고 스킵
        pageParam?: string;     // 페이지 파라미터명 (기본: page)
        nextPageSelectors?: string[]; // 다음 페이지로 이동할 수 있는 셀렉터 후보들
    };
    detail: {
        content: string;         // 본문 컨테이너
        title: string;           // 제목 컨테이너
        titleCategoryLink?: string; // 카테고리 링크(제목 내부)
        author: string;          // 작성자
        metaBlock?: string;      // 작성자/시간/조회수 등 메타 정보가 포함된 블록
        authorAvatarSelectors?: string[];   // 작성자 아바타 셀렉터 후보
        commentAvatarSelectors?: string[];  // 댓글 아바타 셀렉터 후보
        timestampIsSiblingOfAuthor?: boolean; // 작성자 엘리먼트의 다음 형제에 시간 텍스트가 있는지
        timestampSelectors?: string[];      // 시간 텍스트를 직접 찾을 CSS 셀렉터 목록 (우선순위대로 시도)
        viewCount?: string;      // 조회수 텍스트 엘리먼트
        likeCount?: string;      // 추천/좋아요 텍스트 엘리먼트
        dislikeCount?: string;      // 비추천/싫어요 텍스트 엘리먼트
        // 댓글
        commentLikeCount?: string;        // 댓글 추천 수 (단일 표시)
        commentDislikeCount?: string;     // 댓글 비추천 수 (단일 표시)
        commentsContainer?: string;     // 댓글 래퍼
        commentArticle?: string;        // 댓글 아이템
        commentAuthor?: string;         // 댓글 작성자 셀렉터
        commentTime?: string;           // 댓글 시간 셀렉터 (title 속성 또는 텍스트)
        commentContentHtml?: string;    // 댓글 콘텐츠 HTML 셀렉터
        commentRawTextareaPrefix?: string; // 대댓글 raw 텍스트 영역 ID prefix
        // 태그/임베드/이미지
        tagSelectors?: string[];
        imageSelectors?: string[];
        iframeSelectors?: string[];
        twitterSelectors?: string[];
        restrictedTextProbes?: string[]; // 제한/삭제 여부 감지용 텍스트가 들어있는 범위
        restrictedTextRegex?: (string | RegExp)[]; // 사이트 전용 제한/삭제 패턴
    };
};

// ===[ 보조 유틸 ]===========================================================
/* ── oEmbed 캐시 (메모리 LRU + KVStore) ───────────────────────────── */
const _embedStorePromise = KeyValueStore.open('embedCache');
// 실행 중 사용할 소형 LRU (최근 요청 우선) – 메모리 제한 간단 적용(최대 500)
const _ytLru = new Map<string, Promise<{ title: string | null }>>();
function _capLRU(map: Map<any, any>, max = 500) {
    if (map.size > max) {
        const first = map.keys().next().value;
        map.delete(first);
    }
}

async function getYouTubeTitle(videoId: string): Promise<string | null> {
    if (_ytLru.has(videoId)) return (await _ytLru.get(videoId)!).title;

    const fetchJob = (async () => {
        const store = await _embedStorePromise;
        const kvKey = 'yt-' + videoId; // KV 키 규칙
        const kvCached = await store.getValue<{ title: string | null }>(kvKey);
        if (kvCached) return kvCached;

        const yUrls = [
            `https://www.youtube.com/oembed?url=https://youtu.be/${videoId}&format=json`,
            `https://noembed.com/embed?url=https://youtu.be/${videoId}`,
        ];

        for (const api of yUrls) {
            try {
                const res = await fetch(api, { headers: { 'Accept': 'application/json' } });
                if (!res.ok) continue;
                const data: any = await res.json();
                if (typeof data.title === 'string' && data.title.trim()) {
                    const payload = { title: data.title.trim() };
                    await store.setValue(kvKey, payload);
                    return payload;
                }
            } catch { /* try next */ }
        }

        await store.setValue(kvKey, { title: null });
        return { title: null };
    })();

    _ytLru.set(videoId, fetchJob);
    _capLRU(_ytLru);
    return (await fetchJob).title;
}

/* ── YouTube 임베드 정보(title·썸네일) 병렬 수집 ───────────────────── */
async function getYouTubeInfo(videoId: string): Promise<{ title: string | null; thumbnail: string }> {
    let thumbnail = `https://img.youtube.com/vi/${videoId}/hqdefault.jpg`;

    const [titleResult] = await Promise.allSettled([
        getYouTubeTitle(videoId),
        (async () => {
            const ctrl = new AbortController();
            const t = setTimeout(() => ctrl.abort(), 800);
            try {
                const resp = await fetch(`https://img.youtube.com/vi/${videoId}/maxresdefault.jpg`, { method: 'HEAD', signal: ctrl.signal });
                clearTimeout(t);
                if (resp.ok) thumbnail = resp.url;
            } catch { /* stick with hqdefault */ }
            finally {
                clearTimeout(t);
            }
        })(),
    ]);

    const title = (titleResult.status === 'fulfilled') ? titleResult.value : null;
    return { title, thumbnail };
}


function toAbsoluteUrl(base: string, url: string | null | undefined): string | null {
    if (!url) return null;
    const u = url.trim();
    if (!u) return null;
    if (u.startsWith('//')) return 'https:' + u;
    return new URL(u, base).toString();
}
function uniq(arr: string[]): string[] {
    const s = new Set(arr.filter(Boolean));
    return [...s];
}
function normalizeAbsolute(base: string, arr: Array<string | null | undefined>): string[] {
    const out: string[] = [];
    for (const u of arr) {
        const abs = toAbsoluteUrl(base, (u || '').trim());
        if (abs) out.push(abs);
    }
    return uniq(out);
}

// Serialize Cheerio document back to fragment (no <html>/<body> wrapper)
function _toHtmlFragment($: cheerio.CheerioAPI): string {
    const body = $('body');
    if (body.length) return (body.html() || '').trim();
    const nodes = $.root().children().toArray();
    return nodes.map((n) => $.html(n)).join('').trim();
}

/**
 * Rewrite all relative src/href/srcset references inside an HTML fragment
 * into absolute URLs using `baseUrl`.
 * - 이 단계는 **렌더링 의존 없이** 결과 JSON의 `contentHtml` 일관성을 보장합니다.
 * - 링크/이미지/스크립트 등 주요 리소스 속성을 폭넓게 커버합니다.
 */
function absolutizeContentUrls(html: string, baseUrl: string): string {
    if (!html || !html.trim()) return html || '';
    const $ = cheerio.load(html);

    // Convert src/href
    $('img[src], a[href], source[src], video[src], audio[src], track[src], iframe[src], link[href], script[src]').each((_, el) => {
        const $el = $(el);
        const attr = $el.is('a, link') ? 'href' : 'src';
        const val = ($el.attr(attr) || '').trim();
        if (!val) return;
        const abs = toAbsoluteUrl(baseUrl, val);
        if (abs) $el.attr(attr, abs);
    });

    // Handle srcset
    $('img[srcset], source[srcset]').each((_, el) => {
        const $el = $(el);
        const srcset = ($el.attr('srcset') || '').trim();
        if (!srcset) return;
        const parts = srcset.split(',').map(s => s.trim()).filter(Boolean);
        const mapped = parts.map(p => {
            const [u, d] = p.split(/\s+/, 2);
            const abs = toAbsoluteUrl(baseUrl, (u || '').trim());
            return abs ? (d ? `${abs} ${d}` : abs) : p;
        });
        if (mapped.length) $el.attr('srcset', mapped.join(', '));
    });

    // Handle common lazy-loading patterns (data-original / data-src)
    $('img[data-original], img[data-src]').each((_, el) => {
        const $el = $(el);
        const dataOriginal = ($el.attr('data-original') || '').trim();
        const dataSrc = ($el.attr('data-src') || '').trim();
        const candidate = dataOriginal || dataSrc;
        if (!candidate) return;
        const abs = toAbsoluteUrl(baseUrl, candidate);
        if (abs) {
            // Upgrade placeholder src (e.g., //cdn2.../images/lazyloading.jpg) to the real source
            const curSrc = ($el.attr('src') || '').trim();
            if (!curSrc || /\/images\/lazyloading\.jpg(?:$|\?)/i.test(curSrc)) {
                $el.attr('src', abs);
            }
            // Also absolutize the data-* attributes themselves
            if (dataOriginal) $el.attr('data-original', abs);
            if (dataSrc) $el.attr('data-src', abs);
        }
    });

    return _toHtmlFragment($);
}

/**
 * Remove tracking pixels from an HTML fragment.
 * Rules:
 *  - <img> with width/height <= 1 (attributes)
 *  - styles that hide or make it 1px (display:none / visibility:hidden / opacity:0 / width/height:1px)
 *  - known tracking hosts (cr*.shopping.naver.com, search.naver.com)
 */
function sanitizeTrackingPixels(html: string): string {
    if (!html || !html.trim()) return html || '';
    const $ = cheerio.load(html);

    $('img').each((_, el) => {
        const $el = $(el);
        const src = ($el.attr('src') || '').trim();
        const widthAttr = parseInt(($el.attr('width') || '').trim(), 10);
        const heightAttr = parseInt(($el.attr('height') || '').trim(), 10);
        const style = (($el.attr('style') || '').trim()).toLowerCase();

        const hiddenByStyle = /display\s*:\s*none|visibility\s*:\s*hidden|opacity\s*:\s*0/.test(style);
        const tinyByAttrs = (Number.isFinite(widthAttr) && widthAttr <= 1) || (Number.isFinite(heightAttr) && heightAttr <= 1);
        const tinyByStyle = /\bwidth\s*:\s*1px\b|\bheight\s*:\s*1px\b/.test(style);

        let suspiciousHost = false;
        try {
            // tolerate relative paths by supplying a dummy base
            const u = new URL(src, 'https://dummy.local');
            suspiciousHost = /(?:^|\.)cr\d*\.shopping\.naver\.com$/i.test(u.hostname)
                || /^search\.naver\.com$/i.test(u.hostname);
        } catch { /* ignore malformed src */ }

        if (hiddenByStyle || tinyByAttrs || tinyByStyle || suspiciousHost) {
            $el.remove();
        }
    });

    return _toHtmlFragment($);
}

function isRestrictedOrDeleted(text: string, patterns?: (string | RegExp)[]): boolean {
    const s = (text || '').replace(/\s+/g, '');
    const pats = patterns ?? [/권한이없|삭제|존재하지|비공개/];
    return pats.some(p => (typeof p === 'string' ? new RegExp(p) : p).test(s));
}

function detectCharset(ct?: string) {
    if (!ct) return 'utf-8';
    const m = ct.match(/charset=([^;]+)/i);
    return (m?.[1] || 'utf-8').toLowerCase();
}

function isDefaultPpomAvatarUrl(src?: string | null): boolean {
    const s = (src || '').trim().toLowerCase();
    if (!s) return false;
    return /\/images\/no_face\.jpg(?:$|\?)/.test(s)
        || /\/images\/icon_reply(?:\d*)\.png(?:$|\?)/.test(s)
        || /\/images\/icon_reply\.png(?:$|\?)/.test(s);
}

async function findNickconInPage(page: import('playwright').Page): Promise<string | null> {
    try {
        const loc = page.locator('img[src*="zboard/nickcon/"]').first();
        if (await loc.count()) {
            const src = await loc.getAttribute('src');
            return (src && src.trim()) || null;
        }
    } catch { }
    return null;
}

async function findNickconInElement(el: import('playwright').ElementHandle<HTMLElement>): Promise<string | null> {
    try {
        const imgEl = await el.$('img[src*="zboard/nickcon/"]');
        if (imgEl) {
            const src = await imgEl.getAttribute('src');
            return (src && src.trim()) || null;
        }
    } catch { }
    return null;
}

async function findPpomIconInElement(el: import('playwright').ElementHandle<HTMLElement>): Promise<string | null> {
    try {
        const imgEl = await el.$('img[src*="PPOM_icon/"]');
        if (imgEl) {
            const src = await imgEl.getAttribute('src');
            return (src && src.trim()) || null;
        }
    } catch { }
    return null;
}

// Page-level PPOM_icon finder (for post avatar priority)
async function findPpomIconInPage(page: import('playwright').Page): Promise<string | null> {
    try {
        const loc = page.locator('img[src*="PPOM_icon/"]').first();
        if (await loc.count()) {
            const src = await loc.getAttribute('src');
            return (src && src.trim()) || null;
        }
    } catch { }
    return null;
}

// ===[ 시간 파서 (KST → UTC ISO) ]===========================================
function toUtcIsoFromKst(y: number, m: number, d: number, hh = 0, mm = 0, ss = 0): string {
    // KST 기준 시간을 UTC로 변환 (초 단위까지 지원)
    const dateKst = Date.UTC(y, m - 1, d, hh, mm, ss) - KST_OFFSET_MIN * 60 * 1000;
    return new Date(dateKst).toISOString();
}

function normalizeTwoDigitYear(yy: number): number {
    // Treat 00–99 as 2000–2099 to match ppomppu list hints like "25/08/27" → 2025/08/27
    if (!Number.isFinite(yy)) return yy;
    if (yy >= 0 && yy <= 99) return 2000 + yy;
    return yy;
}

/**
 * 파라미터: 게시판에서 추출한 KST 기준의 시간 문자열을 **UTC ISO**로 변환합니다.
 * 허용 포맷(엄격, fail-fast):
 *  - "HH:mm[:ss]" (오늘로 간주)
 *  - "MM.DD HH:mm" (연도는 현재/미래 판별로 보정)
 *  - "YYYY.MM.DD[ HH:mm[:ss]]"
 *  - "YYYY-MM-DD HH:mm[:ss]" (KST 기준)
 *  - "YYYY/MM/DD[ HH:mm[:ss]]"
 *  - "YY/MM/DD[ HH:mm[:ss]]" (two-digit year; 20YY assumed)
 *  - 상대시간: "N분 전", "N시간 전", 또는 blind된 숫자(분)
 *  - 명시적 ISO/RFC 2822 는 그대로 통과
 * 실패 시 에러를 발생시켜 상위에서 원인을 빠르게 확인할 수 있게 합니다.
 */
function parseKstToUtcIso(dateStr: string | null, context?: string): string {
    const ctx = context ? ` [context: ${context}]` : '';
    if (!dateStr || !dateStr.trim()) {
        throw new Error(`Timestamp parsing failed: empty or null input${ctx}`);
    }
    let s = dateStr.replace('등록', '').trim();
    let m: RegExpMatchArray | null;

    // N분 전 / N시간 전
    let rel = s.match(/^(\d{1,3})\s*분\s*전$/);
    if (rel) {
        const mins = parseInt(rel[1], 10);
        return new Date(Date.now() - mins * 60_000).toISOString();
    }
    rel = s.match(/^(\d{1,3})\s*시간\s*전$/);
    if (rel) {
        const hrs = parseInt(rel[1], 10);
        return new Date(Date.now() - hrs * 3_600_000).toISOString();
    }
    // blind 스팬으로 숫자만 남는 경우: "13" → 13분 전
    rel = s.match(/^(\d{1,3})$/);
    if (rel) {
        const mins = parseInt(rel[1], 10);
        return new Date(Date.now() - mins * 60_000).toISOString();
    }

    const nowUtc = new Date();
    const nowKst = new Date(nowUtc.getTime() + KST_OFFSET_MIN * 60 * 1000);
    const kY = nowKst.getUTCFullYear();
    const kM = nowKst.getUTCMonth() + 1;
    const kD = nowKst.getUTCDate();

    // HH:mm:ss
    m = s.match(/^(?:\s*)(\d{2}):(\d{2}):(\d{2})(?:\s*)$/);
    if (m) return toUtcIsoFromKst(kY, kM, kD, parseInt(m[1], 10), parseInt(m[2], 10), parseInt(m[3], 10));

    // HH:mm
    m = s.match(/^(?:\s*)(\d{2}):(\d{2})(?:\s*)$/);
    if (m) return toUtcIsoFromKst(kY, kM, kD, parseInt(m[1], 10), parseInt(m[2], 10));

    // MM.DD HH:mm
    m = s.match(/^(?:\s*)(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})(?:\s*)$/);
    if (m) {
        // 연도 추정(미래면 전년도)
        let y = kY;
        const month = parseInt(m[1], 10);
        const day = parseInt(m[2], 10);
        const hh = parseInt(m[3], 10);
        const mm_ = parseInt(m[4], 10);
        const asUtc = new Date(toUtcIsoFromKst(kY, month, day, hh, mm_));
        if (asUtc.getTime() > nowUtc.getTime()) y = kY - 1;
        return toUtcIsoFromKst(y, month, day, hh, mm_);
    }

    // YYYY.MM.DD
    m = s.match(/^(?:\s*)(\d{4})\.(\d{2})\.(\d{2})(?:\s*)$/);
    if (m) return toUtcIsoFromKst(parseInt(m[1], 10), parseInt(m[2], 10), parseInt(m[3], 10));

    // YYYY.MM.DD HH:mm
    m = s.match(/^(?:\s*)(\d{4})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})(?:\s*)$/);
    if (m) return toUtcIsoFromKst(
        parseInt(m[1], 10),
        parseInt(m[2], 10),
        parseInt(m[3], 10),
        parseInt(m[4], 10),
        parseInt(m[5], 10)
    );

    // YYYY/MM/DD HH:mm[:ss]
    m = s.match(/^(?:\s*)(\d{4})\/(\d{2})\/(\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?(?:\s*)$/);
    if (m) return toUtcIsoFromKst(
        parseInt(m[1], 10),
        parseInt(m[2], 10),
        parseInt(m[3], 10),
        parseInt(m[4], 10),
        parseInt(m[5], 10),
        m[6] ? parseInt(m[6], 10) : 0
    );

    // YYYY/MM/DD
    m = s.match(/^(?:\s*)(\d{4})\/(\d{2})\/(\d{2})(?:\s*)$/);
    if (m) return toUtcIsoFromKst(
        parseInt(m[1], 10),
        parseInt(m[2], 10),
        parseInt(m[3], 10)
    );

    // YY/MM/DD HH:mm[:ss]  → assume 20YY
    m = s.match(/^(?:\s*)(\d{2})\/(\d{2})\/(\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?(?:\s*)$/);
    if (m) return toUtcIsoFromKst(
        normalizeTwoDigitYear(parseInt(m[1], 10)),
        parseInt(m[2], 10),
        parseInt(m[3], 10),
        parseInt(m[4], 10),
        parseInt(m[5], 10),
        m[6] ? parseInt(m[6], 10) : 0
    );

    // YY/MM/DD → assume 20YY
    m = s.match(/^(?:\s*)(\d{2})\/(\d{2})\/(\d{2})(?:\s*)$/);
    if (m) return toUtcIsoFromKst(
        normalizeTwoDigitYear(parseInt(m[1], 10)),
        parseInt(m[2], 10),
        parseInt(m[3], 10)
    );

    // YYYY-MM-DD HH:mm[:ss] (신규 포맷, KST 기준)
    m = s.match(/^(?:\s*)(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?(?:\s*)$/);
    if (m) return toUtcIsoFromKst(
        parseInt(m[1], 10),
        parseInt(m[2], 10),
        parseInt(m[3], 10),
        parseInt(m[4], 10),
        parseInt(m[5], 10),
        m[6] ? parseInt(m[6], 10) : 0
    );

    // Accept explicit ISO / RFC 2822 only (no implicit fallbacks)
    const parsed = new Date(s);
    if (!isNaN(parsed.getTime())) return parsed.toISOString();

    throw new Error(`Timestamp parsing failed for input: "${s}"${ctx}`);
}

// ===[ 게시판 레지스트리 ]====================================================
interface BoardOptions {
    siteName: string;
    boardName: string;
    baseUrl: string;
    baseListUrl: string;
}

function makePpomppuBoard(opts: BoardOptions): BoardConfig {
    const STORE_KEY = `CRAWL_STATE_${opts.siteName.toUpperCase()}_${opts.boardName.replace(/[^a-z0-9]/gi, '_').toUpperCase()}`;
    return {
        SITE_NAME: opts.siteName,
        BOARD_NAME: opts.boardName,
        STATE_STORE_NAME: STORE_KEY,
        baseListUrl: opts.baseListUrl,
        postUrlAllow: (u, base) => {
            if (u.hostname !== base.hostname) return false;
            if (!(u.pathname === '/zboard/view.php' || u.pathname === '/zboard/zboard.php')) return false;
            const no = u.searchParams.get('no');
            return !!(no && /^\d+$/.test(no));
        },
        list: {
            container: "table.board_table tbody",
            item: "tr.baseList",
            postLink: "td.title a.baseList-title",
            timestampHint: "td.board_date",
            skipNotice: undefined,
            // 광고/스폰서 행은 타이틀 내부에 #ad-icon 이 존재함
            skipPromo: ":has(#ad-icon), :has([id*=\"ad-icon\"]), :has([class*=\"ad-icon\"]), :has([aria-label*=\"광고\"]), :has([data-ad]), :has([data-sponsored])",
            pageParam: "page",
            nextPageSelectors: [
                'a.next_page',
                'a[rel="next"]',
                'a:has-text("다음")',
                'a:has-text(">")'
            ],
        },
        detail: {
            // Support both classic board view and news article view
            content: "td.board-contents, .news_content .Lnews .main-text, .news_content .Lnews",
            title: "#topTitle h1, .news_content .Lnews h1",
            author: "li.topTitle-name img[alt], .topTitle-name .baseList-name",
            metaBlock: '#topTitle .topTitle-mainbox',
            authorAvatarSelectors: [
                "#topTitle .topTitle-profile",
                "#topTitle .topTitle-profile img",
                ".han_s > a > img"
            ],
            commentAvatarSelectors: [
                'b > a img[src*="nickcon/"]',
                'img[src*="PPOM_icon/"]',
                'td[width="100px"] img'
            ],
            commentsContainer: '#newbbs',
            commentArticle: '#newbbs div[id^="comment_"]',
            commentAuthor: 'b > a',
            commentTime: '.eng-day',
            commentContentHtml: '.mid-text-area',
            commentRawTextareaPrefix: undefined,
            tagSelectors: [],
            imageSelectors: ['td.board-contents img', '.news_content .Lnews img'],
            iframeSelectors: ['td.board-contents iframe[src]'],
            twitterSelectors: ['blockquote.twitter-tweet'],
            restrictedTextProbes: ['td.board-contents', '.news_content .Lnews'],
            restrictedTextRegex: [/권한이 없|삭제된|존재하지 않는/],
            // Add or update timestampSelectors to include the news time element
            timestampSelectors: ['.Lnews h1 time'],
        },
    };
}

export const BOARDS: Record<string, BoardConfig> = {
    hot: makePpomppuBoard({
        siteName: 'ppomppu',
        boardName: 'hot',
        baseUrl: 'https://www.ppomppu.co.kr',
        baseListUrl: 'https://www.ppomppu.co.kr/hot.php',
    }),
};

/**
 * 보드 설정(`BoardConfig`)을 받아 Crawlee Playwright 라우터를 생성합니다.
 * 반환 라우터는 두 핸들러를 가집니다:
 *  - LIST  : 목록에서 상세 링크 수집 + 페이지네이션
 *  - DETAIL: 상세의 메타/본문/첨부/임베드/댓글 수집 후 Dataset에 push
 *
 */
export function createRouter(cfg: BoardConfig, outDatasetId = 'posts') {
    const router = createPlaywrightRouter();
    // 실행(run) 단위 상태 (KVStore 대신 메모리 사용)
    let pagesCrawledInRun = 0;

    // 목록 핸들러: 링크 수집 및 페이지네이션
    router.addHandler("LIST", async ({ page, enqueueLinks, log, request }) => {
        const { mode, cutoffDate, maxPages } = request.userData;
        log.info(`목록 페이지 처리: ${page.url()} (모드: ${mode}, 보드: ${cfg.BOARD_NAME})`);

        const stateStore = await KeyValueStore.open(cfg.STATE_STORE_NAME);
        const lastStopId = mode === 'incremental' ? await stateStore.getValue("lastStopId") : null;

        // 이번 실행 동안 방문한 목록 페이지 수 카운트 (메모리)
        pagesCrawledInRun += 1;
        const pagesSoFar = pagesCrawledInRun;

        // 페이지 한도(hard max) 적용: maxPages가 유한수일 때만 제한
        const hardMax = (typeof maxPages === 'number' && isFinite(maxPages)) ? maxPages : null;
        if (hardMax !== null && pagesSoFar > hardMax) {
            log.info(`페이지 한도(${hardMax})를 초과하여 중단합니다. (pagesCrawled=${pagesSoFar})`);
            return; // 이 페이지부터는 처리하지 않음
        }

        await page.waitForSelector(cfg.list.container, { timeout: 5000 });
        const postElements = await page.$$(cfg.list.container + ' ' + cfg.list.item);
        log.info(`현재 페이지에서 ${postElements.length}개의 게시물을 찾았습니다.`);

        let newLastStopId: string | null = null;
        let shouldStop = false;
        const pageParam = cfg.list.pageParam || 'page';
        // 내구성 있는 첫 페이지 판별
        const u = new URL(page.url());
        const isFirstPage = !u.searchParams.has(pageParam) || u.searchParams.get(pageParam) === '1';

        for (const postElement of postElements) {
            // --- 스킵 규칙 ---
            if (cfg.list.skipNotice && await postElement.$(cfg.list.skipNotice)) continue;

            // 광고(AD) 표시 강건 스캔: 우선 CSS 셀렉터, 실패 시 DOM 프로빙
            let isAdRow = false;
            try {
                if (cfg.list.skipPromo) {
                    // 빠른 경로: 다양한 마커를 커버하는 CSS :has()
                    isAdRow = !!(await postElement.$(cfg.list.skipPromo));
                }
            } catch { /* ignore */ }

            if (!isAdRow) {
                try {
                    isAdRow = await postElement.evaluate((el) => {
                        const scope = el.querySelector('.title, td.title, .baseList-cover, .baseList-box') || el;

                        // 1) 명시적 마커 탐지 (id/class/data/aria)
                        if (scope.querySelector('#ad-icon, [id*="ad-icon"], [class*="ad-icon"], [data-ad], [data-sponsored], [aria-label*="광고"]')) return true;

                        // 2) 작은 토큰 형태의 정확한 'AD' 텍스트 (오탐 방지)
                        const smalls = Array.from(scope.querySelectorAll('span, i, b, em'));
                        for (const n of smalls) {
                            const txt = (n.textContent || '').trim();
                            if (txt === 'AD') return true;
                            const idc = ((n.id || '') + ' ' + (n.className || '')).toLowerCase();
                            if (/\b(ad|admark|ad-icon|sponsor|promotion)\b/.test(idc)) return true;
                        }
                        return false;
                    });
                } catch { /* ignore */ }
            }
            if (isAdRow) continue;

            const linkElement = await postElement.$(cfg.list.postLink);
            if (!linkElement) continue;

            const postUrl = await linkElement.evaluate(el => (el as HTMLAnchorElement).href);
            // 상세 URL 허용 규칙 (호스트/경로 기반 기본값, 또는 사용자 정의 함수)
            try {
                const u = new URL(postUrl);
                const base = new URL(cfg.baseListUrl);
                const allow = cfg.postUrlAllow ? cfg.postUrlAllow(u, base, cfg) : (u.hostname === base.hostname && u.pathname.startsWith(`/${cfg.BOARD_NAME}/`));
                if (!allow) continue;
            } catch { continue; }

            let postId: string | null = null;
            try {
                const u = new URL(postUrl);
                postId = u.searchParams.get('no');
            } catch {
                continue; // Invalid URL
            }
            if (!postId || !/^\d+$/.test(postId)) continue;

            if (isFirstPage && !newLastStopId) newLastStopId = postId;

            // Incremental 모드 중단 조건
            if (mode === 'incremental' && lastStopId && postId === lastStopId) {
                shouldStop = true;
                log.info(`Incremental: 마지막 수집 게시물(ID: ${lastStopId})에 도달하여 중단합니다.`);
                break;
            }

            // Full 모드 중단 조건 (목록의 타임스탬프 힌트 사용, robust parsing + logging)
            if (mode === 'full' && cutoffDate && cfg.list.timestampHint) {
                const tsText = await postElement.evaluate((el, hintSel) => {
                    const t = (el as HTMLElement).querySelector(hintSel as string);
                    return t ? (t.textContent || '') : '';
                }, cfg.list.timestampHint);

                const trimmed = (tsText || '').replace('등록', '').trim();
                let postTimestamp: string | null = null;
                try {
                    postTimestamp = parseKstToUtcIso(trimmed, `LIST ${cfg.BOARD_NAME} ${page.url()} postId=${postId}`);
                    // DEBUG-only success trace
                    try { log.debug?.(`[timestamp][LIST] parsed ok: postId=${postId} ts="${postTimestamp}" from hint="${trimmed}" url=${page.url()}`); } catch { /* ignore */ }
                } catch (err) {
                    // Fail-open: log and skip cutoff check for this item
                    log.warning?.(`[timestamp][LIST] parse failed for postId=${postId} hint="${trimmed}" url=${page.url()} err=${String(err)}`);
                    postTimestamp = null;
                }

                if (postTimestamp && postTimestamp < cutoffDate) {
                    shouldStop = true;
                    log.info(`Full: Cutoff 날짜(${cutoffDate}) 이전 게시물에 도달하여 중단합니다. (post: ${postTimestamp}, postId: ${postId})`);
                    break;
                }
            }

            await enqueueLinks({ urls: [postUrl], label: "DETAIL", userData: { postId } });
        }

        if (mode === 'incremental' && isFirstPage && newLastStopId) {
            await stateStore.setValue("lastStopId", newLastStopId);
            log.info(`Incremental: 새로운 lastStopId를 저장했습니다: ${newLastStopId}`);
        }

        // 다음 페이지 이동: 클릭 대신 큐에 다음 LIST URL을 추가
        if (!shouldStop) {
            // hardMax가 있고 이미 이 페이지가 한도에 도달했으면 더 이상 enqueue하지 않음
            const hardMax = (typeof maxPages === 'number' && isFinite(maxPages)) ? maxPages : null;
            if (hardMax !== null && pagesSoFar >= hardMax) {
                log.info(`페이지 한도(${hardMax}) 도달: 다음 페이지를 큐에 추가하지 않습니다. (pagesCrawled=${pagesSoFar})`);
            } else {
                let nextUrl: string | null = null;
                // 1) 셀렉터에서 href 추출 우선 (ppomppu는 보통 a.next_page 등에 절대/상대 href 존재)
                for (const sel of cfg.list.nextPageSelectors || []) {
                    try {
                        const loc = page.locator(sel).first();
                        if (await loc.count()) {
                            const href = await loc.getAttribute('href');
                            if (href) {
                                nextUrl = new URL(href, page.url()).toString();
                                break;
                            }
                        }
                    } catch { /* try next selector */ }
                }
                // 2) 보조: pageParam 기반으로 다음 페이지 URL 구성
                if (!nextUrl) {
                    try {
                        const u = new URL(page.url());
                        const pageParam = cfg.list.pageParam || 'page';
                        const cur = parseInt(u.searchParams.get(pageParam) || '1', 10);
                        const nxt = Number.isFinite(cur) ? cur + 1 : 2;
                        u.searchParams.set(pageParam, String(nxt));
                        nextUrl = u.toString();
                    } catch { /* ignore */ }
                }
                if (nextUrl) {
                    await enqueueLinks({
                        urls: [nextUrl],
                        label: 'LIST',
                        userData: request.userData, // mode/cutoff/maxPages 유지
                    });
                    log.info(`다음 페이지 큐에 추가: ${nextUrl}`);
                } else {
                    log.info('다음 페이지 링크를 찾지 못했거나 마지막 페이지입니다.');
                }
            }
        }
    });

    // 상세 핸들러: 콘텐츠/메타/댓글 수집
    router.addHandler("DETAIL", async ({ request, page, log }) => {
        log.info(`상세 페이지 처리: ${request.loadedUrl} (보드: ${cfg.BOARD_NAME})`);
        const { postId } = request.userData;

        await page.waitForSelector(cfg.detail.content, { timeout: 3000 });

        // --- 제목/카테고리 ---
        let category: string | null = null;
        try {
            if (cfg.detail.titleCategoryLink) {
                category = (await page.locator(cfg.detail.titleCategoryLink).first().textContent({ timeout: 1000 }))?.trim() ?? null;
            }
        } catch { }

        // Extract title text excluding inline comment count, icons, and <time> (news),
        // then format:
        //  - forum style → "<title>"
        //  - news style  → "<title>"
        const titleEval = await page.locator(cfg.detail.title).evaluate((el) => {
            const node = el.cloneNode(true) as HTMLElement;
            // Remove inline counters and time/meta
            const c = node.querySelector('#comment');
            if (c) c.remove();
            const t = node.querySelector('time');
            const hasTime = !!t;
            if (t) t.remove();
            // Defensive: strip scripts/styles
            node.querySelectorAll('script,style').forEach((n) => n.remove());
            const text = (node.textContent || '').trim();
            return { text, isNews: hasTime };
        });
        let titleCore = (titleEval.text || '')
            .replace(category ?? '', '')
            .replace(/\s*\[\d+\]\s*$/, '')   // safety: remove trailing "[n]" if present
            .replace(/\s*\d+\s*$/, '')         // remove trailing bare numbers
            .replace(/\s+/g, ' ')
            .trim();
        // 뉴스형은 대괄호 없이 원문 그대로, 포럼형도 동일하게 plain text로
        const title = titleEval.isNews ? titleCore : titleCore;

        // --- 삭제/권한 제한 빠른 감지 (제목/본문 기반) ---
        try {
            const titleProbe = (titleCore || '').trim();
            let contentProbe = '';
            try {
                const contentNode = page.locator(cfg.detail.content).first();
                if (await contentNode.count()) {
                    contentProbe = await contentNode.innerText();
                }
            } catch { /* ignore */ }
            const probeText = `${titleProbe}\n${contentProbe}`;
            if (isRestrictedOrDeleted(probeText, cfg.detail.restrictedTextRegex)) {
                log.debug('삭제/권한 제한 게시물로 판단되어 결과 저장을 생략합니다.', { url: request.loadedUrl });
                return;
            }
        } catch { /* ignore */ }

        // --- 작성자/시간/조회/추천 (메타블록 파싱) ---
        let author: string | null = null;
        try {
            const aLoc = page.locator(cfg.detail.author).first();
            if (await aLoc.count()) {
                const tag = await aLoc.evaluate(el => el.tagName.toLowerCase());
                if (tag === 'img') {
                    author = (await aLoc.getAttribute('alt'))?.trim() || null;
                } else {
                    author = (await aLoc.textContent())?.replace(/\s+/g, ' ').trim() || null;
                }
            }
        } catch { author = null; }

        let rawTimestamp: string | null = null;
        let view_count: number | null = null;
        let like_count: number | null = null;
        let dislike_count: number | null = null;

        if (cfg.detail.metaBlock) {
            try {
                const metaLoc = page.locator(cfg.detail.metaBlock).first();
                // 빠른 존재 확인 (없으면 즉시 스킵)
                const exists = await metaLoc.count().catch(() => 0);
                if (exists) {
                    // 가벼운 API와 짧은 타임아웃 사용 (innerText → textContent)
                    const metaText = (await metaLoc.textContent({ timeout: 1200 }))?.trim() || '';
                    // e.g. "NVlDIA\n등록일 2025-08-09 17:42\n조회수 14726"
                    const timeMatch = metaText.match(/등록일\s*:?\s*([0-9]{4}[.-]\d{2}[.-]\d{2}\s+\d{2}:\d{2}(?::\d{2})?)/);
                    if (timeMatch && timeMatch[1]) {
                        rawTimestamp = timeMatch[1].trim();
                    }

                    const likeMatch = metaText.match(/(?:추천|좋아요)\s*:?\s*(\d+)/);
                    if (likeMatch && likeMatch[1]) {
                        like_count = parseInt(likeMatch[1], 10);
                    }

                    const viewMatch = metaText.match(/(?:조회수|조회)\s*:?\s*(\d+)/);
                    if (viewMatch && viewMatch[1]) {
                        view_count = parseInt(viewMatch[1], 10);
                    }
                }
            } catch (e) {
                log.debug?.('메타 블록이 없거나 빠른 파싱 실패(무시)', { url: request.loadedUrl, error: (e as any)?.message });
            }
        }
        // Fallback: explicit recommend box counts
        try {
            // Like: #vote_list_btn_txt, Dislike: #vote_anti_list_btn_txt
            const likeTxt = (await page.locator('#recommend #vote_list_btn_txt').first().textContent({ timeout: 800 }).catch(() => null))?.trim() || '';
            const dislikeTxt = (await page.locator('#recommend #vote_anti_list_btn_txt').first().textContent({ timeout: 800 }).catch(() => null))?.trim() || '';
            const likeNum = Number(likeTxt.replace(/\D/g, '')) || 0;
            const dislikeNum = Number(dislikeTxt.replace(/\D/g, '')) || 0;
            if (!Number.isFinite(like_count) || like_count == null) like_count = likeNum;
            else if (likeNum > 0) like_count = likeNum; // prefer visible counter if present
            if (!Number.isFinite(dislike_count) || dislike_count == null) dislike_count = dislikeNum;
            else if (dislikeNum > 0) dislike_count = dislikeNum;
        } catch { /* ignore recommend box fallback */ }

        // Fallback: try explicit timestamp selectors (e.g., news article time tag)
        if ((!rawTimestamp || !rawTimestamp.trim()) && (cfg.detail.timestampSelectors?.length)) {
            for (const sel of cfg.detail.timestampSelectors) {
                try {
                    const loc = page.locator(sel).first();
                    if (await loc.count()) {
                        const t = (await loc.textContent())?.trim() || '';
                        if (t) { rawTimestamp = t; break; }
                    }
                } catch { /* try next */ }
            }
        }
        // Normalize to a strict datetime token like "YYYY-MM-DD HH:mm[:ss]" or "YYYY.MM.DD HH:mm[:ss]" or "YYYY/MM/DD HH:mm[:ss]"
        if (rawTimestamp) {
            const m = rawTimestamp.match(/(\d{4}[./-]\d{2}[./-]\d{2}\s+\d{2}:\d{2}(?::\d{2})?)/);
            if (m) rawTimestamp = m[1];
        }
        const timestamp = parseKstToUtcIso(rawTimestamp || '', `DETAIL ${cfg.BOARD_NAME} ${request.loadedUrl} postId=${postId}`);

        // 아바타 (포스트): 우선순위 = PPOM_icon → nickcon → placeholder(단, no_face.jpg는 생략)
        let avatar: string | null = null;

        // 0) 기존 셀렉터에서 1차 후보
        for (const sel of cfg.detail.authorAvatarSelectors || []) {
            try {
                const loc = page.locator(sel).first();
                if (await loc.count() === 0) continue;
                const tagName = await loc.evaluate(el => el.tagName.toLowerCase());
                if (tagName === 'img') {
                    const src = (await loc.getAttribute('src'))?.trim() || '';
                    if (src) { avatar = src; }
                } else {
                    const styleBg = await loc.evaluate(el => (el as HTMLElement).style?.backgroundImage || getComputedStyle(el as HTMLElement).backgroundImage || '');
                    const m = styleBg.match(/url\(["']?(.*?)["']?\)/i);
                    const bgUrl = m && m[1] ? m[1] : null;
                    if (bgUrl && bgUrl.trim()) { avatar = bgUrl.trim(); }
                }
                if (avatar) break;
            } catch { /* continue */ }
        }

        // A) PPOM_icon 최우선
        try {
            const pp = await findPpomIconInPage(page);
            if (pp) avatar = pp;
        } catch { }

        // B) nickcon 다음
        if (!avatar || isDefaultPpomAvatarUrl(avatar)) {
            try {
                const nc = await findNickconInPage(page);
                if (nc) avatar = nc;
            } catch { }
        }

        // C) placeholder 허용(단, no_face.jpg 는 생략)
        if (avatar && /\/images\/no_face\.jpg(?:$|\?)/i.test(avatar)) {
            avatar = null;
        }
        if (avatar) {
            const abs = toAbsoluteUrl(request.loadedUrl || cfg.baseListUrl, avatar);
            avatar = abs || avatar;
        }

        // --- 본문 ---
        let content = '';
        let contentHtml = '';
        if (titleEval.isNews) {
            // 뉴스 형태: 본문에서 <h1> (제목/시간) 제거
            const cleaned = await page.locator(cfg.detail.content).first().evaluate((el) => {
                const node = el.cloneNode(true) as HTMLElement;
                // 제목 블록과 time 메타 제거
                const h1 = node.querySelector('h1');
                if (h1) h1.remove();
                node.querySelectorAll('time').forEach(t => t.remove());
                // 불필요한 스크립트/스타일 방어적 제거
                node.querySelectorAll('script,style').forEach(n => n.remove());
                return {
                    text: (node.textContent || '').trim(),
                    html: (node.innerHTML || '').trim(),
                };
            });
            content = (cleaned?.text || '').trim();
            contentHtml = (cleaned?.html || '').trim();
        } else {
            // 포럼 형태: 기존 방식 유지
            const contentNode = page.locator(cfg.detail.content).first();
            content = (await contentNode.innerText()).trim();
            contentHtml = (await contentNode.innerHTML()).trim();
        }
        // 본문 내부의 상대 경로(src/href/srcset) → 절대 경로로 변환
        try {
            const baseForContent = request.loadedUrl || cfg.baseListUrl;
            contentHtml = absolutizeContentUrls(contentHtml || '', baseForContent);
        } catch { /* noop */ }

        // --- 태그/이미지/임베드 ---
        const images: string[] = await collectImages(page, cfg);
        // Ensure twitter iframe rendered: conditionally nudge only if size is tiny
        try {
            const twFrameLoc = page.locator('iframe[src*="platform.twitter.com/embed/Tweet.html"]');
            if (await twFrameLoc.count()) {
                await twFrameLoc.first().scrollIntoViewIfNeeded();
                try { await page.waitForTimeout(300); } catch { }
                await page.$$eval('iframe[src*="platform.twitter.com/embed/Tweet.html"]', (els) => {
                    els.forEach((el) => {
                        const f = el as HTMLIFrameElement;
                        const w = (f as any).clientWidth || 0;
                        const h = (f as any).clientHeight || 0;
                        if (w < 50 || h < 50) {
                            f.style.width = '600px';
                            f.style.height = '800px';
                            f.style.visibility = 'visible';
                            f.style.opacity = '1';
                            f.removeAttribute('hidden');
                        }
                    });
                });
                try { await page.waitForTimeout(600); } catch { }
            }
        } catch { }
        const embeddedContent: Embedded[] = await collectEmbeds(page, cfg, log);
        const tags = await collectTags(page, cfg);

        // --- 댓글 수집 (깊이 추정 → 트리 구성 + 평면 개수) ---
        const collectComments = async (): Promise<{ topLevel: Comment[]; flatCount: number }> => {
            const topLevel: Comment[] = [];
            let flat: Comment[] = [];
            try {
                const hasContainer = cfg.detail.commentsContainer
                    ? await page.waitForSelector(cfg.detail.commentsContainer, { timeout: 3000 }).then(() => true).catch(() => false)
                    : false;
                if (!hasContainer || !cfg.detail.commentArticle) return { topLevel, flatCount: 0 };

                const articleEls = await page.$$(cfg.detail.commentArticle);
                log.debug(`[comments] page1 nodes=${articleEls.length}`);
                if (!articleEls.length) return { topLevel, flatCount: 0 };

                // Give page 1 a brief chance to hydrate like counts before scraping.
                // We wait until any span[id^="vote_cnt_"] shows a digit, up to ~1.2s.
                try {
                    await page.waitForFunction(() => {
                        const spans = Array.from(document.querySelectorAll('span[id^="vote_cnt_"]'));
                        return spans.some(s => /\d/.test((s.textContent || '').trim()));
                    }, { timeout: 1200 });
                } catch { /* proceed even if not hydrated */ }

                // ── 1) 현재(1페이지) DOM에서 수집 ───────────────────────────────
                flat = [];
                const scrapeArticleElement = async (el: import('playwright').ElementHandle<HTMLElement>) => {
                    const rawDomId = (await el.getAttribute('id')) || '';
                    const cid = rawDomId.replace(/^comment_/, '').replace(/^c_/, '');
                    const dbgTag = `[comments][p1] cid=${cid}`;

                    // depth 추정 (루트=0 보장)
                    let depth = 0;

                    // 1) class 기반 (comment_divN 우선)
                    try {
                        const classAttr = (await el.getAttribute('class')) || '';
                        const divMatch = classAttr.match(/comment_div(\d+)/);
                        if (divMatch) depth = parseInt(divMatch[1], 10) || 0;
                        if (depth === 0) {
                            const tmplMatch = classAttr.match(/comment_template_depth(\d+)/);
                            if (tmplMatch) {
                                const n = parseInt(tmplMatch[1], 10);
                                if (!isNaN(n)) depth = Math.max(0, n - 1);
                            }
                        }
                    } catch { /* ignore */ }

                    // 2) fallback: margin-left 기반
                    if (depth === 0) {
                        try {
                            const mlNode = await el.$('[style*="margin-left"]');
                            const styleAttr = (await (mlNode || el).getAttribute('style')) || '';
                            let m = styleAttr.match(/margin-left\s*:\s*([\d.]+)\s*rem/i);
                            if (m) depth = Math.round(parseFloat(m[1]));
                            if (depth === 0) {
                                m = styleAttr.match(/margin-left\s*:\s*([\d.]+)\s*px/i);
                                if (m) {
                                    const px = parseFloat(m[1]);
                                    // 16px ≈ 1 depth, 루트는 0 보장
                                    depth = Math.max(0, Math.round(px / 16));
                                }
                            }
                        } catch { /* ignore */ }
                    }

                    let author: string | null = null;
                    if (cfg.detail.commentAuthor) {
                        try {
                            const aEl = await el.$(cfg.detail.commentAuthor);
                            if (aEl) {
                                let txt = ((await aEl.textContent()) || '').replace(/\s+/g, ' ').trim();
                                if (!txt) {
                                    const imgEl = await aEl.$('img[alt]');
                                    const alt = imgEl ? await imgEl.getAttribute('alt') : null;
                                    txt = (alt || '').trim();
                                }
                                author = txt || null;
                            }
                        } catch { author = null; }
                    }

                    // 댓글 아바타 (depth별 우선순위)
                    //  - 루트(depth=0): PPOM_icon → nickcon → placeholder(단, no_face.jpg는 생략)
                    //  - 대댓글(depth>0): nickcon → placeholder(icon_reply)
                    let avatar: string | null = null;

                    if (depth === 0) {
                        // ROOT: PPOM_icon → nickcon → placeholder(no_face 제외)
                        try {
                            const pp = await findPpomIconInElement(el);
                            if (pp) avatar = pp;
                        } catch { }
                        if (!avatar) {
                            try {
                                const nc = await findNickconInElement(el);
                                if (nc) avatar = nc;
                            } catch { }
                        }
                        if (!avatar && (cfg.detail.commentAvatarSelectors?.length)) {
                            for (const s of cfg.detail.commentAvatarSelectors) {
                                try {
                                    const imgEl = await el.$(s);
                                    if (!imgEl) continue;
                                    const src = (await imgEl.getAttribute('src'))?.trim() || '';
                                    if (!src) continue;
                                    // 루트: no_face는 생략, icon_reply는 루트에서 보통 없지만 예외적으로도 제외
                                    if (/\/images\/no_face\.jpg(?:$|\?)/i.test(src)) continue;
                                    if (/\/images\/icon_reply(?:\d*)\.png(?:$|\?)/i.test(src)) continue;
                                    avatar = src;
                                    break;
                                } catch { /* try next */ }
                            }
                        }
                    } else {
                        // REPLY: nickcon only (placeholder 금지: icon_reply, no_face 등 불허)
                        try {
                            const nc = await findNickconInElement(el);
                            if (nc) avatar = nc;
                        } catch { }
                        // ※ 대댓글은 placeholder 허용하지 않음. nickcon이 없으면 avatar는 null 유지
                    }

                    if (avatar) {
                        const abs = toAbsoluteUrl(request.loadedUrl || cfg.baseListUrl, avatar);
                        avatar = abs || avatar;
                    }

                    let createdRaw: string | null = null;
                    if (cfg.detail.commentTime) {
                        try {
                            const tEl = await el.$(cfg.detail.commentTime);
                            if (tEl) {
                                const titleAttr = await tEl.getAttribute('title');
                                const tTxt = await tEl.textContent();
                                createdRaw = ((titleAttr || tTxt || '').trim()) || null;
                            }
                        } catch { createdRaw = null; }
                    }

                    // Ppomppu comment votes: like -> #vote_cnt_<cid>, dislike -> #anti_vote_cnt_<cid>
                    let likeCnt = 0;
                    let dislikeCnt = 0;
                    try {
                        const upSel = `#vote_cnt_${cid}`;
                        const upEl = await page.$(upSel);
                        const t = upEl ? ((await upEl.textContent()) || '').trim() : '';
                        likeCnt = Number((t || '0').replace(/\D/g, '')) || 0;
                    } catch { likeCnt = 0; }
                    try {
                        const downSel = `#anti_vote_cnt_${cid}`;
                        const downEl = await page.$(downSel);
                        const t = downEl ? ((await downEl.textContent()) || '').trim() : '';
                        dislikeCnt = Number((t || '0').replace(/\D/g, '')) || 0;
                    } catch { dislikeCnt = 0; }

                    // 원문/HTML
                    let raw: string | null = null;
                    try {
                        // Ppomppu: original html is kept in #ori_comment_<cid> textarea.ori_comment
                        const rawSel = `#ori_comment_${cid} textarea.ori_comment`;
                        const rawLoc = page.locator(rawSel).first();
                        if (await rawLoc.count()) raw = (await rawLoc.textContent()) || null;
                    } catch { raw = null; }
                    if (raw) {
                        try {
                            const baseForComment = request.loadedUrl || cfg.baseListUrl;
                            raw = sanitizeTrackingPixels(absolutizeContentUrls(raw, baseForComment));
                        } catch { /* noop */ }
                    }
                    let html: string | null = null;
                    let textContent: string | null = null;
                    if (cfg.detail.commentContentHtml) {
                        try {
                            const cEl = await el.$(cfg.detail.commentContentHtml);
                            if (cEl) {
                                html = await cEl.innerHTML();
                                const it = await cEl.innerText();
                                textContent = (it || '').trim() || null;
                                // Absolutize comment HTML (src/href/srcset + lazy data-original/data-src)
                                try {
                                    const baseForComment = request.loadedUrl || cfg.baseListUrl;
                                    html = absolutizeContentUrls(html || '', baseForComment);
                                } catch { /* noop */ }
                                html = sanitizeTrackingPixels(html || '');
                            }
                        } catch { html = null; textContent = null; }
                    }
                    textContent = raw ?? textContent;

                    const createdSanitized = (createdRaw || '')
                        .replace(/[^0-9:.\-\s]/g, '')
                        .trim();

                    const timestampIso = parseKstToUtcIso(
                        createdSanitized || '',
                        `COMMENT ${cfg.BOARD_NAME} ${request.loadedUrl} postId=${postId} cid=${cid}`
                    );

                    const item: Comment = {
                        id: `${cfg.SITE_NAME}-${cfg.BOARD_NAME}-${postId}_${cid}`,
                        author,
                        avatar,
                        content: textContent,
                        timestamp: timestampIso,
                        isReply: depth > 0,
                        replies: [],
                        likeCount: likeCnt,
                        dislikeCount: dislikeCnt,
                        reactionCount: 0,
                        raw,
                        html,
                        depth,
                        parentId: null,
                    };
                    flat.push(item);
                };

                for (const el of articleEls) {
                    try { await scrapeArticleElement(el as any); } catch { /* skip */ }
                }
                // (debug logs removed)

                // 현재 DOM에서 댓글 마지막 페이지 수를 결정 (#page_list의 마지막 <a> 숫자)
                let maxPages = 1;
                try {
                    const pageListLoc = page.locator('#page_list');
                    if (await pageListLoc.count()) {
                        const html = (await pageListLoc.innerHTML()) || '';
                        const $ = cheerio.load(html);
                        const nums = $('#page_list a, #page_list font.page_inert, a, font.page_inert')
                            .map((_, el) => Number($(el).text().trim()))
                            .get()
                            .filter((n) => Number.isFinite(n));
                        if (nums.length) maxPages = Math.max(...(nums as number[]));
                    }
                } catch { maxPages = 1; }
                log.debug(`[comments] detected maxPages=${maxPages}`);
                try {
                    const base = new URL(request.loadedUrl);
                    const boardId = base.searchParams.get('id') || cfg.BOARD_NAME;
                    const postNo = String(postId);

                    const fetchCommentPage = async (pageNum: number): Promise<string | null> => {
                        log.info(`[comments] fetching fragment p=${pageNum}`);
                        const baseParams = `id=${boardId}&no=${postNo}&c_page=${pageNum}&comment_mode=`;
                        const url = `${base.origin}/zboard/comment.php?${baseParams}`;
                        const headers = {
                            'X-Requested-With': 'XMLHttpRequest',
                            'Accept': 'text/html, */*; q=0.01',
                            'Referer': request.loadedUrl || `${base.origin}/zboard/view.php?id=${boardId}&no=${postNo}`,
                            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
                        } as Record<string, string>;
                        try {
                            const resp = await page.request.get(url, { headers });
                            if (!resp.ok()) return null;
                            const ct = resp.headers()['content-type'];
                            const charset = detectCharset(ct);
                            const buf = await resp.body();
                            const body = iconv.decode(buf, charset || 'utf-8');
                            log.debug(`[comments] fetched p=${pageNum} ok=${resp.ok()} len=${body.length} charset=${charset}`);
                            return body.trim() ? body : null;
                        } catch { return null; }
                    };

                    const extractFromFragment = async (htmlFrag: string) => {
                        let items: any[] = [];
                        try {
                            const $ = cheerio.load(htmlFrag);
                            const found: any[] = [];
                            const nodes = $('div[id^="comment_"]');
                            const debugCount = nodes.length;

                            nodes.each((_, el) => {
                                const $el = $(el);
                                const rawDomId = $el.attr('id') || '';
                                const cid = rawDomId.replace(/^comment_/, '').replace(/^c_/, '');

                                // depth 추정 (루트=0 보장)
                                let depth = 0;
                                const cls = ($el.attr('class') || '');
                                let m: RegExpMatchArray | null = null;
                                let dm = cls.match(/comment_div(\d+)/);
                                if (dm) {
                                    depth = parseInt(dm[1], 10) || 0;
                                } else {
                                    const tm = cls.match(/comment_template_depth(\d+)/);
                                    if (tm) {
                                        const n = parseInt(tm[1], 10);
                                        if (!isNaN(n)) depth = Math.max(0, n - 1);
                                    }
                                }
                                if (depth === 0) {
                                    const styleAttr = $el.attr('style') || '';
                                    m = styleAttr.match(/margin-left\s*:\s*([\d.]+)\s*rem/i);
                                    if (m) depth = Math.round(parseFloat(m[1]));
                                    if (depth === 0) {
                                        m = styleAttr.match(/margin-left\s*:\s*([\d.]+)\s*px/i);
                                        if (m) depth = Math.max(0, Math.round(parseFloat(m[1]) / 16));
                                    }
                                }

                                const q = (sel: string) => $el.find(sel).first();
                                let author: string | null = null;
                                if (cfg.detail.commentAuthor) {
                                    const $a = q(cfg.detail.commentAuthor);
                                    const txt = ($a.text() || '').replace(/\s+/g, ' ').trim();
                                    if (txt) {
                                        author = txt;
                                    } else {
                                        // nickcon: nickname is in <img alt="...">
                                        const alt = $a.find('img[alt]').first().attr('alt');
                                        author = alt ? alt.trim() : null;
                                    }
                                }

                                // avatar (depth별 우선순위)
                                //  - 루트(depth=0): PPOM_icon → nickcon → placeholder(단, no_face.jpg 생략)
                                //  - 대댓글(depth>0): nickcon only (placeholder 불허)
                                let avatar: string | null = null;
                                if (depth === 0) {
                                    // ROOT
                                    const pp = $el.find('img[src*="PPOM_icon/"]').first();
                                    const pSrc = pp && pp.length ? (pp.attr('src') || '').trim() : '';
                                    if (pSrc) {
                                        const abs = toAbsoluteUrl(request.loadedUrl || cfg.baseListUrl, pSrc);
                                        avatar = abs || pSrc;
                                    }
                                    if (!avatar) {
                                        const nickcon = $el.find('img[src*="zboard/nickcon/"]').first();
                                        const ncSrc = nickcon && nickcon.length ? (nickcon.attr('src') || '').trim() : '';
                                        if (ncSrc) {
                                            const abs = toAbsoluteUrl(request.loadedUrl || cfg.baseListUrl, ncSrc);
                                            avatar = abs || ncSrc;
                                        }
                                    }
                                    if (!avatar && (cfg.detail.commentAvatarSelectors?.length)) {
                                        for (const s of cfg.detail.commentAvatarSelectors) {
                                            const img = $el.find(s).first();
                                            const src = (img.attr('src') || '').trim();
                                            if (!src) continue;
                                            if (/\/images\/no_face\.jpg(?:$|\?)/i.test(src)) continue;
                                            if (/\/images\/icon_reply(?:\d*)\.png(?:$|\?)/i.test(src)) continue;
                                            const abs = toAbsoluteUrl(request.loadedUrl || cfg.baseListUrl, src);
                                            avatar = abs || src;
                                            break;
                                        }
                                    }
                                } else {
                                    // REPLY: nickcon only (placeholder 금지)
                                    const nickcon = $el.find('img[src*="zboard/nickcon/"]').first();
                                    const ncSrc = nickcon && nickcon.length ? (nickcon.attr('src') || '').trim() : '';
                                    if (ncSrc) {
                                        const abs = toAbsoluteUrl(request.loadedUrl || cfg.baseListUrl, ncSrc);
                                        avatar = abs || ncSrc;
                                    }
                                    // ※ 대댓글은 placeholder 허용하지 않음. nickcon이 없으면 avatar는 null 유지
                                }

                                const timeEl = cfg.detail.commentTime ? q(cfg.detail.commentTime) : null;
                                const createdRaw = timeEl ? ((timeEl.attr('title') || timeEl.text() || '').trim()) : null;

                                // Ppomppu: like/dislike count spans
                                let likeCnt = 0;
                                let dislikeCnt = 0;
                                {
                                    let txtUp = '';
                                    const exactUp = $el.find(`#vote_cnt_${cid}`).first();
                                    if (exactUp && exactUp.length) txtUp = exactUp.text() || '';
                                    else {
                                        const upEl = $el.find('span[id^="vote_cnt_"]').first();
                                        txtUp = upEl.length ? (upEl.text() || '') : '';
                                    }
                                    likeCnt = Number((txtUp || '0').replace(/\D/g, '')) || 0;

                                    let txtDown = '';
                                    const exactDown = $el.find(`#anti_vote_cnt_${cid}`).first();
                                    if (exactDown && exactDown.length) txtDown = exactDown.text() || '';
                                    else {
                                        const downEl = $el.find('span[id^="anti_vote_cnt_"]').first();
                                        txtDown = downEl.length ? (downEl.text() || '') : '';
                                    }
                                    dislikeCnt = Number((txtDown || '0').replace(/\D/g, '')) || 0;
                                }

                                let raw: string | null = null;
                                const ori = $(`#ori_comment_${cid} textarea.ori_comment`).first();
                                if (ori && ori.length) raw = ori.text() || null;
                                if (raw) {
                                    try {
                                        const baseForComment = request.loadedUrl || cfg.baseListUrl;
                                        raw = sanitizeTrackingPixels(absolutizeContentUrls(raw, baseForComment));
                                    } catch { /* noop */ }
                                }

                                let htmlInner: string | null = null;
                                let textContent: string | null = null;
                                if (cfg.detail.commentContentHtml) {
                                    const c = q(cfg.detail.commentContentHtml);
                                    htmlInner = c.length ? c.html() || null : null;
                                    textContent = c.length ? (c.text() || '').trim() : null;
                                }
                                // Absolutize within fragment HTML as well
                                if (htmlInner) htmlInner = absolutizeContentUrls(htmlInner, request.loadedUrl || cfg.baseListUrl);
                                if (htmlInner) htmlInner = sanitizeTrackingPixels(htmlInner);

                                found.push({ cid, depth, author, avatar, createdRaw, likeCnt, dislikeCnt, raw, html: htmlInner, textContent });
                            });

                            // log.info(`[comments] fragment nodes=${debugCount}`);
                            items = found;
                            // log.info(`[comments] fragment parsed items=${items.length}`);
                        } catch (e: any) {
                            log.error(`[comments] fragment parse error: ${e?.message || e}`);
                            items = [];
                        }

                        for (const it of items) {
                            const createdSanitized = (it.createdRaw || '').replace(/[^0-9:.\-\s]/g, '').trim();
                            const item: Comment = {
                                id: `${cfg.SITE_NAME}-${cfg.BOARD_NAME}-${postId}_${it.cid}`,
                                author: it.author,
                                avatar: it.avatar,
                                content: it.raw ?? it.textContent ?? null,
                                timestamp: parseKstToUtcIso(createdSanitized || null, `COMMENT ${cfg.BOARD_NAME} ${request.loadedUrl} postId=${postId} cid=${it.cid}`),
                                isReply: (it.depth || 0) > 0,
                                replies: [],
                                likeCount: Number(it.likeCnt) || 0,
                                dislikeCount: Number(it.dislikeCnt) || 0,
                                reactionCount: 0,
                                raw: it.raw,
                                html: it.html,
                                depth: it.depth || 0,
                                parentId: null,
                            };
                            flat.push(item);
                        }
                    };

                    // Concurrently fetch all subsequent comment pages
                    if (maxPages > 1) {
                        const pageNumbers = Array.from({ length: maxPages - 1 }, (_, i) => i + 2);
                        const promises = pageNumbers.map(p => fetchCommentPage(p));
                        const htmlFragments = await Promise.all(promises);

                        for (const html of htmlFragments) {
                            if (html) {
                                try {
                                    await extractFromFragment(html);
                                } catch (e: any) {
                                    log.error(`[comments] extract error from fragment: ${e?.message || e}`);
                                }
                            }
                        }
                        log.debug(`[comments] merged all ${htmlFragments.length} pages.`);
                    }
                } catch { /* ignore pagination errors */ }

                // ── 3) 깊이 기반 트리 구성 ─────────────────────────────────────
                const stack: { id: string; depth: number }[] = [];
                const byId = new Map<string, Comment>();
                for (const item of flat) {
                    const d = item.depth ?? 0;
                    while (stack.length && stack[stack.length - 1].depth >= d) stack.pop();
                    item.parentId = stack.length ? stack[stack.length - 1].id : null;
                    byId.set(item.id, item);
                    if (item.parentId) {
                        const parent = byId.get(item.parentId);
                        if (parent) parent.replies.push(item); else topLevel.push(item);
                    } else {
                        topLevel.push(item);
                    }
                    stack.push({ id: item.id, depth: d });
                }

                log.debug(`[comments] final flat=${flat.length} topLevel=${topLevel.length}`);
                return { topLevel, flatCount: flat.length };
            } catch (error) {
                log.error('전체 댓글 수집 중 오류', { error, url: request.loadedUrl });
                return { topLevel, flatCount: 0 };
            }
        };

        const { topLevel: comments, flatCount } = await collectComments();

        const idSource = `${cfg.SITE_NAME}-${cfg.BOARD_NAME}-${postId}`;
        const id = createHash("md5").update(idSource).digest("hex").substring(0, HASH_LEN_POST_ID);
        const content_hash = createHash("md5").update(content).digest("hex");

        const result = {
            post_id: String(postId),
            id,
            url: request.loadedUrl,
            site: cfg.SITE_NAME,
            board: cfg.BOARD_NAME,
            title,
            author,
            avatar,
            timestamp,
            view_count,
            like_count,
            dislike_count,
            category,
            tags,
            content,
            contentHtml,
            content_hash,
            images,
            embeddedContent,
            crawledAt: new Date().toISOString(),
            comments,
            // 카운트는 main.ts에서 표준적으로 계산
        };
        await (await Dataset.open(outDatasetId)).pushData(result);
    });

    return router;
}

// ===[ 수집기 ]================================================================
async function collectTags(page: import('playwright').Page, cfg: BoardConfig): Promise<string[]> {
    const sels = cfg.detail.tagSelectors || [];
    const texts: string[] = [];
    for (const sel of sels) {
        const arr = await page.$$eval(sel, (as) => as.map((a) => (a as HTMLAnchorElement).textContent?.trim() || '').filter(Boolean)).catch(() => [] as string[]);
        texts.push(...arr);
    }
    return uniq(texts);
}

async function collectImages(page: import('playwright').Page, cfg: BoardConfig): Promise<string[]> {
    const base = page.url();
    const sel = (cfg.detail.imageSelectors || []).join(',');
    if (!sel) return [];

    const urls = await page.$$eval(sel, (imgs: Element[]) => {
        const collectedUrls: string[] = [];
        for (const img of imgs) {
            const el = img as HTMLImageElement;

            // 1. Filter by computed style for visibility
            const style = window.getComputedStyle(el);
            if (style.display === 'none' || style.visibility === 'hidden' || parseFloat(style.opacity) < 0.1) {
                continue;
            }

            // 2. Filter by size (attributes or rendered) for 1x1 tracking pixels
            const attrWidth = parseInt(el.getAttribute('width') || '999', 10);
            const attrHeight = parseInt(el.getAttribute('height') || '999', 10);
            if (attrWidth <= 1 || attrHeight <= 1) {
                continue;
            }
            
            const renderedWidth = el.clientWidth;
            const renderedHeight = el.clientHeight;
            if ((renderedWidth > 0 && renderedWidth <= 1) || (renderedHeight > 0 && renderedHeight <= 1)) {
                continue;
            }

            // 3. If visible and not a tracking pixel, collect all possible source URLs
            const sources = new Set<string>();
            const ds = el.getAttribute('data-src');
            if (ds) sources.add(ds);
            const s = el.getAttribute('src');
            if (s) sources.add(s);
            const ss = el.getAttribute('srcset');
            if (ss) {
                ss.split(',').forEach((part) => {
                    const url = part.trim().split(' ')[0];
                    if (url) sources.add(url);
                });
            }
            
            collectedUrls.push(...Array.from(sources));
        }
        return collectedUrls;
    }).catch(() => [] as string[]);

    // 4. Filter by URL patterns for things not detectable in the DOM (e.g., extensions)
    const filtered = urls.filter(u => {
        const s = (u || '').trim();
        if (!s) return false;
        // Exclude browser extension images
        if (s.startsWith('chrome-extension://')) return false;
        // Exclude UI elements like 'expand' icons
        const noHash = s.split('#')[0];
        return !/icon_expand_img\.png(?:$|\?)/i.test(noHash);
    });

    return normalizeAbsolute(base, filtered);
}

function isYouTubeUrl(u: string): boolean {
    return /(?:youtube\.com|youtu\.be)/.test(u);
}

/**
 * Pick the most representative Twitter/X thumbnail from a set of twimg URLs.
 * Priority:
 *  1) amplify_video_thumb
 *  2) ext_tw_video_thumb
 *  3) media (generic)
 *  4) generic media
 *  5) explicit poster/bgPoster passed-in
 */
function pickBestTwitterThumb(
    urls: string[],
    poster?: string | null,
    bgPoster?: string | null,
): string | null {
    const unique = Array.from(new Set(urls.filter(Boolean)));
    const byPriority = [/amplify_video_thumb\//i, /ext_tw_video_thumb\//i, /media\//i];
    for (const re of byPriority) {
        const found = unique.find(u => re.test(u));
        if (found) return found;
    }
    if (unique.length) return unique[0];
    return poster || bgPoster || null;
}

async function collectEmbeds(page: import('playwright').Page, cfg: BoardConfig, log?: { info: Function; debug?: Function; warning?: Function }): Promise<Embedded[]> {
    const base = page.url();
    const embeds: Embedded[] = [];

    // 1) YouTube (iframe + anchor)
    const ytIds = new Set<string>();
    try {
        const urlNodesSel = [
            `${cfg.detail.content} iframe[src]`,
            `${cfg.detail.content} a[href]`,
            `${cfg.detail.commentsContainer} a[href]`,
            'iframe[src]',
            'a[href]'
        ].join(', ');
        const rawUrls: string[] = await page.$$eval(urlNodesSel, els =>
            els.map(el => (el as any).src || (el as any).href || '')
        ).catch(() => []);
        for (const url of rawUrls) {
            const m = url.match(YT_ID_RE);
            if (m && m[1]) ytIds.add(m[1]);
        }
    } catch { /* ignore */ }

    const ytEmbeds = await Promise.all(
        [...ytIds].map(async (id) => {
            const { title, thumbnail } = await getYouTubeInfo(id);
            return {
                type: 'youtube',
                url: `https://youtu.be/${id}`,
                provider: 'youtube',
                videoId: id,
                thumbnail,
                title,
                description: null,
            } as Embedded;
        })
    );
    embeds.push(...ytEmbeds);

    // 2) Twitter (iframe + blockquote)
    const tweetIds = new Map<string, string>(); // id -> canonical URL
    try {
        const iframeSrcsRaw = await page.$$eval(
            'iframe[src*="platform.twitter.com/embed/Tweet.html"]',
            els => els.map(el => (el as HTMLIFrameElement).getAttribute('src'))
        ).catch(() => [] as (string | null)[]);
        log?.debug?.(`[twitter] iframe count: ${iframeSrcsRaw.length}`);
        for (const src of iframeSrcsRaw) {
            if (!src) continue;
            try {
                const u = new URL(src, page.url());
                const id = u.searchParams.get('id');
                if (id) {
                    // Canonical URL: use x.com
                    tweetIds.set(id, `https://x.com/i/web/status/${id}`);
                    log?.debug?.(`[twitter] iframe src found id=${id} src=${src}`);
                } else {
                    log?.debug?.(`[twitter] iframe src without id param: ${src}`);
                }
            } catch { /* ignore */ }
        }
    } catch { /* ignore */ }

    try {
        const cites = await page.$$eval('blockquote.twitter-tweet', els =>
            els.map(el => {
                const q = el as HTMLQuoteElement;
                return q.cite || (q.querySelector('a[href]') as HTMLAnchorElement | null)?.href || '';
            })
        ).catch(() => [] as string[]);
        log?.debug?.(`[twitter] blockquote cites: ${cites.length}`);
        for (const url of cites) {
            if (!url) continue;
            const m = url.match(/status\/(\d+)/);
            if (m) tweetIds.set(m[1], url);
        }
        log?.debug?.(`[twitter] tweetIds after cites: ${[...tweetIds.keys()].join(',')}`);
    } catch { /* ignore */ }

    // Enrich from embedded Twitter iframes' DOM (inside the page)
    // Map key: tweetId  →  value: {thumbnail, text}
    const twFrameMeta = new Map<string, { thumbnail?: string | null; text?: string | null }>();
    try {
        const frames = page.frames();
        log?.debug?.(`[twitter] total frames: ${frames.length}`);
        for (const f of frames) {
            const furl = f.url();
            if (!/platform\.twitter\.com\/embed\/Tweet\.html/.test(furl)) continue;
            let frameId: string | null = null;
            try { const u = new URL(furl); frameId = u.searchParams.get('id'); } catch { }
            log?.debug?.(`[twitter] parsing frame url=${furl} idFromUrl=${frameId}`);
            try {
                // Debug capture (disabled by default)
                // try {
                //     const ts = new Date().toISOString().replace(/[:.]/g, '-');
                //     const debugDir = 'output/_debug';
                //     await fs.mkdir(debugDir, { recursive: true }).catch(() => { });
                //     const frameEl = await f.frameElement();
                //     const shotPath = `${debugDir}/tw-frame-${frameId || 'unknown'}-${ts}.png`;
                //     await (frameEl as any).screenshot({ path: shotPath });
                //     log?.info?.(`[twitter][frame] screenshot saved: ${shotPath}`);
                //     try {
                //         const htmlPath = `${debugDir}/tw-frame-${frameId || 'unknown'}-${ts}.html`;
                //         const html = await f.evaluate(() => document.documentElement?.outerHTML || document.body?.outerHTML || '');
                //         await fs.writeFile(htmlPath, html);
                //         log?.info?.(`[twitter][frame] html saved: ${htmlPath}`);
                //     } catch { }
                // } catch { }

                // Minimal waits for content
                try { await f.waitForSelector('video[poster], [data-testid="tweetText"]', { timeout: 2000 }); } catch { }
                try { await f.waitForSelector('[data-testid="tweetText"]', { timeout: 800 }); } catch { }

                // Locator-only extraction (robust in dev runner)
                const poster = await f.locator('video[poster]').first().getAttribute('poster').catch(() => null);
                let text: string | null = null;
                try { text = (await f.locator('[data-testid="tweetText"]').first().innerText()).trim(); } catch { text = null; }
                const thumb = pickBestTwitterThumb(poster ? [poster] : [], poster, null);
                if (frameId && (thumb || text)) twFrameMeta.set(frameId, { thumbnail: thumb, text });
                // no network listeners to clean up
            } catch { /* ignore single frame */ }
        }
    } catch { /* ignore */ }

    log?.debug?.(`[twitter] collected frame meta keys=${JSON.stringify([...twFrameMeta.keys()])} size=${twFrameMeta.size}`);
    const twEmbeds = await Promise.all(
        [...tweetIds].map(async ([id, url]) => {
            const inFrame = twFrameMeta.get(id);
            const text = inFrame?.text || null;
            const title = text ? (text.length > 80 ? (text.slice(0, 80) + '…') : text) : null;
            const thumb = inFrame?.thumbnail ?? null;
            log?.debug?.(`[twitter] finalize(id=${id}) thumb=${thumb} title=${title ? title.slice(0, 40) : null} mapped=${!!inFrame}`);
            return {
                type: 'twitter',
                url,
                provider: 'twitter',
                videoId: id,
                thumbnail: thumb,
                title,
                description: text,
            } as Embedded;
        })
    );
    embeds.push(...twEmbeds);

    // 3) HTML5 <video> sources (+ poster as thumbnail)
    try {
        // Prefer content-scoped videos, but fall back to any videos on the page
        const contentScopedSel = `${cfg.detail.content} video`;
        const globalVideoSel = 'video';

        // 1) Gather video elements (poster/src plus nested <source> list)
        const videos = await page.$$eval(
            `${contentScopedSel}, ${globalVideoSel}`,
            (nodes) => {
                return nodes.map((node) => {
                    const v = node as HTMLVideoElement;
                    const poster = v.getAttribute('poster');
                    const directSrc = v.getAttribute('src');
                    const sources: { src: string | null; type: string | null }[] = [];
                    v.querySelectorAll('source[src]').forEach((s) => {
                        sources.push({ src: s.getAttribute('src'), type: s.getAttribute('type') });
                    });
                    return { poster, src: directSrc, sources };
                });
            }
        ).catch(() => [] as any[]);

        // 2) Additionally capture standalone <source> tags (some skins render only <source> inside <video>)
        const bareSources = await page.$$eval(
            'video source[src]',
            (nodes) => nodes.map((s) => ({ src: (s as HTMLSourceElement).getAttribute('src'), type: (s as HTMLSourceElement).getAttribute('type') }))
        ).catch(() => [] as { src: string | null; type: string | null }[]);

        // Merge candidates
        type Cand = { url: string | null; type: string | null };
        const candidateSrcs: Cand[] = [];
        for (const v of videos) {
            if (v.src) candidateSrcs.push({ url: v.src, type: null });
            for (const s of v.sources || []) candidateSrcs.push({ url: s.src, type: s.type });
        }
        candidateSrcs.push(...bareSources.map((b) => ({ url: b.src, type: b.type })));

        // De-duplicate by absolute URL
        const seen = new Set<string>();
        for (const cand of candidateSrcs) {
            const abs = toAbsoluteUrl(page.url(), cand.url || undefined);
            if (!abs) continue;
            if (seen.has(abs)) continue;
            seen.add(abs);

            const isMp4ByExt = abs.toLowerCase().endsWith('.mp4');
            const mime = (cand.type || '').toLowerCase();
            const looksLikeMp4 = isMp4ByExt || mime.includes('mp4');
            // Accept only clear video-like URLs; prefer mp4 but allow explicit video/* types
            if (!(looksLikeMp4 || mime.startsWith('video/'))) continue;

            embeds.push({
                type: 'video',
                url: abs,
                provider: 'html5',
                videoId: null,
                thumbnail: null, // may be filled by <video poster>; too noisy to merge multiple posters
                title: null,
                description: null,
                mimeType: mime || (isMp4ByExt ? 'video/mp4' : null),
            });
        }
    } catch { /* ignore */ }

    // 4) Generic iframes
    const iframeSel = (cfg.detail.iframeSelectors || []).join(',');
    const iframeSrcs = iframeSel
        ? await page.$$eval(iframeSel, (els) => (els as HTMLIFrameElement[]).map(el => el.getAttribute('src')))
            .catch(() => [] as (string | null)[])
        : [];
    const twitterIframeSrcs = await page.$$eval(
        'iframe[src*="platform.twitter.com/embed/Tweet.html"]',
        (els) => els.map((el) => (el as HTMLIFrameElement).getAttribute('src'))
    ).catch(() => [] as (string | null)[]);
    const allIframeSrcs = normalizeAbsolute(base, [...iframeSrcs, ...twitterIframeSrcs]);

    for (const src of allIframeSrcs) {
        if (isYouTubeUrl(src)) continue; // already handled above
        // Skip Twitter/X iframes; handled separately above
        if (/twitter\.com|x\.com|twimg\.com/i.test(src)) continue;
        embeds.push({ type: 'iframe', url: src, provider: null, videoId: null, thumbnail: null, title: null, description: null });
    }

    return embeds;
}
