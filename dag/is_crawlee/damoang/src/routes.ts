import {
    createPlaywrightRouter,
    Dataset,
    KeyValueStore,
} from "crawlee";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";


/**
 * ---------------------------------------------------------------------------
 * 커뮤니티 게시판 라우터 (std-optimized)
 *  - 게시판별 **셀렉터/규칙** 을 정의하고 Playwright 라우터(LIST/DETAIL)를 제공합니다.
 *  - HTML 구조가 비슷하면 CONFIG와 셀렉터만 바꿔 바로 재사용합니다.
 * ---------------------------------------------------------------------------
 */

// ===[ 고정 상수 ]============================================================
const HASH_LEN_POST_ID = 16;
const KST_OFFSET_MIN = 9 * 60; // KST(UTC+9)
const YT_ID_RE = /(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;

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
    mimeType?: string | null;
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
        skipDeleted?: string;   // 삭제된 게시물 스킵 (리스트에서 제목 등으로 판별)
        pageParam?: string;     // 페이지 파라미터명 (기본: page)
        nextPageSelectors?: string[]; // 다음 페이지로 이동할 수 있는 셀렉터 후보들
    };
    detail: {
        content: string;         // 본문 컨테이너
        title: string;           // 제목 컨테이너
        titleCategoryLink?: string; // 카테고리 링크(제목 내부)
        author: string;          // 작성자
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
        commentReactionBadgeCounts?: string; // 댓글 리액션 배지들의 개수(span) 셀렉터 (합산)
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

function isRestrictedOrDeleted(text: string, patterns?: (string | RegExp)[]): boolean {
    const s = (text || '').replace(/\s+/g, '');
    const pats = patterns ?? [/권한이없|삭제|존재하지|비공개/];
    return pats.some(p => (typeof p === 'string' ? new RegExp(p) : p).test(s));
}

function extractInt(text: string | null | undefined): number {
    return Number(text?.replace(/\D/g, '') || 0);
}

// ===[ 시간 파서 (KST → UTC ISO) ]===========================================
function toUtcIsoFromKst(y: number, m: number, d: number, hh = 0, mm = 0, ss = 0): string {
    // KST 기준 시간을 UTC로 변환 (초 단위까지 지원)
    const dateKst = Date.UTC(y, m - 1, d, hh, mm, ss) - KST_OFFSET_MIN * 60 * 1000;
    return new Date(dateKst).toISOString();
}

/**
 * Damoang 형식 기반 KST 기준 파서 (fail-fast, 명시 포맷만 허용)
 * 허용:
 *  - HH:mm (오늘)
 *  - MM.DD HH:mm (연도 추정, 미래면 전년도)
 *  - YYYY.MM.DD
 *  - YYYY.MM.DD HH:mm (명시적 KST)
 *  - 명시적 ISO/RFC 2822
 */
function parseKstToUtcIso(dateStr: string | null, context?: string): string {
    const ctx = context ? ` [context: ${context}]` : '';
    if (!dateStr || !dateStr.trim()) {
        throw new Error(`Timestamp parsing failed: empty or null input${ctx}`);
    }
    let s = dateStr.replace('등록', '').trim();

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

    // "어제 HH:mm" / "오늘 HH:mm" / "그제|그저께 HH:mm" (KST 기준)
    let ymdShiftMatch = s.match(/^(?:\s*)(오늘|어제|그제|그저께)\s+(\d{1,2}):(\d{2})(?:\s*)$/);
    if (ymdShiftMatch) {
        const token = ymdShiftMatch[1];
        const hh = parseInt(ymdShiftMatch[2], 10);
        const mm2 = parseInt(ymdShiftMatch[3], 10);
        let deltaDays = 0;
        if (token === '어제') deltaDays = -1;
        else if (token === '그제' || token === '그저께') deltaDays = -2;
        else deltaDays = 0; // 오늘

        // KST 자정(00:00)의 UTC 타임스탬프 → deltaDays 적용 → 다시 KST 달력값
        const kstMidnightUtc = Date.UTC(kY, kM - 1, kD, 0, 0, 0) - KST_OFFSET_MIN * 60 * 1000;
        const targetUtc = kstMidnightUtc + deltaDays * 24 * 60 * 60 * 1000;
        const targetKst = new Date(targetUtc + KST_OFFSET_MIN * 60 * 1000);
        const yy = targetKst.getUTCFullYear();
        const mmK = targetKst.getUTCMonth() + 1;
        const dd = targetKst.getUTCDate();
        return toUtcIsoFromKst(yy, mmK, dd, hh, mm2);
    }

    // HH:mm
    let m = s.match(/^(?:\s*)(\d{2}):(\d{2})(?:\s*)$/);
    if (m) return toUtcIsoFromKst(kY, kM, kD, parseInt(m[1], 10), parseInt(m[2], 10));

    // MM.DD HH:mm
    m = s.match(/^(?:\s*)(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})(?:\s*)$/);
    if (m) {
        // 연도 추정(미래면 전년도)
        let y = kY;
        const month = parseInt(m[1], 10);
        const day = parseInt(m[2], 10);
        const hh = parseInt(m[3], 10);
        const mm = parseInt(m[4], 10);
        const asUtc = new Date(toUtcIsoFromKst(kY, month, day, hh, mm));
        if (asUtc.getTime() > nowUtc.getTime()) y = kY - 1;
        return toUtcIsoFromKst(y, month, day, hh, mm);
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
}

export const BOARDS: Record<string, BoardConfig> = {
    free: makeBoard({
        siteName: 'damoang',
        boardName: 'free',
        baseUrl: 'https://damoang.net',
    }),
    // 예시:
    // news_clien: makeBoard({ siteName: 'clien', boardName: 'news', baseUrl: 'https://www.clien.net' }),
};

// 공통 셀렉터가 동일한 커뮤니티 계열 보드를 간단히 생성하는 팩토리
function makeBoard(opts: BoardOptions): BoardConfig {
    const STORE_KEY = `CRAWL_STATE_${opts.siteName.toUpperCase()}_${opts.boardName.replace(/[^a-z0-9]/gi, '_').toUpperCase()}`;
    return {
        SITE_NAME: opts.siteName,
        BOARD_NAME: opts.boardName,
        STATE_STORE_NAME: STORE_KEY,
        baseListUrl: `${opts.baseUrl}/${opts.boardName}`,
        postUrlAllow: (u, base) => (u.hostname === base.hostname) && u.pathname.startsWith(`/${opts.boardName}`),
        list: {
            container: "#fboardlist",
            item: ".list-group-item",
            postLink: "a.da-article-link",
            timestampHint: ".wr-date .da-list-date, .wr-date .orangered, .wr-date, time[datetime]",
            skipNotice: 'img[alt="공지"]',
            skipPromo: 'span:has-text("홍보")',
            // 제목에 "삭제된 게시물"이 포함되거나 dim 처리된 아이템 스킵
            skipDeleted: 'a.da-article-link:has-text("삭제된 게시물"), li.list-group-item.opacity-50 a.da-article-link',
            pageParam: "page",
            nextPageSelectors: [
                'li.page-item.active + li a.page-link',
                'a.page-link:has-text("다음")',
                'a[aria-label="다음"]',
                'a[rel="next"]',
            ],
        },
        detail: {
            content: "#bo_v_con",
            title: "#bo_v_title",
            titleCategoryLink: '#bo_v_title a',
            author: ".sv_member",
            authorAvatarSelectors: [".sv_member img.mb-photo", ".member-photo-container img"],
            commentAvatarSelectors: ["header img.mb-photo"],
            timestampIsSiblingOfAuthor: true,
            timestampSelectors: ['span.orangered', 'time[datetime]'],
            viewCount: 'div.pe-2.text-center:has(i.bi-eye)',
            likeCount: 'div.pe-2.text-center:has(i.bi-hand-thumbs-up)',
            dislikeCount: undefined,
            commentLikeCount: '.da-reaction__count .cnt',
            commentDislikeCount: undefined,
            // 좋아요 카운트(btn-group) 제외를 위해 span 한정
            commentReactionBadgeCounts: 'span.da-reaction__badge .da-reaction__count',
            commentsContainer: 'section#bo_vc',
            commentArticle: 'article[id^="c_"]',
            commentAuthor: 'header .sv_name, header .comment-author, header .wr-name',
            // 신규 포맷(div.ms-auto[title]) 우선 처리 + 기존 포맷 유지
            commentTime: 'header div.ms-auto',
            commentContentHtml: '.comment-content .na-convert',
            commentRawTextareaPrefix: '#save_comment_',
            tagSelectors: ['.tag a', '.tags a', '.bo_v_tag a', '.na-tag a'],
            imageSelectors: ['#bo_v_con img', '#bo_v_img img'],
            iframeSelectors: ['#bo_v_con iframe[src]'],
            twitterSelectors: ['blockquote.twitter-tweet'],
            restrictedTextProbes: ['#bo_v_con'],
            restrictedTextRegex: [/권한이없|삭제|존재하지|비공개/], // 사이트 전용 기본값
        },
    };
}

// ===[ 라우터 ]================================================================
export function createRouter(cfg: BoardConfig, outDatasetId = 'posts') {
    const router = createPlaywrightRouter();

    // 실행(run) 단위 상태 (KVStore가 아닌 메모리)
    // - 동일 실행 내 LIST 재방문 방지
    // - 페이지 카운트
    const seenListUrlsInRun = new Set<string>();
    let pagesCrawledInRun = 0;

    // 목록 핸들러: 링크 수집 및 페이지네이션
    router.addHandler("LIST", async ({ page, enqueueLinks, log, request }) => {
        const { mode, cutoffDate, maxPages } = request.userData;
        log.info(`목록 페이지 처리: ${page.url()} (모드: ${mode}, 보드: ${cfg.BOARD_NAME})`);
        // Stage-1: conservative timeouts + TS __name guard in page context
        await page.setDefaultNavigationTimeout(45_000);
        await page.setDefaultTimeout(12_000);
        try {
            await page.addInitScript(() => {
                (window as any).__name ||= ((fn: any, _n: string) => fn);
            });
        } catch {}
        try {
            await page.evaluate(() => {
                (window as any).__name ||= ((fn: any, _n: string) => fn);
            });
        } catch {}
        // Proceed once DOM is parsed to avoid waiting for network idle
        try { await page.waitForLoadState('domcontentloaded', { timeout: 5_000 }); } catch {}

        const stateStore = await KeyValueStore.open(cfg.STATE_STORE_NAME);
        const lastStopId = mode === 'incremental' ? await stateStore.getValue("lastStopId") : null;

        // 무한 루프 방지: 동일 실행 내 LIST URL 재방문 차단 (메모리)
        const currentListUrl = new URL(page.url()).toString();
        if (seenListUrlsInRun.has(currentListUrl)) {
            log.warning?.(`LIST: 동일 실행 내 재방문 감지 → 중단: ${currentListUrl}`);
            return;
        }
        seenListUrlsInRun.add(currentListUrl);

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
        let parsedCountOnPage = 0;

        for (const postElement of postElements) {
            // --- 스킵 규칙 ---
            if (cfg.list.skipNotice && await postElement.$(cfg.list.skipNotice)) continue;
            if (cfg.list.skipPromo && await postElement.$(cfg.list.skipPromo)) continue;
            // 삭제된 게시물 스킵 (리스트에서 시간/메타가 비어 있어 파싱 실패 유발)
            if (cfg.list.skipDeleted && await postElement.$(cfg.list.skipDeleted)) {
                try { log.debug?.('삭제된 게시물 스킵(리스트)'); } catch { }
                continue;
            }

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

            const postIdMatch = postUrl.match(/(\d+)(?=\/?$)/);
            if (!postIdMatch) continue;
            const postId = postIdMatch[1];

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

                const trimmed = tsText.replace('등록', '').trim();
                let postTimestamp: string | null = null;
                try {
                    postTimestamp = parseKstToUtcIso(trimmed, `LIST ${cfg.BOARD_NAME} ${page.url()} postId=${postId}`);
                    // DEBUG-only success trace
                    try { log.debug?.(`[timestamp][LIST] parsed ok: postId=${postId} ts="${postTimestamp}" from hint="${trimmed}" url=${page.url()}`); } catch { /* ignore */ }
                } catch (err) {
                    // Fail-open: log and skip cutoff check for this item
                    log.debug?.(`[timestamp][LIST] parse failed for postId=${postId} hint="${trimmed}" url=${page.url()} err=${String(err)}`);
                    postTimestamp = null;
                }

                if (postTimestamp) {
                    parsedCountOnPage++;
                    if (postTimestamp < cutoffDate) {
                        shouldStop = true;
                        log.info(`Full: Cutoff 날짜(${cutoffDate}) 이전 게시물에 도달하여 중단합니다. (post: ${postTimestamp}, postId: ${postId})`);
                        break;
                    }
                }
            }

            await enqueueLinks({ urls: [postUrl], label: "DETAIL", userData: { postId } });
        }

        if (mode === 'full' && cutoffDate && !isFirstPage && parsedCountOnPage === 0) {
            shouldStop = true;
            log.warning?.('[timestamp][LIST] 이 페이지에서 타임스탬프를 하나도 파싱하지 못했습니다. 비정상 DOM으로 판단하고 다음 페이지 이동을 중단합니다.');
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
                let enqueued = false;
                for (const sel of cfg.list.nextPageSelectors || []) {
                    try {
                        const next = page.locator(sel).first();
                        if (await next.count()) {
                            const href = await next.getAttribute('href');
                            if (href) {
                                const nextUrl = new URL(href, page.url()).toString();
                                if (nextUrl === page.url()) {
                                    log.warning?.('다음 페이지 링크가 현재 페이지와 동일하여 스킵합니다.');
                                    continue;
                                }
                                await enqueueLinks({
                                    urls: [nextUrl],
                                    label: 'LIST',
                                    // 기존 기준(mode, cutoffDate, maxPages 등) 유지
                                    userData: request.userData,
                                });
                                log.info(`다음 페이지 큐에 추가: ${nextUrl}`);
                                enqueued = true;
                                break;
                            }
                        }
                    } catch { /* try next selector */ }
                }
                if (!enqueued) log.info('다음 페이지 링크를 찾지 못했거나 마지막 페이지입니다.');
            }
        }
    });

    // 상세 핸들러: 콘텐츠/메타/댓글 수집
    router.addHandler("DETAIL", async ({ request, page, log }) => {
        log.info(`상세 페이지 처리: ${request.loadedUrl} (보드: ${cfg.BOARD_NAME})`);
        // Stage-1: conservative timeouts + TS __name guard in page context
        await page.setDefaultNavigationTimeout(45_000);
        await page.setDefaultTimeout(12_000);
        try {
            await page.addInitScript(() => {
                (window as any).__name ||= ((fn: any, _n: string) => fn);
            });
        } catch {}
        try {
            await page.evaluate(() => {
                (window as any).__name ||= ((fn: any, _n: string) => fn);
            });
        } catch {}
        // Proceed once DOM is parsed to avoid waiting for network idle
        try { await page.waitForLoadState('domcontentloaded', { timeout: 5_000 }); } catch {}
        const { postId } = request.userData;

        await page.waitForSelector(cfg.detail.content, { timeout: 10_000 });

        // --- 제목/카테고리 ---
        let category: string | null = null;
        try {
            if (cfg.detail.titleCategoryLink) {
                category = (await page.locator(cfg.detail.titleCategoryLink).first().textContent({ timeout: 1000 }))?.trim() ?? null;
            }
        } catch { }

        const fullTitle = await page.locator(cfg.detail.title).textContent();
        const title = fullTitle?.replace(category ?? '', '').replace(/\s*\[\d+\]\s*$/, '').trim();

        // --- 삭제/권한 제한 빠른 감지 (제목/본문 기반) ---
        try {
            const titleProbe = (fullTitle || '').trim();
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

        // --- 작성자/시간 ---
        const author = (await page.locator(cfg.detail.author).first().textContent())?.replace(/\s+/g, ' ').trim() || null;

        // 아바타 (설정 배열 우선순위로 검색)
        let avatar: string | null = null;
        for (const sel of cfg.detail.authorAvatarSelectors || []) {
            try {
                const attr = await page.locator(sel).first().getAttribute('src');
                if (attr && attr.trim()) { avatar = attr.trim(); break; }
            } catch { }
        }

        let rawTimestamp: string | null = null;
        if (cfg.detail.timestampIsSiblingOfAuthor) {
            try {
                rawTimestamp = await page
                    .locator(cfg.detail.author)
                    .first()
                    .evaluate((el) =>
                        (el.nextSibling && (el.nextSibling as any).textContent)
                            ? (el.nextSibling as any).textContent.trim().replace('등록', '')
                            : null
                    );
            } catch { /* ignore */ }
        }
        if (!rawTimestamp || !rawTimestamp.trim()) {
            for (const sel of cfg.detail.timestampSelectors || []) {
                try {
                    const loc = page.locator(sel).first();
                    if (await loc.count()) {
                        const attrDatetime = await loc.getAttribute('datetime');
                        const attrTitle = await loc.getAttribute('title');
                        const text = await loc.textContent();
                        rawTimestamp = (attrDatetime || attrTitle || text || '').trim();
                        if (rawTimestamp) break;
                    }
                } catch { /* try next */ }
            }
        }
        const timestamp = parseKstToUtcIso(rawTimestamp || '', `DETAIL ${cfg.BOARD_NAME} ${request.loadedUrl} postId=${postId}`);

        // --- 본문/메타 ---
        const view_count = cfg.detail.viewCount
            ? extractInt(await page.locator(cfg.detail.viewCount).first().textContent())
            : null;
        const like_count = cfg.detail.likeCount
            ? extractInt(await page.locator(cfg.detail.likeCount).first().textContent())
            : null;
        const dislike_count = cfg.detail.dislikeCount
            ? extractInt(await page.locator(cfg.detail.dislikeCount).first().textContent())
            : 0;

        const contentNode = page.locator(cfg.detail.content).first();
        const content = (await contentNode.innerText()).trim();
        const contentHtml = (await contentNode.innerHTML()).trim();

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
            try {
                const hasContainer = cfg.detail.commentsContainer
                    ? await page.waitForSelector(cfg.detail.commentsContainer, { timeout: 3000 }).then(() => true).catch(() => false)
                    : false;
                if (!hasContainer || !cfg.detail.commentArticle) return { topLevel: [], flatCount: 0 };

                const raws = await page.$$eval(
                    cfg.detail.commentArticle,
                    (nodes, opts) => {
                        const o = opts as any;
                        const avatarSels: string[] = o.commentAvatarSelectors || [];
                        const selAuthor: string | undefined = o.commentAuthor;
                        const selTime: string | undefined = o.commentTime;
                        const selHtml: string | undefined = o.commentContentHtml;
                        const rawPrefix: string | undefined = o.rawPrefix;
                        const selLike: string | undefined = o.commentLikeCount;
                        const selDislike: string | undefined = o.commentDislikeCount;
                        const selReaction: string | undefined = o.reactionBadgeSel;

                        const toInt = (s: string | null | undefined) => Number((s || '').replace(/\D/g, '')) || 0;

                        return (nodes as Element[]).map((node) => {
                            const id = (node.getAttribute('id') || '').replace(/^c_/, '');

                            // depth 추정 (rem → px 보정)
                            const styleAttr = node.getAttribute('style') || '';
                            let depth = 0;
                            let m = styleAttr.match(/margin-left\s*:\s*([\d.]+)\s*rem/i);
                            if (m) depth = Math.round(parseFloat(m[1]));
                            else {
                                m = styleAttr.match(/margin-left\s*:\s*([\d.]+)\s*px/i);
                                if (m) depth = Math.max(1, Math.round(parseFloat(m[1]) / 16));
                            }

                            // 작성자
                            let author: string | null = null;
                            if (selAuthor) {
                                const el = (node as Element).querySelector(selAuthor);
                                author = el ? (el.textContent || '').replace(/\s+/g, ' ').trim() : null;
                            }

                            // 아바타 (우선순위 리스트)
                            let avatar: string | null = null;
                            for (const s of avatarSels) {
                                const img = (node as Element).querySelector(s) as HTMLImageElement | null;
                                if (img && img.src && img.src.trim()) { avatar = img.src.trim(); break; }
                            }

                            // 시간(raw)
                            let createdRaw: string | null = null;
                            if (selTime) {
                                const t = (node as Element).querySelector(selTime) as HTMLElement | null;
                                if (t) createdRaw = (t.getAttribute('title') || t.textContent || '').trim();
                            }

                            // 추천/비추천/리액션 합산
                            const likeCount = selLike ? (() => {
                                const e = (node as Element).querySelector(selLike) as HTMLElement | null;
                                return toInt(e?.textContent || '0');
                            })() : 0;

                            const dislikeCount = selDislike ? (() => {
                                const e = (node as Element).querySelector(selDislike) as HTMLElement | null;
                                return toInt(e?.textContent || '0');
                            })() : 0;

                            let reactionCount = 0;
                            const rSel = selReaction || 'span.da-reaction__badge .da-reaction__count';
                            const cands = Array.from((node as Element).querySelectorAll(rSel));
                            const filtered = cands.filter((s) => {
                                const badge = (s as HTMLElement).closest('.da-reaction__badge');
                                return !!badge && badge.tagName === 'SPAN';
                            });
                            for (const s of filtered) reactionCount += toInt((s as HTMLElement).textContent || '0');

                            // 본문 HTML/텍스트
                            let html: string | null = null;
                            let text: string | null = null;
                            if (selHtml) {
                                const c = (node as Element).querySelector(selHtml) as HTMLElement | null;
                                html = c ? c.innerHTML : null;
                                text = c ? (c.textContent || '').trim() : null;
                            }

                            // raw textarea (댓글 수정폼 등) – 문서 전역에서 id로 조회
                            let raw: string | null = null;
                            if (rawPrefix && id) {
                                const t = document.querySelector(`textarea${rawPrefix}${id}`) as HTMLTextAreaElement | null;
                                raw = t ? ((t.value || t.textContent || '').trim() || null) : null;
                            }
                            if (raw) text = raw; // raw 우선

                            return { cid: id, depth, author, avatar, createdRaw, likeCount, dislikeCount, reactionCount, html, raw, text };
                        });
                    },
                    {
                        commentAvatarSelectors: cfg.detail.commentAvatarSelectors || [],
                        commentAuthor: cfg.detail.commentAuthor,
                        commentTime: cfg.detail.commentTime,
                        commentContentHtml: cfg.detail.commentContentHtml,
                        rawPrefix: cfg.detail.commentRawTextareaPrefix,
                        commentLikeCount: cfg.detail.commentLikeCount,
                        commentDislikeCount: cfg.detail.commentDislikeCount,
                        reactionBadgeSel: cfg.detail.commentReactionBadgeCounts,
                    }
                );

                // Node 컨텍스트: 파싱 + 트리 구성
                const flat: Comment[] = raws.map((r: any) => ({
                    id: `${cfg.SITE_NAME}-${cfg.BOARD_NAME}-${postId}_${r.cid}`,
                    author: r.author,
                    avatar: r.avatar,
                    content: r.text,
                    timestamp: parseKstToUtcIso(r.createdRaw, `COMMENT ${cfg.BOARD_NAME} ${request.loadedUrl} postId=${postId} cid=${r.cid}`),
                    isReply: (r.depth ?? 0) > 0,
                    replies: [],
                    likeCount: r.likeCount,
                    dislikeCount: r.dislikeCount,
                    reactionCount: r.reactionCount,
                    raw: r.raw,
                    html: r.html,
                    depth: r.depth ?? 0,
                    parentId: null,
                }));

                const byId = new Map<string, Comment>();
                const topLevel: Comment[] = [];
                const stack: { id: string; depth: number }[] = [];
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

                return { topLevel, flatCount: flat.length };
            } catch (error) {
                log.error('전체 댓글 수집 중 오류', { error, url: request.loadedUrl });
                return { topLevel: [], flatCount: 0 };
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
    const urls = await page.$$eval(sel, (imgs) =>
        imgs.map((img) => {
            const el = img as HTMLImageElement;
            const set = new Set<string>();
            const ds = el.getAttribute('data-src'); if (ds) set.add(ds);
            const s = el.getAttribute('src'); if (s) set.add(s);
            const ss = el.getAttribute('srcset'); if (ss) ss.split(',').forEach((part) => set.add(part.trim().split(' ')[0]));
            return Array.from(set);
        }).flat()
    ).catch(() => [] as string[]);
    return normalizeAbsolute(base, urls);
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
            `${cfg.detail.commentsContainer} a[href]`
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
                try { await f.waitForSelector('video[poster], [data-testid="tweetText"]', { timeout: 5000 }); } catch { }
                try { await f.waitForSelector('[data-testid="tweetText"]', { timeout: 2000 }); } catch { }

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
        const videoNodesSel = `${cfg.detail.content} video`;
        const videos = await page.$$eval(videoNodesSel, (nodes) => {
            return nodes.map((el) => {
                const v = el as HTMLVideoElement;
                const poster = v.getAttribute('poster');
                const src = v.getAttribute('src');
                const sources: { src: string | null; type: string | null }[] = [];
                v.querySelectorAll('source').forEach((s) => {
                    sources.push({
                        src: s.getAttribute('src'),
                        type: s.getAttribute('type'),
                    });
                });
                return { poster, src, sources };
            });
        }).catch(() => [] as any[]);
        for (const v of videos) {
            // Compose candidate sources: [src] + <source>
            const candidateSrcs: { url: string | null; type: string | null }[] = [];
            if (v.src) candidateSrcs.push({ url: v.src, type: null });
            for (const s of v.sources) candidateSrcs.push(s);
            const posterAbs = toAbsoluteUrl(base, v.poster);
            for (const cand of candidateSrcs) {
                const abs = toAbsoluteUrl(base, cand.url);
                if (!abs) continue;
                const isMp4ByExt = abs.toLowerCase().endsWith('.mp4');
                const isMp4ByType = (cand.type || '').toLowerCase().includes('mp4');
                const mime = cand.type || (isMp4ByExt ? 'video/mp4' : null);
                // Only record real video-like URLs (prefer mp4, but allow others with explicit type)
                if (mime || isMp4ByExt) {
                    embeds.push({
                        type: 'video',
                        url: abs,
                        provider: 'html5',
                        videoId: null,
                        thumbnail: posterAbs || null,
                        title: null,
                        description: null,
                        mimeType: mime || null,
                    });
                }
            }
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
