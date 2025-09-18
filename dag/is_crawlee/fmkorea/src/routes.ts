import {
    createPlaywrightRouter,
    Dataset,
    KeyValueStore,
} from "crawlee";
import { createHash } from "node:crypto";

/**
 * ---------------------------------------------------------------------------
 * FMKorea 베스트(/best) 라우터 (std-optimized)
 *
 * 목적
 *  - FMKorea 게시판용 **셀렉터/규칙** 과 Playwright 라우터(LIST/DETAIL)를 제공합니다.
 *
 * 유지보수 원칙 (⚠ 결과 JSON 불변)
 *  - 결과 JSON 스키마/값을 변경하지 않습니다.
 *  - 셀렉터는 방어적으로, 폴백을 충분히 두고 구현합니다.
 * ---------------------------------------------------------------------------
 */

// ===[ 고정 상수 ]============================================================
const HASH_LEN_POST_ID = 16;
const KST_OFFSET_MIN = 9 * 60; // KST(UTC+9)
const YT_ID_RE = /(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;

// ===[ 타입 정의 ]============================================================
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
    raw?: string | null;
    avatar?: string | null;
    html?: string | null;
    reactions?: number;
    depth?: number;
    parentId?: string | null;
};

export type Embedded = {
    type: string; // youtube, x, iframe, etc.
    url: string;
    provider?: string | null;
    videoId?: string | null;
    thumbnail?: string | null;
    title?: string | null;
    description?: string | null;
    mimeType?: string | null;
};

export type BoardConfig = {
    SITE_NAME: string;
    BOARD_NAME: string;
    STATE_STORE_NAME: string;
    baseListUrl: string;
};

// ===[ 보조 유틸 ]===========================================================
function toAbsoluteUrl(base: string, url: string | null | undefined): string | null {
    if (!url) return null;
    const u = url.trim();
    if (!u) return null;
    if (u.startsWith('//')) return 'https:' + u;
    try { return new URL(u, base).toString(); } catch { return null; }
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

function toUtcIsoFromKst(y: number, m: number, d: number, hh = 0, mm = 0, ss = 0): string {
    const dateKst = Date.UTC(y, m - 1, d, hh, mm, ss) - KST_OFFSET_MIN * 60 * 1000;
    return new Date(dateKst).toISOString();
}

/**
 * KST 문자열 파서 (FMK 공용 패턴)
 * 허용:
 *  - HH:mm (오늘)
 *  - YYYY.MM.DD
 *  - YYYY.MM.DD HH:mm
 *  - YYYY-MM-DD HH:mm[:ss]
 *  - "N분 전" / "N시간 전" / 숫자만(N은 분)
 *  - <time datetime="...">
 */
function parseKstToUtcIso(dateStr: string | null | undefined): string | null {
    if (!dateStr) return null;
    // 공백/표기 정규화: NBSP 등 → 공백, 다중 공백 → 단일 공백, 접두어 제거
    let s = dateStr.replace(/\u00A0/g, ' ').replace(/\s+/g, ' ').replace('등록', '').replace(/^약\s+/, '').trim();

    // 상대표현: "방금 전" → 1분 전 처리
    if (/방금\s*전$/.test(s)) return new Date(Date.now() - 60_000).toISOString();

    // N분 전 / N시간 전 / N일 전 / 숫자만(N은 분)
    let m = s.match(/^(\d{1,3})\s*분\s*전$/);
    if (m) return new Date(Date.now() - parseInt(m[1], 10) * 60_000).toISOString();
    m = s.match(/^(\d{1,3})\s*시간\s*전$/);
    if (m) return new Date(Date.now() - parseInt(m[1], 10) * 3_600_000).toISOString();
    m = s.match(/^(\d{1,3})\s*일\s*전$/);
    if (m) return new Date(Date.now() - parseInt(m[1], 10) * 86_400_000).toISOString();
    m = s.match(/^(\d{1,3})$/);
    if (m) return new Date(Date.now() - parseInt(m[1], 10) * 60_000).toISOString();

    // time[datetime]
    if (/T\d{2}:\d{2}/.test(s) || /Z$/.test(s)) {
        const d = new Date(s);
        if (!isNaN(d.getTime())) return d.toISOString();
    }

    const nowUtc = new Date();
    const nowKst = new Date(nowUtc.getTime() + KST_OFFSET_MIN * 60 * 1000);
    const kY = nowKst.getUTCFullYear();
    const kM = nowKst.getUTCMonth() + 1;
    const kD = nowKst.getUTCDate();

    // HH:mm (오늘)
    m = s.match(/^(\d{2}):(\d{2})$/);
    if (m) return toUtcIsoFromKst(kY, kM, kD, parseInt(m[1], 10), parseInt(m[2], 10));

    // YYYY.MM.DD
    m = s.match(/^(\d{4})\.(\d{2})\.(\d{2})$/);
    if (m) return toUtcIsoFromKst(parseInt(m[1], 10), parseInt(m[2], 10), parseInt(m[3], 10));

    // YYYY.MM.DD HH:mm
    m = s.match(/^(\d{4})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})$/);
    if (m) return toUtcIsoFromKst(parseInt(m[1], 10), parseInt(m[2], 10), parseInt(m[3], 10), parseInt(m[4], 10), parseInt(m[5], 10));

    // YYYY-MM-DD HH:mm[:ss]
    m = s.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?$/);
    if (m) return toUtcIsoFromKst(parseInt(m[1], 10), parseInt(m[2], 10), parseInt(m[3], 10), parseInt(m[4], 10), parseInt(m[5], 10), m[6] ? parseInt(m[6], 10) : 0);

    const d = new Date(s);
    if (!isNaN(d.getTime())) return d.toISOString();

    return null;
}

async function getYouTubeInfo(videoId: string): Promise<{ title: string | null; thumbnail: string }> {
    // 제목은 알 수 없을 수 있으므로 null 허용, 썸네일은 hqdefault
    return { title: null, thumbnail: `https://img.youtube.com/vi/${videoId}/hqdefault.jpg` };
}

function extractMidFromUrl(pageUrl: string): string {
    try {
        const u = new URL(pageUrl);
        const qpMid = u.searchParams.get('mid');
        if (qpMid) return qpMid;              // ?mid=best, best2, ...
        const segs = u.pathname.split('/').filter(Boolean);
        if (segs.length > 0) {
            // /best/8874..., /best2/..., /humor/...
            if (/^best2?$/.test(segs[0])) return segs[0];
            return segs[0]; // 다른 게시판 대비 (humor 등)
        }
    } catch { }
    return 'best';
}

function buildCommentAjaxBase(mid: string, postId: string): string {
    return `https://www.fmkorea.com/?mid=${encodeURIComponent(mid)}&document_srl=${postId}`;
}

function extractLastPageFromNav(nav: any): number {
    if (!nav) return 1;
    if (typeof nav === 'object' && nav !== null) {
        const n = (nav.last_page ?? nav.total_page ?? nav.lastPage ?? nav.totalPage);
        const v = Number(n);
        return Number.isFinite(v) && v > 0 ? v : 1;
    }
    const html = String(nav);
    const m1 = [...html.matchAll(/data-page\s*=\s*"(\d{1,5})"/g)].map(x => Number(x[1]));
    const m2 = [...html.matchAll(/(?:[?&]cpage=)(\d{1,5})/g)].map(x => Number(x[1]));
    const cand = [...m1, ...m2].filter(n => Number.isFinite(n) && n > 0);
    return cand.length ? Math.max(...cand) : 1;
}

async function fetchCommentPage(page: import('playwright').Page, mid: string, postId: string, cpage: number) {
    const base = buildCommentAjaxBase(mid, postId);
    const url = `${base}&cpage=${cpage}`;
    const res = await page.request.get(url, {
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': base,
        }
    });
    const headers = res.headers();
    const ct = headers['content-type'] || headers['Content-Type'] || '';

    let json: any = null;
    try {
        if (/json/i.test(ct)) {
            json = await res.json();
        } else {
            const raw = await res.text();
            try { json = JSON.parse(raw); } catch { json = { tpl: raw }; }
        }
    } catch {
        const txt = await res.text();
        json = { tpl: txt, comment_page_navigation: null };
    }

    const tpl = String(json.tpl || '');
    const nav = (typeof json.comment_page_navigation !== 'undefined') ? json.comment_page_navigation : null;
    const totalCount = (typeof json.total_count === 'number') ? json.total_count : null;
    return { tpl, nav, totalCount };
}

async function parseCommentsFromTplHtml(page: import('playwright').Page, tplHtml: string, baseUrl: string) {
    return await page.evaluate(({ html }) => {
        const doc = new DOMParser().parseFromString(html, 'text/html');
        const lis = doc.querySelectorAll('ul.fdb_lst_ul > li[id^="comment_"]');
        const out: any[] = [];
        lis.forEach((li: any) => {
            const idAttr = li.getAttribute('id') || '';
            const idMatch = idAttr.match(/comment_(\d+)/);
            const id = (idMatch ? idMatch[1] : idAttr).trim();

            let author: string | null = null;
            const authorEl = li.querySelector('.meta a.member_plate');
            if (authorEl) author = (authorEl.textContent || '').trim() || null;

            let timeRaw = '';
            const timeEl = li.querySelector('.meta .date, time[datetime]');
            if (timeEl) {
                const dt = (timeEl.getAttribute && timeEl.getAttribute('datetime')) || timeEl.textContent || '';
                timeRaw = (dt || '').trim();
            }

            let bodyHtml = '';
            const bodyEl = li.querySelector('.comment-content .xe_content, .comment-content');
            if (bodyEl) bodyHtml = (bodyEl as HTMLElement).innerHTML.trim();

            const isBest = li.classList.contains('comment_best') || !!li.querySelector('.meta .icon-hit');

            const likeSpan = li.querySelector('.fdb_nav .voted_count');
            const dislikeSpan = li.querySelector('.fdb_nav .blamed_count');
            const likeText = (likeSpan && (likeSpan.textContent || '').trim()) || '';
            const dislikeText = (dislikeSpan && (dislikeSpan.textContent || '').trim()) || '';

            let parentId: string | null = null;
            const parentA = li.querySelector('.comment-content a.findParent[onclick]');
            if (parentA) {
                const on = parentA.getAttribute('onclick') || '';
                const m = on.match(/findComment\((\d+)\)/);
                if (m) parentId = m[1];
            }

            let depth: number | null = null;
            const cls = li.getAttribute('class') || '';
            const dm = cls.match(/\bcomment-(\d{1,2})\b/);
            if (dm) depth = parseInt(dm[1], 10);

            out.push({ id, author, timeRaw, bodyHtml, isBest, likeText, dislikeText, parentId, depth });
        });
        return out;
    }, { html: tplHtml, base: baseUrl });
}

function buildCommentTree(flat: any[]): Comment[] {
    // 1) 노드 사전 생성
    const nodes: Record<string, Comment> = {};
    const order = [...flat];

    // BEST/일반 섞여있고, 페이지별로 섞여 들어오니 id asc로 정렬 후 생성
    order.sort((a, b) => (parseInt(a.id, 10) - parseInt(b.id, 10)));

    for (const rc of order) {
        const tsIso = parseKstToUtcIso(rc.timeRaw) || new Date().toISOString();
        const likeCount = (() => {
            const n = (rc.likeText || '').replace(/[^\d]/g, '');
            return n ? parseInt(n, 10) : 0;
        })();
        const dislikeCount = (() => {
            const n = (rc.dislikeText || '').replace(/[^\d]/g, '');
            return n ? parseInt(n, 10) : 0;
        })();

        nodes[String(rc.id)] = {
            id: String(rc.id),
            author: rc.author || null,
            content: null,
            timestamp: tsIso,
            isReply: false,
            replies: [],
            likeCount,
            dislikeCount,
            html: rc.bodyHtml || null,
            parentId: rc.parentId || null,
            // 깊이는 신뢰하지 않고, BEST 여부는 raw로 표시
            raw: rc.isBest ? 'BEST' : null,
        };
    }

    // 2) parentId 기반으로 안전 연결 (부모가 뒤에 있어도 OK)
    const roots: Comment[] = [];
    for (const id of Object.keys(nodes)) {
        const node = nodes[id];
        const pid = node.parentId && String(node.parentId);
        const parent = pid ? nodes[pid] : undefined;
        if (parent) {
            node.isReply = true;
            parent.replies.push(node);
        } else {
            roots.push(node);
        }
    }

    return roots;
}

// ===[ 게시판 레지스트리 ]====================================================
export const BOARDS: Record<string, BoardConfig> = {
    best: {
        SITE_NAME: 'fmkorea',
        BOARD_NAME: 'best',
        STATE_STORE_NAME: 'CRAWL_STATE_FMKOREA_BEST',
        baseListUrl: 'https://www.fmkorea.com/best',
    },
};

// ===[ 라우터 ]================================================================
export function createRouter(cfg: BoardConfig, outDatasetId = 'posts') {
    const router = createPlaywrightRouter();

    // 실행(run) 단위 상태 (KVStore 대신 메모리 사용)
    let pagesCrawledInRun = 0;
    const seenListUrlsInRun = new Set<string>();

    async function savePageCountDelta(delta: number) {
        const store = await KeyValueStore.open(cfg.STATE_STORE_NAME);
        const cur = (await store.getValue<number>('pagesCrawled')) || 0;
        await store.setValue('pagesCrawled', cur + delta);
    }

    // 목록 핸들러: a[href*="/best/"] 기준 링크 수집 (방어형 필터)
    router.addHandler("LIST", async ({ page, enqueueLinks, log, request }) => {
        const { mode, cutoffDate, maxPages } = request.userData;
        log.info(`목록 페이지 처리: ${page.url()} (모드: ${mode}, 보드: ${cfg.BOARD_NAME})`);
        await page.setDefaultNavigationTimeout(45_000);
        await page.setDefaultTimeout(12_000);

        const stateStore = await KeyValueStore.open(cfg.STATE_STORE_NAME);
        const lastStopId = mode === 'incremental' ? await stateStore.getValue<string>("lastStopId") : null;

        const currentListUrl = new URL(page.url()).toString();
        if (seenListUrlsInRun.has(currentListUrl)) {
            log.warning?.(`LIST: 동일 실행 내 재방문 감지 → 중단: ${currentListUrl}`);
            return;
        }
        seenListUrlsInRun.add(currentListUrl);

        pagesCrawledInRun += 1;
        await savePageCountDelta(1);
        const hardMax = (typeof maxPages === 'number' && isFinite(maxPages)) ? maxPages : null;
        if (hardMax !== null && pagesCrawledInRun > hardMax) {
            log.info(`페이지 한도(${hardMax})를 초과하여 중단합니다. (pagesCrawled=${pagesCrawledInRun})`);
            return;
        }

        await page.waitForSelector('div.fm_best_widget', { timeout: 10000 });
        const postLinks = await page.locator('.fm_best_widget h3.title a[href]').all();

        log.info(`현재 페이지에서 ${postLinks.length}개의 게시물 링크를 찾았습니다.`);

        const base = new URL(page.url());

        let newLastStopId: string | null = null;
        let shouldStop = false;
        // 이 페이지에서 **마지막(하단) 실제 게시물**의 시간 기준으로 다음 페이지 진행 여부를 판단
        let lastNonNoticeTimestampUtc: string | null = null;

        const pageParam = 'page';
        const u = new URL(page.url());
        const isFirstPage = !u.searchParams.has(pageParam) || u.searchParams.get(pageParam) === '1';

        for (const link of postLinks) {
            const href = await link.getAttribute('href');
            if (!href) continue;

            let postUrl: string | null = null;
            try {
                postUrl = new URL(href, base).toString();
            } catch {
                continue;
            }

            let postId: string | null = null;
            try {
                const pu = new URL(postUrl);
                const m1 = pu.pathname.match(/\/best\/(\d+)/);
                const m2 = pu.search.match(/(?:^|[?&])document_srl=(\d+)/);
                postId = (m1 && m1[1]) || (m2 && m2[1]) || null;
            } catch { }
            if (!postId) continue;

            if (isFirstPage && !newLastStopId) newLastStopId = postId;

            if (mode === 'incremental' && lastStopId && postId === lastStopId) {
                shouldStop = true;
                log.info(`Incremental: 마지막 수집 게시물(ID: ${lastStopId})에 도달하여 중단합니다.`);
                break;
            }

            // LIST 단계에서 시간 힌트(span.regdate) 읽고 컷오프 적용 (full 모드 전용) + 공지/고정 감지
            let listTimestampUtc: string | null = null;
            let isNoticeLike: boolean = false;
            try {
                const res = await link.evaluate((el) => {
                    const root = (el as HTMLElement).closest('li') as HTMLElement | null;
                    const scope = root || (el as HTMLElement).parentElement || document;
                    // 공지/고정 글 감지: 클래스/뱃지/텍스트
                    let notice = false;
                    const noticeSel = ['.notice', '.notice_icon', '.fa-thumb-tack', '.fa-bullhorn', '.icon-notice', '.badge-notice', '.category'];
                    for (const s of noticeSel) {
                        const n = scope.querySelector(s) as HTMLElement | null;
                        if (!n) continue;
                        const t = (n.getAttribute('title') || n.textContent || '').replace(/\s+/g, '');
                        if (n.classList.contains('notice') || /공지|고정|Notice|Pinned/i.test(t)) {
                            notice = true; break;
                        }
                    }
                    // 후보 셀렉터: 사이트가 24시간 이내엔 상대표현, 이후엔 날짜 표기
                    const sels = ['time[datetime]', 'span.regdate', 'span.date', '.regdate', '.date'];
                    let raw = '';
                    for (const s of sels) {
                        const node = scope.querySelector(s) as HTMLElement | HTMLTimeElement | null;
                        if (!node) continue;
                        // time[datetime] 우선
                        // @ts-ignore
                        const dt = (node.getAttribute && node.getAttribute('datetime')) || '';
                        if (dt && dt.trim()) { raw = dt.trim(); break; }
                        const txt = (node.textContent || '').replace(/\u00A0/g, ' ').trim();
                        if (txt) { raw = txt; break; }
                    }
                    return { raw, notice };
                });
                if (res.raw) {
                    listTimestampUtc = parseKstToUtcIso(res.raw);
                }
                isNoticeLike = !!res.notice;
            } catch { /* ignore */ }

            // 경계 판단을 위해, 비공지이면서 타임스탬프가 있으면 우선 갱신해둔다
            if (!isNoticeLike && listTimestampUtc) {
                lastNonNoticeTimestampUtc = listTimestampUtc;
            }
            if (mode === 'full' && cutoffDate && listTimestampUtc && listTimestampUtc < cutoffDate) {
                // 현재 아이템은 컷오프 이전(더 오래됨) → 큐잉하지 않고 넘어간다.
                // 공지/고정은 무시 대상이지만, 어차피 큐잉도 건너뛰므로 동일 처리
                // (페이지 진행 여부는 페이지의 **마지막 비공지 아이템** 기준으로 결정)
                continue;
            }

            await enqueueLinks({ urls: [postUrl], label: 'DETAIL', userData: { postId, listTimestampUtc } });
        }

        if (mode === 'incremental' && isFirstPage && newLastStopId) {
            await stateStore.setValue("lastStopId", newLastStopId);
            log.info(`Incremental: 새로운 lastStopId 저장: ${newLastStopId}`);
        }

        // 다음 페이지 이동 판단:
        // - incremental/lastStopId로 인한 중단(shouldStop)
        // - full 모드: 이 페이지의 **마지막 비공지 아이템** 시간이 컷오프 이전이면 이후는 전부 더 오래됨 → 중단
        const reachedBoundary = (mode === 'full' && cutoffDate && lastNonNoticeTimestampUtc && lastNonNoticeTimestampUtc < cutoffDate);
        const stopAfterThisPage = shouldStop || !!reachedBoundary;
        if (!stopAfterThisPage) {
            if (hardMax !== null && pagesCrawledInRun >= hardMax) {
                log.info(`페이지 한도(${hardMax}) 도달: 다음 페이지를 큐에 추가하지 않습니다. (pagesCrawled=${pagesCrawledInRun})`);
            } else {
                let nextUrl: string | null = null;
                try {
                    const cur = new URL(page.url());
                    const curPage = parseInt(cur.searchParams.get('page') || '1', 10) || 1;
                    cur.searchParams.set('page', String(curPage + 1));
                    nextUrl = cur.toString();
                } catch { }
                if (nextUrl) {
                    await enqueueLinks({ urls: [nextUrl], label: 'LIST', userData: request.userData });
                    log.info(`다음 페이지 큐에 추가: ${nextUrl}`);
                } else {
                    log.info('다음 페이지 URL을 구성할 수 없어 마지막 페이지로 간주합니다.');
                }
            }
        } else {
            if (reachedBoundary) {
                log.info(`Full: 경계 도달 → 다음 페이지는 큐잉하지 않습니다. (lastItemTs=${lastNonNoticeTimestampUtc}, cutoff=${cutoffDate})`);
            }
        }
    });

    // 상세 핸들러
    router.addHandler("DETAIL", async ({ page, log, request }) => {
        const dataset = await Dataset.open(outDatasetId);
        const { postId, cutoffDate } = request.userData as any;
        await page.setDefaultNavigationTimeout(45_000);
        await page.setDefaultTimeout(12_000);

        if (!postId) {
            log.error(`Mandatory postId is missing for URL: ${page.url()}`);
            return;
        }

        const mid = extractMidFromUrl(page.url());

        // 기본 메타
        const baseUrl = page.url();

        // 타이틀
        const title = (await (async () => {
            const metaOg = await page.$('meta[property="og:title"][content]');
            if (metaOg) {
                const c = await metaOg.getAttribute('content');
                if (c && c.trim()) return c.trim();
            }
            const sels = [
                '#content .rd_hd h1',
                '#content h1',
                'h1',
                '#content .title h1',
            ];
            for (const s of sels) {
                try {
                    const el = await page.$(s);
                    if (!el) continue;
                    const t = (await el.textContent())?.trim();
                    if (t) return t;
                } catch { }
            }
            return null;
        })()) || null;

        // 작성자
        const author = (await (async () => {
            const tMeta = await page.$('meta[name="twitter:creator"][content], meta[property="twitter:creator"][content]');
            if (tMeta) {
                const c = await tMeta.getAttribute('content');
                if (c && c.trim()) return c.trim();
            }
            const sels = [
                '#content .rd_hd .nickname [title]',
                '#content .rd_hd .nickname',
                '.rd_hd .nickname',
                '.author',
                '.nickname',
            ];
            for (const s of sels) {
                const el = await page.$(s);
                if (!el) continue;
                const t = (await el.getAttribute('title')) || (await el.textContent()) || '';
                if (t && t.trim()) return t.trim();
            }
            return null;
        })()) || null;

        // 등록 시간 (여러 후보 텍스트에서 KST 파싱)
        const timestamp = (await (async () => {
            // <time datetime>
            const te = await page.$('time[datetime]');
            if (te) {
                const dt = await te.getAttribute('datetime');
                if (dt) {
                    const d = new Date(dt);
                    if (!isNaN(d.getTime())) return d.toISOString();
                }
            }
            const sels = [
                '#content .rd_hd .date',
                '.rd_hd .date',
                '.view_info .date',
                'span.date',
                'span.regdate',
            ];
            for (const s of sels) {
                try {
                    const el = await page.$(s);
                    if (!el) continue;
                    const raw = (await el.textContent())?.trim() || '';
                    const iso = parseKstToUtcIso(raw);
                    if (iso) return iso;
                } catch { }
            }
            // meta로는 시간 노출이 없을 수 있음 → 실패 허용
            return null;
        })()) || null;

        // LIST에서 전달된 시간 폴백 사용 (상세에서 실패 시)
        const listTimestampUtc = (request.userData as any).listTimestampUtc as string | undefined;
        if (!timestamp && listTimestampUtc) {
            (timestamp as any) = listTimestampUtc;
        }

        // Cutoff 가드: 상세 파싱 비용을 줄이기 위해 가능한 한 이른 시점에 중단
        if (cutoffDate && timestamp && timestamp < cutoffDate) {
            return;
        }

        // 본문 컨테이너 (우선순위 순으로 시도)
        const contentHandle = await (async () => {
            const sels = [
                '#content .rd_body .xe_content',
                '#content .rd_body',
                '.rd_body .xe_content',
                '.rd_body',
                'article',
            ];
            for (const s of sels) {
                const h = await page.$(s);
                if (h) return h;
            }
            return null;
        })();

        // 본문 HTML / 텍스트 / 이미지 / 임베드
        let content: string | null = null;
        let contentHtml: string | null = null;
        let images: string[] = [];
        const embeddedContent: Embedded[] = [];
        if (contentHandle) {
            try { content = (await contentHandle.innerText()).trim(); } catch { }
            try { contentHtml = (await contentHandle.innerHTML())?.trim() || null; } catch { }
            try {
                const srcs = await contentHandle.$$eval('img[src]', (imgs, base) => {
                    const out: string[] = [];
                    for (const im of imgs as HTMLImageElement[]) {
                        const s = im.getAttribute('src') || im.dataset.original || '';
                        if (s && !s.startsWith('data:')) {
                            try { out.push(new URL(s, base).toString()); } catch { }
                        }
                    }
                    return out;
                }, baseUrl);
                images = uniq(srcs);
                // 필터: 사이트에서 자동 삽입되는 1px 투명 트래킹 이미지 제거
                images = images.filter(u => !/https?:\/\/image\.fmkorea\.com\/classes\/lazy\/img\/transparent\.gif(?:\?.*)?$/.test(u));
            } catch (e) {
                log.debug(`Image extraction failed: ${(e as Error).message}`);
            }

            try {
                const iframes = await contentHandle.$$eval('iframe[src]', (ifs) => ifs.map(i => i.getAttribute('src') || '').filter(Boolean));
                for (const src of iframes) {
                    const abs = toAbsoluteUrl(baseUrl, src);
                    if (!abs) continue;
                    const m = abs.match(YT_ID_RE);
                    if (m) {
                        const videoId = m[1];
                        const info = await getYouTubeInfo(videoId);
                        embeddedContent.push({ type: 'youtube', url: abs, provider: 'YouTube', videoId, thumbnail: info.thumbnail, title: info.title });
                    } else {
                        embeddedContent.push({ type: 'iframe', url: abs });
                    }
                }
            } catch (e) {
                log.debug(`Iframe extraction failed: ${(e as Error).message}`);
            }

            try {
                const tw = await contentHandle.$$eval('blockquote.twitter-tweet a[href]', (as) => as.map(a => a.getAttribute('href') || '').filter(Boolean));
                for (const u of tw) {
                    const abs = toAbsoluteUrl(baseUrl, u);
                    if (abs) {
                        embeddedContent.push({ type: 'x', url: abs, provider: 'X' });
                    }
                }
            } catch (e) {
                log.debug(`Twitter embed extraction failed: ${(e as Error).message}`);
            }

            try {
                const videos = await contentHandle.$$eval('video > source[src]', (sources) => sources.map(s => s.getAttribute('src') || '').filter(Boolean));
                for (const src of videos) {
                    const abs = toAbsoluteUrl(baseUrl, src);
                    if (abs) {
                        embeddedContent.push({ type: 'video', url: abs });
                    }
                }
            } catch (e) {
                log.debug(`Video extraction failed: ${(e as Error).message}`);
            }
        }
        // --- content_hash: compute after contentHtml ---
        const content_hash = createHash('md5').update(contentHtml || '').digest('hex');

        // 게시글 지표(조회/추천/댓글)
        const extractNumber = (text: string | null) => text ? parseInt(text.replace(/[^\d]/g, ''), 10) : null;

        const view_count = await page.locator('.rd_hd .btm_area .side.fr span:has-text("조회 수") b').first().textContent().then(extractNumber).catch(() => null);
        const like_count = await page.locator('.fm_vote .new_voted_count').first().textContent().then(extractNumber).catch(() => null);
        let headerCommentCount = await page.locator('.rd_hd .btm_area .side.fr span:has-text("댓글") b').first().textContent().then(extractNumber).catch(() => null);

        // 댓글: AJAX 페이지(cpage) 전부 수집하여 트리 구성
        let comments: Comment[] = [];
        let ajaxTotalCount: number | null = null;

        try {
            if (postId) {
                // 1) 1페이지로 네비 파악
                const first = await fetchCommentPage(page, mid, postId, 1);
                const firstFlat = await parseCommentsFromTplHtml(page, first.tpl, baseUrl);
                let totalPage = extractLastPageFromNav(first.nav);
                let ajaxTotalCount: number | null = (typeof (first as any).totalCount === 'number') ? (first as any).totalCount : null;

                // 2) 수집 + 중복 제거 준비
                const seen = new Set<string>();
                const allFlat: any[] = [];
                for (const it of firstFlat) { const k = String(it.id); if (!seen.has(k)) { seen.add(k); allFlat.push(it); } }

                if (totalPage > 1) {
                    // 병렬 수집
                    const tasks: Promise<any>[] = [];
                    for (let p = 2; p <= totalPage; p++) {
                        tasks.push((async () => {
                            const { tpl } = await fetchCommentPage(page, mid, postId!, p);
                            return await parseCommentsFromTplHtml(page, tpl, baseUrl);
                        })());
                    }
                    const pagesFlat = await Promise.all(tasks);
                    for (const arr of pagesFlat) {
                        for (const it of arr) {
                            const k = String(it.id);
                            if (!seen.has(k)) { seen.add(k); allFlat.push(it); }
                        }
                    }
                } else {
                    // 네비 정보를 알 수 없는 경우: 더 이상 새 댓글이 나오지 않을 때까지 순차 수집 (안전 상한 200페이지)
                    for (let p = 2; p <= 200; p++) {
                        const { tpl } = await fetchCommentPage(page, mid, postId!, p);
                        if (!tpl || !tpl.trim()) break;
                        const arr = await parseCommentsFromTplHtml(page, tpl, baseUrl);
                        let added = 0;
                        for (const it of arr) {
                            const k = String(it.id);
                            if (!seen.has(k)) { seen.add(k); allFlat.push(it); added++; }
                        }
                        if (added === 0) break;
                    }
                }

                comments = buildCommentTree(allFlat);
            }
        } catch { }

        // AJAX total_count가 있으면 헤더 카운트 보정
        if (ajaxTotalCount !== null) {
            headerCommentCount = ajaxTotalCount;
        }

        const crawledAt = new Date().toISOString();
        const idSource = `${cfg.SITE_NAME}-${cfg.BOARD_NAME}-${postId}`;
        const id = createHash("md5").update(idSource).digest("hex").substring(0, HASH_LEN_POST_ID);

        const post: any = {
            id,
            post_id: String(postId),
            url: baseUrl,
            site: cfg.SITE_NAME,
            board: cfg.BOARD_NAME,
            title,
            author,
            timestamp,
            crawledAt,
            content,
            contentHtml,
            images,
            embeddedContent,
            comments,
            // 게시글 지표
            view_count,
            like_count,
            commentCountHeader: headerCommentCount,
            content_hash,
        };

        // full 모드 cutoff: 상세 timestamp 기준
        if (cutoffDate && post.timestamp && post.timestamp < cutoffDate) {
            log.info(`Full: Cutoff(${cutoffDate}) 이전 글 – 스킵: postId=${post.post_id}`);
            return; // 저장하지 않고 중단 없이 다음으로
        }

        await dataset.pushData(post);
    });

    return router;
}
