import { PlaywrightCrawler, log, Dataset, KeyValueStore, Configuration, Log } from "crawlee";
import { firefox } from 'playwright';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { createRouter, BOARDS } from "./routes.js";
import fs from 'node:fs';
import path from 'node:path';

/**
 * ---------------------------------------------------------------------------
 * FMKorea 베스트(/best) 크롤러 엔트리 (std-optimized for FMK)
 *
 * 역할
 *  - 실행 엔트리. **셀렉터/규칙** 은 routes.ts에서 정의하며,
 *    본 파일은 실행 모드/CLI 파싱/결과 저장·검증을 담당합니다.
 *
 * 모드
 *  • incremental (기본): 첫 페이지의 최신 postId를 저장, 다음 실행 시 그 지점에서 중단
 *  • full            : --hours/--minutes 기준으로 cutoff 이전 글에서 중단(상세 진입 기준)
 *  • hot             : 전달된 URL만 DETAIL 크롤
 *
 * 유지보수 원칙 (⚠ 결과 JSON 불변)
 *  - 리팩토링/정리 작업은 결과 JSON 스키마와 값의 **동일성**을 보장합니다.
 *  - 로그/주석/내부 최적화만 수행하고, 데이터 생성 로직은 변경하지 않습니다.
 * ---------------------------------------------------------------------------
 */

// ===[ 공통 상수 ]===========================================================
const BLOCKED_DOMAINS = [
    // 광고/분석 일반 차단. 단, fmkorea 하위 도메인은 허용해야 하므로 제외
    'google-analytics.com', 'googletagmanager.com', 'doubleclick.net', 'criteo.com',
    'adnxs.com', 'adform.net', 'bidswitch.net', 'adsrvr.org', 'facebook.net',
    'bing.com',
] as const;

// ===[ 유틸 ]=================================================================
/** ISO 문자열로 정규화 (파싱 불가 시 원문 유지) */
const toIso = (s?: string): string | undefined => {
    if (!s) return s as any;
    const d = new Date(s);
    return isNaN(d.getTime()) ? (s as any) : d.toISOString();
};

/** 댓글 트리를 평면 개수로 카운트 */
function countFlat(comments: any[] | undefined): number {
    if (!Array.isArray(comments)) return 0;
    let count = 0;
    const stack = [...comments];
    while (stack.length) {
        const node: any = stack.pop();
        if (!node) continue;
        count += 1;
        if (Array.isArray(node.replies)) stack.push(...node.replies);
    }
    return count;
}

/** 결과 정규화 및 간단 검증 */
function validateAndNormalize(items: any[]) {
    const seenIds = new Set<string>();
    const seenPostIds = new Set<string>();
    const issues: string[] = [];

    const posts = items.map((p, idx) => {
        const post: any = { ...p };

        // id / post_id 문자열화 및 유일성 체크
        if (typeof post.id !== 'string') post.id = String(post.id ?? '');
        if (seenIds.has(post.id)) issues.push(`dup id at index ${idx}: ${post.id}`);
        seenIds.add(post.id);

        if (typeof post.post_id !== 'string') post.post_id = String(post.post_id ?? '');
        if (seenPostIds.has(post.post_id)) issues.push(`dup post_id at index ${idx}: ${post.post_id}`);
        seenPostIds.add(post.post_id);

        // 이미지 / 임베드 / 댓글 카운트 정규화 (키 순서 보장)
        post.image_count = Array.isArray(post.images) ? post.images.length : 0;
        post.images = Array.isArray(post.images) ? post.images : [];

        post.embed_count = Array.isArray(post.embeddedContent) ? post.embeddedContent.length : 0;
        post.embeddedContent = Array.isArray(post.embeddedContent) ? post.embeddedContent : [];

        const flatCount = countFlat(post.comments);
        post.comment_count = flatCount;

        // 시간 필드 ISO 정규화(파싱 가능할 때만)
        post.timestamp = toIso(post.timestamp);
        post.crawledAt = toIso(post.crawledAt);

        return post;
    });

    return { posts, issues };
}

// ===[ CLI 파서 ]=============================================================
const args = process.argv.slice(2);
const options: { [key: string]: any } = {};
const urls: string[] = [];

let i = 0;
while (i < args.length) {
    const arg = args[i];
    if (arg.startsWith('--')) {
        const key = arg.substring(2);
        const nextArg = args[i + 1];
        if (nextArg && !nextArg.startsWith('--')) {
            options[key] = nextArg;
            i++;
        } else {
            options[key] = true;
        }
    } else {
        urls.push(arg);
    }
    i++;
}

if (urls.length > 0) options.urls = urls;

const resolvedMode = options.mode || (options.urls ? 'hot' : 'incremental');
const explicitMaxPages = options['max-pages'] ? parseInt(options['max-pages'], 10) : null;

const input = {
    mode: resolvedMode,
    maxPages: resolvedMode === 'full' ? explicitMaxPages : (explicitMaxPages ?? 3),
    maxRequests: options['max-requests'] ? parseInt(options['max-requests'], 10) : 30,
    hours: options.hours ? parseInt(options.hours, 10) : null,
    minutes: options.minutes ? parseInt(options.minutes, 10) : null,
    urls: options.urls ? (Array.isArray(options.urls) ? options.urls : String(options.urls).split(',').map((u: string) => u.trim())) : [],
    board: options.board || null,
};

// ===[ 로그 레벨 ]============================================================
// env LOG_LEVEL=DEBUG|INFO|WARN|ERROR
const level = String(process.env.LOG_LEVEL ?? 'INFO').toUpperCase();
(log as any).setLevel((log.LEVELS as any)[level] ?? log.LEVELS.INFO);
log.info('실행 파라미터:', input);

// 보드 선택: 기본은 BOARDS의 첫 번째 키, --board 사용 시 특정 보드 또는 yall
const boardNames = Object.keys(BOARDS);
if (boardNames.length === 0) {
    log.error('BOARDS가 비어 있습니다. routes.ts의 BOARDS를 확인하세요.');
    process.exit(1);
}
const defaultBoard = boardNames[0];

let selectedBoards: string[] = [];
if (!input.board) selectedBoards = [defaultBoard];
else if (input.board === 'yall') selectedBoards = boardNames;
else if (boardNames.includes(input.board)) selectedBoards = [input.board];
else {
    log.error(`알 수 없는 보드: ${input.board}. 가능: ${boardNames.join(', ')}, 또는 yall`);
    process.exit(1);
}

for (const bname of selectedBoards) {
    const cfg = BOARDS[bname];
    log.info(`보드 시작: ${cfg.SITE_NAME}/${cfg.BOARD_NAME}`);
    const DATASET_ID = `posts_${cfg.SITE_NAME}_${cfg.BOARD_NAME}_${Date.now()}`;
    log.info(`Dataset: ${DATASET_ID}`);

    // Start URLs & userData 구성
    let startUrls: { url: string; label?: string; userData?: Record<string, any> }[] = [];
    const userData: Record<string, any> = { mode: input.mode, maxPages: input.maxPages };

    switch (input.mode) {
        case 'full': {
            // cutoff 계산 (기본 72시간) — LIST에서 시간 힌트를 쓰지 않고 DETAIL 기준으로만 중단
            let totalMinutes = 0;
            if (input.hours) totalMinutes += input.hours * 60;
            if (input.minutes) totalMinutes += input.minutes;
            if (totalMinutes === 0) totalMinutes = 72 * 60; // 기본값 72시간
            (userData as any).cutoffDate = new Date(Date.now() - totalMinutes * 60 * 1000).toISOString();
            startUrls = [{ url: cfg.baseListUrl, label: "LIST", userData }];
            log.info(`Full 모드로 실행합니다. ${totalMinutes / 60}시간 전 게시물까지 수집합니다. (Cutoff: ${(userData as any).cutoffDate})`);
            break;
        }
        case 'hot': {
            const hotUrls = input.urls.filter((u: string) => u.startsWith('http'));
            if (hotUrls.length === 0) {
                log.error("Hot 모드에서는 --urls 파라미터 또는 유효한 URL이 필요합니다.");
                process.exit(1);
            }
            startUrls = hotUrls.map((url: string) => {
                let postId: string | null = null;
                try {
                    const u = new URL(url);
                    const m1 = u.pathname.match(/\/best\/(\d+)(?:\/)?$/);
                    const m2 = u.search.match(/(?:^|[?&])document_srl=(\d+)/);
                    postId = (m1 && m1[1]) || (m2 && m2[1]) || null;
                } catch { }
                return { url, label: 'DETAIL', userData: { postId } };
            });
            log.info(`Hot 모드로 실행합니다. 대상 URL: ${hotUrls.join(', ')}`);
            break;
        }
        case 'incremental':
        default: {
            startUrls = [{ url: cfg.baseListUrl, label: "LIST", userData }];
            log.info('Incremental 모드로 실행합니다.');
            break;
        }
    }

    // ===[ Crawler 생성 (보드별) ]===========================================
    const cfoLaunchOptions = await camoufoxLaunchOptions({
        headless: process.env.HEADFUL === '1' ? false : true,
    });

    const crawlerOptions: any = {
        requestHandler: createRouter(cfg, DATASET_ID),
        postNavigationHooks: [
            async ({ handleCloudflareChallenge, log: localLog }: {
                handleCloudflareChallenge: () => Promise<void>;
                log: Log;
            }) => {
                try {
                    localLog.info('Attempting to handle Cloudflare challenge...');
                    await handleCloudflareChallenge();
                    localLog.info('Cloudflare challenge handler finished.');
                } catch (e: any) {
                    localLog.warning(`handleCloudflareChallenge failed. This might be okay if no challenge was present. Error: ${e.message}`);
                }
            },
        ],
        browserPoolOptions: {
            useFingerprints: false,
        },
        launchContext: {
            launcher: firefox,
            launchOptions: {
                ...cfoLaunchOptions, // 기존 camoufox 옵션 유지
                // 자동 재생 비활성화 및 음소거 설정 추가
                firefoxUserPrefs: {
                    ...(cfoLaunchOptions as any).firefoxUserPrefs,
                    // 'media.autoplay.enabled': true,
                },
            },
        },
        preNavigationHooks: [
            async ({ page }: { page: import('playwright').Page }, gotoOptions: any) => {
                // ✔ DOM만 준비되면 바로 파싱해도 충분
                gotoOptions.waitUntil = 'domcontentloaded';

                if ((page as any)._routeAttached) return;
                try { await page.setViewportSize({ width: 1280, height: 1800 }); } catch { }
                try { await page.setExtraHTTPHeaders({ 'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7' }); } catch { }
                try {
                    await page.addInitScript(() => {
                        try { Object.defineProperty(navigator, 'webdriver', { get: () => false }); } catch { }
                    });
                } catch { }

                await page.route('**/*', (route) => {
                    const req = route.request();
                    const url = req.url();

                    // 광고/트래킹 도메인만 차단
                    if ((BLOCKED_DOMAINS as readonly string[]).some(domain => url.includes(domain))) {
                        return route.abort();
                    }
                    return route.continue();
                });

                (page as any)._routeAttached = true;
            },
        ],
        useSessionPool: false,
        maxRequestsPerCrawl: input.maxRequests,
        navigationTimeoutSecs: 30,
        requestHandlerTimeoutSecs: 120,
    };

    if (input.mode === 'full') {
        log.info('시간 기반 Full 모드이므로 maxRequestsPerCrawl 제한을 해제합니다.');
        crawlerOptions.maxRequestsPerCrawl = undefined;
    }

    const crawler = new PlaywrightCrawler(crawlerOptions, new Configuration({ persistStorage: false }));

    // 보드별 pagesCrawled 초기화
    const stateStoreInit = await KeyValueStore.open(cfg.STATE_STORE_NAME);
    await stateStoreInit.setValue('pagesCrawled', 0);

    log.info(`크롤러를 시작합니다. [${cfg.BOARD_NAME}]`);
    await crawler.run(startUrls);
    log.info(`크롤링이 완료되었습니다. 결과를 처리합니다. [${cfg.BOARD_NAME}]`);

    // ===[ 결과 처리 및 저장 (보드별) ]======================================
    {
        const dataset = await Dataset.open(DATASET_ID);
        const { items } = await dataset.getData();

        const { posts, issues } = validateAndNormalize(items as any[]);
        if (issues.length) issues.forEach((m) => log.warning(`[validate] ${m}`));

        const stateStore = await KeyValueStore.open(cfg.STATE_STORE_NAME);
        const lastStopId = await stateStore.getValue("lastStopId");

        const now = new Date();
        const meta = {
            site: cfg.SITE_NAME,
            board: cfg.BOARD_NAME,
            crawled_at: now.toISOString(),
            crawler_version: "std-optimized-fmk-camoufox-v4",
            total_posts: posts.length,
            pages_crawled: (await stateStore.getValue('pagesCrawled')) || 0,
            crawl_config: {
                mode: input.mode,
                max_pages: input.maxPages,
                max_requests: input.maxRequests,
                incremental: input.mode === 'incremental',
                last_stop_id: lastStopId || null,
                ...(input.mode === 'full' && {
                    hours: input.hours,
                    minutes: input.minutes,
                    cutoff_date: (userData as any).cutoffDate,
                }),
            },
        } as const;

        const finalOutput = [{ meta, posts }];

        const formattedDate = `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now.getDate().toString().padStart(2, '0')}`;
        const formattedTime = `${now.getHours().toString().padStart(2, '0')}-${now.getMinutes().toString().padStart(2, '0')}-${now.getSeconds().toString().padStart(2, '0')}`;
        const fileName = `${cfg.SITE_NAME}_${cfg.BOARD_NAME}_${formattedDate}_${formattedTime}.json`;

        const outputDir = path.resolve(process.cwd(), 'output');
        const backupDir = path.resolve(process.cwd(), '../../../output_data');
        if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
        if (!fs.existsSync(backupDir)) fs.mkdirSync(backupDir, { recursive: true });

        const filePath = path.join(outputDir, fileName);
        fs.writeFileSync(filePath, JSON.stringify(finalOutput, null, 2));
        const backupPath = path.join(backupDir, fileName);
        fs.copyFileSync(filePath, backupPath);
        log.info(`최종 결과를 백업 디렉토리에 복사했습니다: ${backupPath}`);

        log.info(`최종 결과가 ${filePath}에 저장되었습니다. [${cfg.BOARD_NAME}]`);
    }
}

log.info("크롤러가 종료되었습니다.");
