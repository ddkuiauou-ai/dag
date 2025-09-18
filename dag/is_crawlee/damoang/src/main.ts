import { PlaywrightCrawler, log, Dataset, KeyValueStore, Configuration } from "crawlee";
import { createRouter, BOARDS } from "./routes.js";
import fs from 'node:fs';
import path from 'node:path';

/**
 * ---------------------------------------------------------------------------
 * 커뮤니티 게시판 크롤러 엔트리 (std-optimized)
 *
 * 📌 역할
 *   - 실행 진입점(entry). 실제 **셀렉터/규칙** 은 routes.ts 에서 정의하며
 *     main.ts 는 크롤링 모드, CLI 파싱, 결과 저장/검증을 담당.
 *
 * 🚦 실행 모드
 *   • incremental (기본) : 최근 페이지만 조회, 최신 postId 저장
 *   • full              : --hours/--minutes 로 지정한 시간 이전 글에서 중단
 *   • hot               : 전달된 URL(여러 개 가능)만 DETAIL 크롤
 * ---------------------------------------------------------------------------
 */

// ===[ 공통 상수 ]===========================================================
const BLOCKED_RESOURCE_TYPES = ["stylesheet", "font", "image", "media"] as const;
const BLOCKED_DOMAINS = [
    'google-analytics.com', 'googletagmanager.com', 'doubleclick.net', 'criteo.com',
    'adnxs.com', 'adform.net', 'bidswitch.net', 'adsrvr.org', 'facebook.net',
    'bing.com', 'youtube-nocookie.com'
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
        // 1) image_count 먼저, 그 다음 images
        post.image_count = Array.isArray(post.images) ? post.images.length : 0;
        post.images = Array.isArray(post.images) ? post.images : [];

        // 2) embed_count 먼저, 그 다음 embeddedContent
        post.embed_count = Array.isArray(post.embeddedContent) ? post.embeddedContent.length : 0;
        post.embeddedContent = Array.isArray(post.embeddedContent) ? post.embeddedContent : [];

        // 3) comment_count
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
    // Full 모드 기본은 무제한(null). 사용자가 --max-pages 명시 시 그 값을 사용.
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
            // cutoff 계산 (기본 48시간)
            let totalMinutes = 0;
            if (input.hours) totalMinutes += input.hours * 60;
            if (input.minutes) totalMinutes += input.minutes;
            if (totalMinutes === 0) totalMinutes = 48 * 60; // 기본값 48시간
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
                const postIdMatch = url.match(/(\d+)(?=\/?$)/);
                return { url, label: 'DETAIL', userData: { postId: postIdMatch ? postIdMatch[1] : null } };
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
    const crawlerOptions: any = {
        requestHandler: createRouter(cfg, DATASET_ID),
        launchContext: {
            launchOptions: {
                headless: process.env.HEADFUL === '1' ? false : true,
                channel: process.env.PW_CHANNEL || 'chrome',
                args: [
                    '--autoplay-policy=no-user-gesture-required',
                    '--disable-features=PreloadMediaEngagementData,AutoplayIgnoreWebAudio,SameSiteByDefaultCookies,CookiesWithoutSameSiteMustBeSecure',
                    '--enable-features=NetworkService,UseSurfaceLayerForVideo',
                    '--lang=ko-KR',
                ],
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
                    const resourceType = req.resourceType();
                    const url = req.url();

                    // 1) 무거운 리소스는 도메인과 무관하게 차단
                    if ((BLOCKED_RESOURCE_TYPES as readonly string[]).includes(resourceType as any)) {
                        return route.abort();
                    }
                    // 2) 광고/트래킹 도메인 차단
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
            crawler_version: "std-optimized",
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
