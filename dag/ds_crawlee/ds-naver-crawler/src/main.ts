// For more information, see https://crawlee.dev/
import { PlaywrightCrawler, ProxyConfiguration } from 'crawlee';
import 'dotenv/config';
import { router } from './routes.js';
import { Client } from 'pg';

/** Base URL for Naver search queries */
const BASE_SEARCH_URL = 'https://search.naver.com/search.naver?where=nexearch&query=';

/**
 * Formats milliseconds into a HH:MM:SS string.
 * @param ms Milliseconds
 * @returns Formatted time string
 */
function formatTime(ms: number): string {
    if (ms < 0) ms = 0;
    const totalSeconds = Math.floor(ms / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}

// 환경 변수 체크
if (!process.env.DS_DATABASE_URL &&
    (!process.env.DS_POSTGRES_USER || !process.env.DS_POSTGRES_PASSWORD || !process.env.DS_POSTGRES_HOST || !process.env.DS_POSTGRES_PORT || !process.env.DS_POSTGRES_DB)
) {
    console.error('Database connection environment variables are missing.');
    process.exit(1);
}

const connectionString =
    process.env.DS_DATABASE_URL ??
    `postgres://${process.env.DS_POSTGRES_USER}:${encodeURIComponent(
        process.env.DS_POSTGRES_PASSWORD!
    )}@${process.env.DS_POSTGRES_HOST}:${process.env.DS_POSTGRES_PORT}/${process.env.DS_POSTGRES_DB}`;

console.log('Connecting to database with connection string:', connectionString);

const client = new Client({ connectionString });
try {
    await client.connect();
} catch (err) {
    console.error('Failed to connect to database:', err);
    process.exit(1);
}

let nameAndId: { name: string; id: string; searchUrl: string }[];
try {
    // const res = await client.query("SELECT name, id FROM company where id = '32118'");
    const res = await client.query("SELECT name, id FROM company where is_deleted = false");
    nameAndId = res.rows.map((row: any) => ({
        name: row.name,
        id: row.id,
        searchUrl: BASE_SEARCH_URL + encodeURIComponent(row.name),
    }));
} catch (err) {
    console.error('Database query failed:', err);
    await client.end();
    process.exit(1);
}

if (!nameAndId || nameAndId.length === 0) {
    console.warn('No companies found to crawl.');
    await client.end();
    process.exit(0);
}

// --- Progress Logging Setup ---
let processedCompanies = 0;
const totalCompanies = nameAndId.length;
const startTime = Date.now();

console.log(`Starting crawl for ${totalCompanies} companies.`);

const logProgress = (requestUrl: string, status: 'SUCCESS' | 'FAILURE', imageFound: boolean) => {
    processedCompanies++;
    const elapsedTime = Date.now() - startTime;
    const avgTimePerCompany = processedCompanies > 0 ? elapsedTime / processedCompanies : 0;
    const estimatedRemainingTime = avgTimePerCompany * (totalCompanies - processedCompanies);
    const companyName = decodeURIComponent(requestUrl.split('query=')[1] || requestUrl);

    // Only log if an image was found
    if (imageFound) {
        if (status === 'SUCCESS') {
            console.log(`[${new Date().toISOString()}] [SUCCESS] Processed ${processedCompanies}/${totalCompanies} (${((processedCompanies / totalCompanies) * 100).toFixed(2)}%). Company: ${companyName}. Image Found. ETA: ${formatTime(estimatedRemainingTime)}`);
        } else {
            // Still log failures if an image was expected (or found then failed to process) for debugging
            console.warn(`[${new Date().toISOString()}] [FAILURE] Failed request for ${companyName}. Processed ${processedCompanies}/${totalCompanies}. Image Found (process failed). ETA: ${formatTime(estimatedRemainingTime)}`);
        }
    } else if (status === 'FAILURE') {
        // Optionally, log failures even if no image was found, if that's useful for general error tracking
        console.warn(`[${new Date().toISOString()}] [FAILURE] Failed request for ${companyName}. Processed ${processedCompanies}/${totalCompanies}. No Image. ETA: ${formatTime(estimatedRemainingTime)}`);
    }
    // If status is SUCCESS and no image was found, no log will be printed based on the new requirement.
};

const originalRequestHandler = router;

const crawler = new PlaywrightCrawler({
    // proxyConfiguration: new ProxyConfiguration({ proxyUrls: ['...'] }),
    async requestHandler(context) {
        // Initialize imageFound in userData for this request
        context.request.userData.imageFound = false;
        await originalRequestHandler(context);
        // Pass the imageFound status from userData to logProgress
        logProgress(context.request.url, 'SUCCESS', context.request.userData.imageFound);
    },
    async failedRequestHandler(context, error) {
        // You might want to call the original router's failedRequestHandler if it exists
        // or handle the error specifically here.
        console.error(`Error processing ${context.request.url}: ${error.message}`);
        // Pass the imageFound status (likely false if request failed early) from userData
        logProgress(context.request.url, 'FAILURE', context.request.userData.imageFound || false);
    },
    // maxConcurrency: 10,
    // maxRequestsPerCrawl: 40,
});

// Uncomment this block to test with specific search URLs
// await crawler.run([
//     BASE_SEARCH_URL + encodeURIComponent("삼성E&A"),
//     BASE_SEARCH_URL + "한국팩키지",
//     BASE_SEARCH_URL + "세종텔레콤",
//     BASE_SEARCH_URL + "삼성전자",
//     BASE_SEARCH_URL + "에이플러스에셋"
// ]);

try {
    await crawler.run(nameAndId.map(item => ({
        url: BASE_SEARCH_URL + encodeURIComponent(item.name),
        userData: {
            id: item.id,
        },
    })));
    console.log('Crawling completed.');
} catch (err) {
    console.error('Crawling failed:', err);
    process.exit(1);
} finally {
    await client.end();
}
console.log('Database connection closed.');