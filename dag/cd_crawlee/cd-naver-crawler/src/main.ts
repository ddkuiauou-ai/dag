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
if (!process.env.CD_DATABASE_URL &&
    (!process.env.CD_POSTGRES_USER || !process.env.CD_POSTGRES_PASSWORD || !process.env.CD_POSTGRES_HOST || !process.env.CD_POSTGRES_PORT || !process.env.CD_POSTGRES_DB)
) {
    console.error('Database connection environment variables are missing.');
    process.exit(1);
}

const connectionString =
    process.env.CD_DATABASE_URL ??
    `postgres://${process.env.CD_POSTGRES_USER}:${encodeURIComponent(
        process.env.CD_POSTGRES_PASSWORD!
    )}@${process.env.CD_POSTGRES_HOST}:${process.env.CD_POSTGRES_PORT}/${process.env.CD_POSTGRES_DB}`;

console.log('Connecting to database with connection string:', connectionString);

const client = new Client({ connectionString });
try {
    await client.connect();
} catch (err) {
    console.error('Failed to connect to database:', err);
    process.exit(1);
}

let nameAndCompanyId: { name: string; companyId: string; searchUrl: string }[];
try {
    const res = await client.query("SELECT name, company_id FROM company limit 20");
    // const res = await client.query("SELECT name, company_id FROM company where name = '삼성전자'");
    nameAndCompanyId = res.rows.map((row: any) => ({
        name: row.name,
        companyId: row.company_id,
        searchUrl: BASE_SEARCH_URL + encodeURIComponent(row.name),
    }));
} catch (err) {
    console.error('Database query failed:', err);
    await client.end();
    process.exit(1);
}

if (!nameAndCompanyId || nameAndCompanyId.length === 0) {
    console.warn('No companies found to crawl.');
    await client.end();
    process.exit(0);
}

// --- Progress Logging Setup ---
let processedCompanies = 0;
const totalCompanies = nameAndCompanyId.length;
const startTime = Date.now();

console.log(`Starting crawl for ${totalCompanies} companies.`);

const logProgress = (requestUrl: string, status: 'SUCCESS' | 'FAILURE') => {
    processedCompanies++;
    const elapsedTime = Date.now() - startTime;
    const avgTimePerCompany = processedCompanies > 0 ? elapsedTime / processedCompanies : 0;
    const estimatedRemainingTime = avgTimePerCompany * (totalCompanies - processedCompanies);
    // Extract company name from URL, assuming it's the query parameter
    const companyNameMatch = requestUrl.match(/query=([^&]+)/);
    const companyName = companyNameMatch && companyNameMatch[1] ? decodeURIComponent(companyNameMatch[1]) : 'Unknown Company';

    console.log(
        `[${new Date().toISOString()}] [${status}] ${processedCompanies}/${totalCompanies} (${((processedCompanies / totalCompanies) * 100).toFixed(2)}%) - ${companyName}. ETA: ${formatTime(estimatedRemainingTime)}`
    );
};

const originalRequestHandler = router; // Save the original router

const crawler = new PlaywrightCrawler({
    // proxyConfiguration: new ProxyConfiguration({ proxyUrls: ['...'] }),
    // requestHandler: router, // Replace with custom handlers
    async requestHandler(context) {
        if (typeof originalRequestHandler === 'function') {
            await originalRequestHandler(context);
        } else {
            console.warn('Original request handler is not a function. Check routes.js export.');
        }
        logProgress(context.request.url, 'SUCCESS');
    },
    async failedRequestHandler(context, error) {
        console.error(`Failed request for ${context.request.url}: ${error.message}`);
        logProgress(context.request.url, 'FAILURE');
    },
    // maxConcurrency: 10,
    // maxRequestsPerCrawl: 20, // Limit for testing
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
    await crawler.run(nameAndCompanyId.map(item => ({
        url: BASE_SEARCH_URL + encodeURIComponent(item.name),
        userData: {
            companyId: item.companyId,
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