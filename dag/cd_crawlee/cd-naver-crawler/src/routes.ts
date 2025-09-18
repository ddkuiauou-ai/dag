import { createPlaywrightRouter } from 'crawlee';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import fs from 'fs';

export const router = createPlaywrightRouter();

const s3 = new S3Client({
    region: 'auto',
    endpoint: process.env.R2_ENDPOINT,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY_ID!,
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
    },
});

async function uploadToR2(localPath: string, remoteKey: string) {
    const fileContent = await fs.promises.readFile(localPath);
    await s3.send(new PutObjectCommand({
        Bucket: process.env.R2_BUCKET,
        Key: remoteKey,
        Body: fileContent,
        ContentType: 'image/png',
    }));
}

router.addDefaultHandler(async ({ request, page, log }) => {
    // 페이지 로딩 대기 및 로깅
    const companyId = request.userData.companyId;
    log.info(`Crawling company ID: ${companyId}`);
    try {
        await page.waitForLoadState('networkidle', { timeout: 15000 });
        await page.waitForSelector('#main_pack', { timeout: 15000 });
    } catch (err) {
        log.error(`Page load or selector wait failed for companyId ${companyId}: ${err}`);
        return;
    }

    // 회사 정보 이미지 셀렉터
    const selector = '#main_pack > section.sc_new.cs_common_simple._company_info_simple > div.cm_content_wrap > div > div > div.detail_info > a > img';
    const imgLocator = page.locator(selector);
    let imgCount = 0;
    try {
        imgCount = await imgLocator.count();
    } catch (err) {
        log.error(`Error counting images for companyId ${companyId}: ${err}`);
        return;
    }

    if (imgCount > 0) {
        log.info(`Found ${imgCount} company info image(s). Saving first one...`);
        const filename = `${companyId}`;
        const filePath = `./storage/screenshots/clogo-${filename}.png`;
        try {
            await imgLocator.first().screenshot({ path: filePath });
            log.info(`Company logo saved: ${filePath}`);
            // R2 업로드
            const remoteKey = `clogo-${filename}.png`;
            try {
                await uploadToR2(filePath, remoteKey);
                log.info(`Uploaded to R2: ${remoteKey}`);
            } catch (err) {
                log.error(`Failed to upload to R2: ${err}`);
            }
        } catch (err) {
            log.error(`Failed to save screenshot for companyId ${companyId}: ${err}`);
        }
    } else {
        log.info('No company info image found.');
    }
});
