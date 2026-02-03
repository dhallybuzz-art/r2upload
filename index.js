const express = require('express');
const axios = require('axios');
const { S3Client, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const stream = require('stream');

const app = express();
const PORT = process.env.PORT || 3000;

const s3Client = new S3Client({
    region: "auto",
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY,
        secretAccessKey: process.env.R2_SECRET_KEY,
    },
});

// --- কনকারেন্সি কন্ট্রোল ভেরিয়েবল ---
const MAX_CONCURRENT_UPLOADS = 5; // সর্বোচ্চ ৫টি ফাইল
let runningUploads = 0;           // বর্তমানে কতটি চলছে
const uploadQueue = [];           // অপেক্ষমান ফাইলের তালিকা
const activeUploads = new Set();  // কিউ বা রানিং অবস্থায় থাকা ফাইল আইডি

app.get('/favicon.ico', (req, res) => res.status(204).end());

// --- কিউ প্রসেস করার ফাংশন ---
const processQueue = async () => {
    // যদি স্লট খালি থাকে এবং কিউতে ফাইল থাকে
    if (runningUploads >= MAX_CONCURRENT_UPLOADS || uploadQueue.length === 0) {
        return;
    }

    const task = uploadQueue.shift(); // কিউ থেকে প্রথম ফাইলটি নেওয়া
    runningUploads++;
    
    const { fileId, fileName, r2Key, gDriveUrl } = task;
    console.log(`[Queue] Starting upload (${runningUploads}/${MAX_CONCURRENT_UPLOADS}): ${fileName}`);

    try {
        const response = await axios({ 
            method: 'get', 
            url: gDriveUrl, 
            responseType: 'stream', 
            timeout: 0 
        });

        const upload = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key,
                Body: response.data.pipe(new stream.PassThrough()),
                ContentType: response.headers['content-type'] || 'video/x-matroska',
                ContentDisposition: `attachment; filename="${fileName}"`
            },
            queueSize: 5, 
            partSize: 1024 * 1024 * 20 // 10MB parts
        });

        await upload.done();
        console.log(`[Success] Finished: ${fileName}`);
    } catch (err) {
        console.error(`[Error] Upload failed for ${fileName}:`, err.message);
    } finally {
        runningUploads--;
        activeUploads.delete(fileId);
        processQueue(); // একটি শেষ হলে পরেরটি শুরু করার জন্য কল করা
    }
};

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID" });

    try {
        let fileName = "";
        let fileSize = 0;

        // ১. নাম উদ্ধার (আপনার পছন্দের Scraping Method সহ)
        try {
            const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name;
            fileSize = parseInt(metaResponse.data.size) || 0;
        } catch (e) {
            console.log(`Meta API failed for ${fileId}, trying Scraping...`);
            try {
                const previewUrl = `https://drive.google.com/file/d/${fileId}/view`;
                const response = await axios.get(previewUrl, { 
                    headers: { 'User-Agent': 'Mozilla/5.0' } 
                });
                const match = response.data.match(/<title>(.*?) - Google Drive<\/title>/);
                if (match && match[1]) {
                    fileName = match[1].trim();
                    console.log(`Found via Scraping: ${fileName}`);
                }
            } catch (scrapErr) {
                console.error(`Scraping method also failed for ${fileId}`);
            }
        }

        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;

        // ৩য় অপশন: নাম না পাওয়া গেলে Header চেক
        if (!fileName) {
            try {
                const headRes = await axios.head(gDriveUrl);
                const cd = headRes.headers['content-disposition'];
                if (cd && cd.includes('filename=')) {
                    fileName = cd.split('filename=')[1].replace(/['"]/g, '');
                }
            } catch (hErr) {}
        }

        if (!fileName) fileName = `Movie_${fileId}.mkv`;

        const r2Key = fileName;
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(r2Key)}`;

        // ২. R2 চেক
        try {
            const headData = await s3Client.send(new HeadObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key
            }));
            
            return res.json({
                status: "success",
                filename: fileName,
                size: headData.ContentLength || fileSize,
                url: r2PublicUrl
            });
        } catch (e) { /* ফাইল নেই */ }

        // ৩. কিউতে যুক্ত করা (যদি অলরেডি না থাকে)
        if (!activeUploads.has(fileId)) {
            activeUploads.add(fileId);
            uploadQueue.push({ fileId, fileName, r2Key, gDriveUrl });
            console.log(`[Queue] Added: ${fileName}. Queue position: ${uploadQueue.length}`);
            processQueue(); // কিউ চেক করা
        }

        // বর্তমান অবস্থা জানানো
        const queuePos = uploadQueue.findIndex(f => f.fileId === fileId) + 1;
        
        res.json({
            status: "processing",
            filename: fileName,
            message: queuePos > 0 ? `In queue (Position: ${queuePos})` : "Uploading now...",
            active_uploads: `${runningUploads}/${MAX_CONCURRENT_UPLOADS}`,
            queue_length: uploadQueue.length
        });

    } catch (error) {
        res.json({ status: "processing", message: "Initializing stream..." });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));


