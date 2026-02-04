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

// --- কনকারেন্সি কন্ট্রোল ভেরিয়েবল ---
const MAX_CONCURRENT_UPLOADS = 5; 
let runningUploads = 0;           
const uploadQueue = [];           
const activeUploads = new Set();  

app.get('/favicon.ico', (req, res) => res.status(204).end());

// --- কিউ প্রসেস করার ফাংশন ---
const processQueue = async () => {
    // স্লট খালি না থাকলে বা কিউ খালি থাকলে ফিরে যাবে
    if (runningUploads >= MAX_CONCURRENT_UPLOADS || uploadQueue.length === 0) {
        return;
    }

    const task = uploadQueue.shift(); 
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
            queueSize: 3, 
            partSize: 1024 * 1024 * 10 
        });

        await upload.done();
        console.log(`[Success] Finished: ${fileName}`);
    } catch (err) {
        console.error(`[Error] Upload failed for ${fileName}:`, err.message);
        // যদি ফেইল করে তবে সেট থেকে রিমুভ করে দিচ্ছি যাতে পরে আবার ট্রাই করা যায়
        activeUploads.delete(fileId);
    } finally {
        runningUploads--;
        // আপলোড শেষ হওয়ার পর সেট থেকে ফাইলটি রিমুভ করার প্রয়োজন নেই যদি আমরা চাই 
        // যে এটি দ্বিতীয়বার প্রসেস না হোক। তবে মেমোরি পরিষ্কার রাখতে বা সাকসেস হলে রিমুভ করা ভালো।
        
        setTimeout(() => {
            processQueue();
        }, 300);
    }
};

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID" });

    try {
        let fileName = "";
        let fileSize = 0;

        // ১. নাম উদ্ধার (API এবং Scraping Method) 
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
                    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' } 
                });
                const match = response.data.match(/<title>(.*?) - Google Drive<\/title>/);
                if (match && match[1]) {
                    fileName = match[1].trim();
                }
            } catch (scrapErr) {
                console.error(`Scraping failed for ${fileId}`);
            }
        }

        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;

        // ২. বিকল্প Header চেক (যদি উপরের মেথডে নাম না পাওয়া যায়)
        if (!fileName) {
            try {
                const headRes = await axios.head(gDriveUrl);
                const cd = headRes.headers['content-disposition'];
                if (cd && cd.includes('filename=')) {
                    fileName = cd.split('filename=')[1].replace(/['"]/g, '');
                }
            } catch (hErr) {}
        }

        // একদমই নাম না পাওয়া গেলে আইডি ব্যবহার
        if (!fileName) fileName = `Movie_${fileId}.mkv`;

        const r2Key = fileName;
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(r2Key)}`;

        // ৩. R2 চেক 
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
        } catch (e) { /* ফাইল নেই, আপলোড দরকার */ }

        // ৪. কিউতে যুক্ত করা (ডুপ্লিকেট চেক)
        if (!activeUploads.has(fileId)) {
            activeUploads.add(fileId);
            uploadQueue.push({ fileId, fileName, r2Key, gDriveUrl });
            console.log(`[Queue] Added: ${fileName}`);
            // কিউ প্রসেসিং শুরু করা (যদি অলরেডি না চলে)
            processQueue(); 
        }

        // কিউতে পজিশন বের করা
        const queuePos = uploadQueue.findIndex(f => f.fileId === fileId) + 1;
        
        res.json({
            status: "processing",
            filename: fileName,
            message: queuePos > 0 ? `In queue (Position: ${queuePos})` : "Uploading...",
            active_slots: `${runningUploads}/${MAX_CONCURRENT_UPLOADS}`,
            queue_length: uploadQueue.length
        });

    } catch (error) {
        res.json({ status: "error", message: "Server encountered an error." });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
