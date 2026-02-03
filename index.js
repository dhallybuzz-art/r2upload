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

const activeUploads = new Set();

app.get('/favicon.ico', (req, res) => res.status(204).end());

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID" });

    try {
        let fileName = "";
        let fileSize = 0;

        // ১. প্রথম চেষ্টা: Google Drive API
        try {
            const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name;
            fileSize = parseInt(metaResponse.data.size) || 0;
        } catch (e) {
            console.log(`Meta API failed for ${fileId}, trying Scraping...`);
            
            // ২. দ্বিতীয় চেষ্টা: Scraping (আপনার লগে এটি সফল হয়েছে)
            try {
                const previewUrl = `https://drive.google.com/file/d/${fileId}/view`;
                const response = await axios.get(previewUrl, { 
                    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' } 
                });
                const match = response.data.match(/<title>(.*?) - Google Drive<\/title>/);
                if (match && match[1]) {
                    fileName = match[1].trim();
                    console.log(`Success via Scraping: ${fileName}`);
                }
            } catch (scrapErr) {
                console.error(`Scraping also failed.`);
            }
        }

        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;

        // ৩. তৃতীয় চেষ্টা: যদি নাম এখনো খালি থাকে, তবে সরাসরি স্ট্রীম হেডার চেক করা
        if (!fileName) {
            try {
                const headRes = await axios.head(gDriveUrl);
                const cd = headRes.headers['content-disposition'];
                if (cd && cd.includes('filename=')) {
                    fileName = cd.split('filename=')[1].replace(/['"]/g, '');
                }
            } catch (hErr) { /* fallback to id */ }
        }

        // সব ব্যর্থ হলে আইডি ব্যবহার
        if (!fileName) fileName = `Movie_${fileId}.mkv`;

        const r2Key = fileName;
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(r2Key)}`;

        // R2 তে ফাইল আছে কি না চেক
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
        } catch (e) { /* ফাইল নেই, আপলোড শুরু হবে */ }

        // আপলোড প্রসেস
        if (!activeUploads.has(fileId)) {
            const startUpload = async () => {
                activeUploads.add(fileId);
                try {
                    const response = await axios({ method: 'get', url: gDriveUrl, responseType: 'stream', timeout: 0 });
                    
                    const upload = new Upload({
                        client: s3Client,
                        params: {
                            Bucket: process.env.R2_BUCKET_NAME,
                            Key: r2Key,
                            Body: response.data.pipe(new stream.PassThrough()),
                            ContentType: response.headers['content-type'] || 'video/x-matroska',
                            ContentDisposition: `attachment; filename="${fileName}"`
                        },
                        queueSize: 10, // স্পিড বাড়ানোর জন্য বাড়ানো হয়েছে
                        partSize: 1024 * 1024 * 10 // 10MB parts
                    });

                    await upload.done();
                    console.log(`Upload Complete: ${fileName}`);
                } catch (err) {
                    console.error(`Upload error:`, err.message);
                } finally {
                    activeUploads.delete(fileId);
                }
            };
            startUpload();
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "Naming secured. Background upload is running.",
            progress: "Running"
        });

    } catch (error) {
        res.json({ status: "processing", message: "Starting stream..." });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
