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

        // ১. প্রথম চেষ্টা: Google Drive Meta API
        try {
            const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name;
            fileSize = parseInt(metaResponse.data.size) || 0;
        } catch (e) {
            console.error(`Meta API failed for ${fileId}, trying Page Scraping method...`);
            
            // ২. দ্বিতীয় চেষ্টা (বিকল্প): Google Drive Preview Page থেকে নাম বের করা
            try {
                const previewUrl = `https://drive.google.com/file/d/${fileId}/view`;
                const response = await axios.get(previewUrl, { 
                    headers: { 'User-Agent': 'Mozilla/5.0' } 
                });
                // HTML থেকে টাইটেল ট্যাগ বা নির্দিষ্ট প্যাটার্ন দিয়ে নাম খোঁজা
                const match = response.data.match(/<title>(.*?) - Google Drive<\/title>/);
                if (match && match[1]) {
                    fileName = match[1].trim();
                    console.log(`Found name via Scraping: ${fileName}`);
                }
            } catch (scrapErr) {
                console.error(`Scraping method also failed.`);
            }
        }

        // যদি কোনোভাবেই নাম না পাওয়া যায়, তবেই শেষ ভরসা আইডি
        if (!fileName) fileName = `Movie_${fileId}.mkv`;

        const r2Key = fileName; 
        const encodedKey = encodeURIComponent(r2Key);
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodedKey}`;

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
        } catch (e) { /* ফাইল নেই */ }

        // ৪. ব্যাকগ্রাউন্ড আপলোড
        if (!activeUploads.has(fileId)) {
            const startUpload = async () => {
                activeUploads.add(fileId);
                try {
                    const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
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
                        queueSize: 5, 
                        partSize: 1024 * 1024 * 20
                    });

                    await upload.done();
                    console.log(`Successfully finished: ${fileName}`);
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
            message: "Naming fixed and upload started.",
            progress: "Running"
        });

    } catch (error) {
        res.json({ status: "processing", message: "Working..." });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
