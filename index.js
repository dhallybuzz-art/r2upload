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
        // ১. Google Drive Metadata সংগ্রহ (আসল নাম এবং সাইজ উদ্ধার)
        const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
        let fileName = `Movie_${fileId}.mkv`; // Default fallback name
        let fileSize = 0;

        try {
            const metaResponse = await axios.get(metaUrl);
            if (metaResponse.data.name) {
                fileName = metaResponse.data.name; 
                fileSize = parseInt(metaResponse.data.size) || 0;
                console.log(`Fetched original name: ${fileName}`);
            }
        } catch (e) {
            // যদি 404 আসে, তার মানে ফাইলটি পাবলিক নয় অথবা API কী-তে সমস্যা আছে
            console.error(`Meta Fetch Error for ${fileId}:`, e.message);
        }

        // ২. বাকেট Key এবং পাবলিক URL নির্ধারণ
        const r2Key = fileName; 
        const encodedKey = encodeURIComponent(r2Key);
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodedKey}`;

        // ৩. R2-তে ফাইলটি ইতিমধ্যে আছে কি না চেক করা
        try {
            const headData = await s3Client.send(new HeadObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key
            }));
            
            return res.json({
                status: "success",
                filename: fileName,
                size: headData.ContentLength || fileSize,
                url: r2PublicUrl,
                r2_key: r2Key 
            });
        } catch (e) { 
            // ফাইল R2 তে নেই, আপলোড শুরু করতে হবে
        }

        // ৪. ব্যাকগ্রাউন্ড আপলোড (যদি অলরেডি প্রসেসিং-এ না থাকে)
        if (!activeUploads.has(fileId)) {
            const startUpload = async () => {
                activeUploads.add(fileId);
                console.log(`Starting upload to R2: ${fileName}`);
                try {
                    const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
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
                        partSize: 1024 * 1024 * 20 // 20MB parts
                    });

                    await upload.done();
                    console.log(`Successfully uploaded to R2: ${fileName}`);
                } catch (err) {
                    console.error(`Upload failed for ${fileName}:`, err.message);
                } finally {
                    activeUploads.delete(fileId);
                }
            };
            startUpload();
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "File found and upload started. Please check back in a moment.",
            progress: "Running"
        });

    } catch (error) {
        console.error("General Error:", error.message);
        res.status(500).json({ status: "error", message: "Internal server error" });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
