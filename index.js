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
        // ১. Google Drive Metadata সংগ্রহ (আসল নাম উদ্ধার)
        const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
        let fileName = `Movie_${fileId}.mkv`; 
        let fileSize = 0;

        try {
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name; 
            fileSize = parseInt(metaResponse.data.size) || 0;
        } catch (e) {
            console.error("Meta Fetch Error:", e.message);
        }

        // ২. বাকেট Key নির্ধারণ (এখন আসল নামেই R2 তে সেভ হবে)
        const r2Key = fileName; 
        const encodedKey = encodeURIComponent(r2Key); // URL-এর জন্য নাম এনকোড করা
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
                url: r2PublicUrl,
                r2_key: r2Key 
            });
        } catch (e) { /* ফাইল নেই */ }

        // ৪. ব্যাকগ্রাউন্ড আপলোড (আসল নাম এবং হেডারসহ)
        if (!activeUploads.has(fileId)) {
            const startUpload = async () => {
                activeUploads.add(fileId);
                console.log(`Starting upload for: ${fileName}`);
                try {
                    const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
                    const response = await axios({ method: 'get', url: gDriveUrl, responseType: 'stream', timeout: 0 });

                    const upload = new Upload({
                        client: s3Client,
                        params: {
                            Bucket: process.env.R2_BUCKET_NAME,
                            Key: r2Key, // R2-তে এই নামেই ফাইলটি জমা হবে
                            Body: response.data.pipe(new stream.PassThrough()),
                            ContentType: response.headers['content-type'] || 'video/x-matroska',
                            // এটি ডাউনলোডের সময় আসল নাম নিশ্চিত করবে
                            ContentDisposition: `attachment; filename="${fileName}"`
                        },
                        queueSize: 1, 
                        partSize: 1024 * 1024 * 10
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
            message: "Upload started with original filename. Please wait.",
            progress: "Running"
        });

    } catch (error) {
        res.json({ status: "processing", message: "Initialing..." });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
