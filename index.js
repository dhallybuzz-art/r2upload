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

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid File ID" });

    try {
        // ১. Metadata সংগ্রহ (আসল নাম উদ্ধার)
        const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
        let fileName = `Movie_${fileId}.mkv`;
        let fileSize = 0;

        try {
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name; 
            fileSize = parseInt(metaResponse.data.size) || 0;
        } catch (e) {
            return res.json({ status: "processing", message: "Check File ID or Privacy Settings." });
        }

        const r2Key = fileName; // ফাইলটি আসল নামেই বাকেটে থাকবে
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(r2Key)}`;

        // ২. বাকেটে আছে কি না চেক
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

        // ৩. আপলোড প্রসেস
        if (!activeUploads.has(fileId)) {
            const startUpload = async () => {
                activeUploads.add(fileId);
                try {
                    const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
                    const response = await axios({ method: 'get', url: gDriveUrl, responseType: 'stream' });

                    const upload = new Upload({
                        client: s3Client,
                        params: {
                            Bucket: process.env.R2_BUCKET_NAME,
                            Key: r2Key,
                            Body: response.data.pipe(new stream.PassThrough()),
                            ContentType: 'video/x-matroska',
                            ContentDisposition: `attachment; filename="${fileName}"` // IDM ফিক্স
                        },
                        queueSize: 1, // মেমোরি সেফ
                        partSize: 10 * 1024 * 1024 // ১০ মেগাবাইট চাঙ্ক
                    });

                    await upload.done();
                    console.log(`Successfully Uploaded: ${fileName}`);
                } catch (err) {
                    console.error("403 Forbidden: File is not Public!");
                } finally {
                    activeUploads.delete(fileId);
                }
            };
            startUpload();
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "Initial upload started. Please refresh after a while."
        });

    } catch (error) {
        res.json({ status: "processing" });
    }
});

app.listen(PORT, () => console.log(`Uploader running on port ${PORT}`));
