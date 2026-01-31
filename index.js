const express = require('express');
const axios = require('axios');
const { S3Client, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const stream = require('stream');

const app = express();
const PORT = process.env.PORT || 3000;

// Cloudflare R2 Client Configuration
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
        // ১. Google Drive Metadata সংগ্রহ
        const apiKey = process.env.GDRIVE_API_KEY;
        const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size,mimeType&key=${apiKey}`;
        
        let fileName, fileSize, mimeType;

        try {
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name;
            fileSize = parseInt(metaResponse.data.size) || 0;
            mimeType = metaResponse.data.mimeType;
        } catch (e) {
            console.error("Meta Fetch Error:", e.message);
            return res.status(403).json({ status: "error", message: "Google Drive API access denied. Check API Key and File Permissions." });
        }

        const r2Key = fileName;
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(r2Key)}`;

        // ২. R2 চেক
        try {
            await s3Client.send(new HeadObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key
            }));
            
            return res.json({
                status: "success",
                filename: fileName,
                size: fileSize,
                url: r2PublicUrl
            });
        } catch (e) { /* ফাইল নেই, আপলোড করতে হবে */ }

        // ৩. ব্যাকগ্রাউন্ড আপলোড (Bypass Virus Warning লজিকসহ)
        if (!activeUploads.has(fileId)) {
            const startUpload = async () => {
                activeUploads.add(fileId);
                console.log(`Starting transfer for: ${fileName}`);
                try {
                    // বড় ফাইলের ভাইরাস স্ক্যান ওয়ার্নিং বাইপাস করতে confirm=t যোগ করা হয়েছে
                    const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${apiKey}&confirm=t`;
                    
                    const response = await axios({
                        method: 'get',
                        url: gDriveUrl,
                        responseType: 'stream',
                        timeout: 0,
                        maxContentLength: Infinity,
                        maxBodyLength: Infinity,
                        headers: {
                            'User-Agent': 'Mozilla/5.0' // ব্রাউজার হিসেবে রিকোয়েস্ট পাঠানো
                        }
                    });

                    const upload = new Upload({
                        client: s3Client,
                        params: {
                            Bucket: process.env.R2_BUCKET_NAME,
                            Key: r2Key,
                            Body: response.data.pipe(new stream.PassThrough()),
                            ContentType: mimeType || 'video/x-matroska',
                            ContentDisposition: `attachment; filename="${encodeURIComponent(fileName)}"`
                        },
                        queueSize: 4,
                        partSize: 10 * 1024 * 1024 // 10MB parts
                    });

                    await upload.done();
                    console.log(`Successfully finished: ${fileName}`);
                } catch (err) {
                    console.error(`Upload error for ${fileId}:`, err.message);
                } finally {
                    activeUploads.delete(fileId);
                }
            };
            startUpload();
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "Transfer started from GDrive to R2. Please wait and refresh.",
            progress: "Running"
        });

    } catch (error) {
        res.json({ status: "error", message: error.message });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
