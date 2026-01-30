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

// ১. Favicon এবং রুট রিকোয়েস্ট হ্যান্ডলিং
app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("API Service is Running..."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    
    if (!fileId || fileId.length < 15) {
        return res.status(400).json({ status: "error", message: "Invalid File ID" });
    }

    const r2Key = `${fileId}.mp4`;
    const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${r2Key}`;

    try {
        // ২. Google Drive থেকে ফাইলের নাম এবং সাইজ সংগ্রহ (Metadata)
        const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
        let fileName = `Movie_${fileId}`; // ডিফল্ট নাম
        let fileSize = 0;

        try {
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name; // আসল নাম উদ্ধার
            fileSize = parseInt(metaResponse.data.size) || 0;
        } catch (e) {
            console.error("Meta Fetch Error:", e.message);
        }

        // ৩. R2 বাকেটে ফাইল আছে কি না চেক
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
            // ফাইল নেই, আপলোড প্রসেসে যাবে
        }

        // ৪. অলরেডি আপলোড চলছে কি না চেক
        if (activeUploads.has(fileId)) {
            return res.json({
                status: "processing",
                filename: fileName,
                message: "File is being uploaded to storage... please wait.",
                progress: "Running"
            });
        }

        // ৫. ব্যাকগ্রাউন্ড আপলোড ফাংশন
        const startUpload = async () => {
            activeUploads.add(fileId);
            console.log(`Starting background upload for: ${fileName} (${fileId})`);
            
            try {
                const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
                const response = await axios({ method: 'get', url: gDriveUrl, responseType: 'stream', timeout: 0 });

                const passThrough = new stream.PassThrough();
                response.data.pipe(passThrough);

                const upload = new Upload({
                    client: s3Client,
                    params: {
                        Bucket: process.env.R2_BUCKET_NAME,
                        Key: r2Key,
                        Body: passThrough,
                        ContentType: response.headers['content-type'] || 'video/mp4'
                    },
                    queueSize: 4,
                    partSize: 1024 * 1024 * 10
                });

                await upload.done();
                console.log(`Successfully finished: ${fileName}`);
            } catch (err) {
                console.error(`Upload error for ${fileId}:`, err.message);
            } finally {
                activeUploads.delete(fileId);
            }
        };

        // ফায়ার অ্যান্ড ফরগেট (Fire and Forget)
        startUpload();

        // ৬. প্রসেসিং রেসপন্স (এখন আসল নামসহ দেখাবে)
        res.json({
            status: "processing",
            filename: fileName,
            message: "Initialization successful. Processing in background.",
            progress: "Starting"
        });

    } catch (error) {
        console.error("Endpoint Error:", error.message);
        res.json({ status: "processing", message: "Retrying..." });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));


