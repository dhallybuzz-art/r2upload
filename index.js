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

    const r2KeyForStorage = `${fileId}.mp4`; 
    const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${r2KeyForStorage}`;

    try {
        // ১. ফাইলের মেটাডাটা সংগ্রহ
        const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
        let fileName = `Movie_${fileId}`; 
        let fileSize = 0;

        try {
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name; 
            fileSize = parseInt(metaResponse.data.size) || 0;
        } catch (e) {
            console.error("Metadata Fetch Failed - Check API Key or File Privacy");
        }

        // ২. R2 চেক (যদি আগে আপলোড হয়ে থাকে)
        try {
            const headData = await s3Client.send(new HeadObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2KeyForStorage
            }));
            
            return res.json({
                status: "success",
                filename: fileName,
                size: headData.ContentLength || fileSize,
                url: r2PublicUrl,
                r2_key: fileName 
            });
        } catch (e) {}

        // ৩. ব্যাকগ্রাউন্ড আপলোড লজিক
        if (!activeUploads.has(fileId)) {
            const startUpload = async () => {
                activeUploads.add(fileId);
                console.log(`Starting background upload: ${fileName}`);
                
                try {
                    const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
                    
                    // ৪MD এরর এড়াতে সরাসরি স্ট্রীম ব্যবহার
                    const response = await axios({ method: 'get', url: gDriveUrl, responseType: 'stream', timeout: 0 });

                    const upload = new Upload({
                        client: s3Client,
                        params: {
                            Bucket: process.env.R2_BUCKET_NAME,
                            Key: r2KeyForStorage,
                            Body: response.data.pipe(new stream.PassThrough()),
                            ContentType: response.headers['content-type'] || 'video/mp4'
                        },
                        queueSize: 4, // স্পিড বাড়াবে
                        partSize: 1024 * 1024 * 10 // ১০ মেগাবাইট চাঙ্ক (মেমোরি সাশ্রয়ী)
                    });

                    await upload.done();
                    console.log(`Successfully uploaded: ${fileName}`);
                } catch (err) {
                    console.error(`Upload error (Check 403 Forbidden):`, err.message);
                } finally {
                    activeUploads.delete(fileId);
                }
            };
            startUpload();
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "Initialization successful. Processing in background.",
            progress: "Running"
        });

    } catch (error) {
        res.json({ status: "processing", message: "Retrying..." });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
