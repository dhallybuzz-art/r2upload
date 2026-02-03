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
        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;

        // ১. প্রথম চেষ্টা: Google Drive API মেটাডেটা থেকে নাম সংগ্রহ
        try {
            const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name;
            fileSize = parseInt(metaResponse.data.size) || 0;
        } catch (e) {
            console.error(`Meta API failed for ${fileId}, trying Header method...`);
            
            // ২. বিকল্প পদ্ধতি: সরাসরি ডাউনলোড লিঙ্ক থেকে Headers চেক করা
            try {
                const headResponse = await axios.head(gDriveUrl);
                const contentDisp = headResponse.headers['content-disposition'];
                if (contentDisp && contentDisp.includes('filename=')) {
                    // filename="example.mkv" থেকে নাম বের করা
                    fileName = contentDisp.split('filename=')[1].replace(/['"]/g, '');
                }
                fileSize = parseInt(headResponse.headers['content-length']) || 0;
            } catch (headErr) {
                console.error(`Header method also failed:`, headErr.message);
            }
        }

        // যদি কোনোভাবেই নাম না পাওয়া যায়, তবেই কেবল আইডি ব্যবহার হবে (নিরাপত্তার জন্য)
        if (!fileName) {
            fileName = `File_${fileId}.mkv`;
        }

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
                    const response = await axios({ method: 'get', url: gDriveUrl, responseType: 'stream', timeout: 0 });

                    // যদি শুরুতে নাম না পাওয়া যেত, তবে ডাউনলোড স্ট্রিম থেকেও নাম নেওয়া সম্ভব
                    const finalFileName = fileName;

                    const upload = new Upload({
                        client: s3Client,
                        params: {
                            Bucket: process.env.R2_BUCKET_NAME,
                            Key: r2Key,
                            Body: response.data.pipe(new stream.PassThrough()),
                            ContentType: response.headers['content-type'] || 'video/x-matroska',
                            ContentDisposition: `attachment; filename="${finalFileName}"`
                        },
                        queueSize: 5,
                        partSize: 1024 * 1024 * 20
                    });

                    await upload.done();
                    console.log(`Successfully finished: ${finalFileName}`);
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
            message: "Fetching file and starting upload...",
            progress: "Running"
        });

    } catch (error) {
        res.json({ status: "error", message: "Something went wrong" });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
