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
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error" });

    try {
        // ১. মেটাডাটা সংগ্রহ
        const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
        let fileName = `Movie_${fileId}.mkv`, fileSize = 0;

        try {
            const meta = await axios.get(metaUrl);
            fileName = meta.data.name;
            fileSize = parseInt(meta.data.size) || 0;
        } catch (e) { console.error("Metadata Fetch Failed!"); }

        const r2Key = fileName;
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(r2Key)}`;

        // ২. বাকেটে চেক
        try {
            const head = await s3Client.send(new HeadObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: r2Key }));
            return res.json({ status: "success", filename: fileName, size: head.ContentLength || fileSize, url: r2PublicUrl, r2_key: r2Key });
        } catch (e) {}

        // ৩. আপলোড প্রসেস
        if (!activeUploads.has(fileId)) {
            const startUpload = async () => {
                activeUploads.add(fileId);
                console.log(`Starting transfer for: ${fileName}`);
                try {
                    const gUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
                    const resp = await axios({ method: 'get', url: gUrl, responseType: 'stream' });

                    const upload = new Upload({
                        client: s3Client,
                        params: {
                            Bucket: process.env.R2_BUCKET_NAME,
                            Key: r2Key,
                            Body: resp.data.pipe(new stream.PassThrough()),
                            ContentType: 'video/x-matroska',
                            ContentDisposition: `attachment; filename="${fileName}"`
                        },
                        queueSize: 1, // RAM সাশ্রয়ী
                        partSize: 10 * 1024 * 1024 // 10MB চাঙ্ক
                    });
                    await upload.done();
                    console.log(`Success: ${fileName}`);
                } catch (err) { 
                    console.error(`Upload error for ${fileId}: ${err.message}`); 
                } finally { activeUploads.delete(fileId); }
            };
            startUpload();
        }
        res.json({ status: "processing", filename: fileName, message: "Transfer started..." });
    } catch (error) { res.json({ status: "processing" }); }
});

app.listen(PORT, () => console.log(`Active on port ${PORT}`));
