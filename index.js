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
    const apiKey = process.env.GDRIVE_API_KEY;

    try {
        // ১. মেটাডাটা ও পাবলিক চেক
        const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${apiKey}`;
        let fileName, fileSize;

        try {
            const meta = await axios.get(metaUrl);
            fileName = meta.data.name;
            fileSize = meta.data.size;
        } catch (e) {
            return res.json({ status: "error", message: "API Key invalid or File not public enough." });
        }

        const r2Key = fileName;
        const encodedKey = encodeURIComponent(r2Key);
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodedKey}`;

        // ২. R2 চেক
        try {
            await s3Client.send(new HeadObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: r2Key }));
            return res.json({ status: "success", filename: fileName, size: fileSize, url: r2PublicUrl });
        } catch (e) {}

        // ৩. আপলোড (অবশ্যই queueSize ১ রাখুন)
        if (!activeUploads.has(fileId)) {
            const startUpload = async () => {
                activeUploads.add(fileId);
                try {
                    const gUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${apiKey}`;
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
                        queueSize: 2, // Heroku র‍্যাম বাঁচাবে 
                        partSize: 50 * 1024 * 1024 // 10MB চাঙ্ক 
                    });
                    await upload.done();
                } catch (err) {
                    console.error("Critical: Google blocked the transfer.");
                } finally { activeUploads.delete(fileId); }
            };
            startUpload();
        }

        res.json({ status: "processing", filename: fileName });
    } catch (error) { res.json({ status: "error" }); }
});

app.listen(PORT, () => console.log(`Running on ${PORT}`));

