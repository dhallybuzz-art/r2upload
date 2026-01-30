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

// আপলোড ট্র্যাকিং করার জন্য একটি সিম্পল অবজেক্ট
const activeUploads = new Set();

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    const r2Key = `storage/${fileId}.mp4`;
    const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${r2Key}`;

    try {
        // ১. চেক করি ফাইলটি ইতিমধ্যে R2 তে আছে কি না
        try {
            const headData = await s3Client.send(new HeadObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key
            }));
            
            return res.json({
                status: "success",
                filename: `Movie_${fileId}`,
                size: headData.ContentLength,
                url: r2PublicUrl,
                r2_key: r2Key
            });
        } catch (e) {
            console.log(`File ${fileId} not in R2.`);
        }

        // ২. যদি ফাইলটি এখন আপলোড হতে থাকে
        if (activeUploads.has(fileId)) {
            return res.json({
                status: "processing",
                message: "Uploading in progress... please wait.",
                progress: "50%" // স্ট্যাটিক মেসেজ
            });
        }

        // ৩. নতুন আপলোড শুরু করা (Background Task)
        const startUpload = async () => {
            activeUploads.add(fileId);
            try {
                const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
                const response = await axios({ method: 'get', url: gDriveUrl, responseType: 'stream', timeout: 0 });

                const passThrough = new stream.PassThrough();
                response.data.pipe(passThrough);

                const parallelUploads3 = new Upload({
                    client: s3Client,
                    params: {
                        Bucket: process.env.R2_BUCKET_NAME,
                        Key: r2Key,
                        Body: passThrough,
                        ContentType: response.headers['content-type'] || 'video/mp4'
                    },
                    queueSize: 1,
                    partSize: 1024 * 1024 * 5
                });

                await parallelUploads3.done();
                console.log(`Successfully uploaded: ${fileId}`);
            } catch (err) {
                console.error(`Upload failed for ${fileId}:`, err.message);
            } finally {
                activeUploads.delete(fileId);
            }
        };

        // আপলোড শুরু করুন কিন্তু 'await' করবেন না
        startUpload();

        // ৪. PHP-কে সাথে সাথে রেসপন্স দিন (Time-out এড়াতে)
        res.json({
            status: "processing",
            message: "Upload started. This might take a few minutes for large files.",
            progress: "10%"
        });

    } catch (error) {
        res.json({ status: "error", message: error.message });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
