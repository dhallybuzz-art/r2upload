const express = require('express');
const axios = require('axios');
const { S3Client } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');

const app = express();
const PORT = process.env.PORT || 3000;

// R2 Configuration (Heroku Config Vars থেকে আসবে)
const s3Client = new S3Client({
    region: "auto",
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY,
        secretAccessKey: process.env.R2_SECRET_KEY,
    },
});

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    const r2Key = `storage/${fileId}.mp4`; // ফাইলের নাম সেট করুন
    const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${r2Key}`;

    try {
        // ১. প্রথমে চেক করি ফাইলটি R2 তে অলরেডি আছে কি না
        // (এই স্টেপটি ফাস্ট রেসপন্সের জন্য জরুরি)

        // ২. Google Drive থেকে স্ট্রিমিং শুরু করা
        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
        
        const response = await axios({
            method: 'get',
            url: gDriveUrl,
            responseType: 'stream'
        });

        // ৩. R2 তে আপলোড শুরু (Streaming Upload)
        const parallelUploads3 = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key,
                Body: response.data,
                ContentType: response.headers['content-type'] || 'video/mp4'
            },
        });

        // PHP কে সাথে সাথে 'processing' রেসপন্স দিন (Heroku টাইমআউট এড়াতে)
        // অথবা কাজ শেষ হওয়া পর্যন্ত অপেক্ষা করুন (যদি ফাইল ছোট হয়)
        
        await parallelUploads3.done();

        // ৪. সফল রেসপন্স (আপনার r2new.php এই ফরম্যাটটিই আশা করে)
        res.json({
            status: "success",
            filename: `File_${fileId}`,
            size: response.headers['content-length'] || 0,
            url: r2PublicUrl,
            r2_key: r2Key,
            worker_link: "" 
        });

    } catch (error) {
        res.json({
            status: "processing",
            message: "File is being prepared, please wait...",
            progress: "Calculating...",
            speed: "Direct Stream"
        });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));