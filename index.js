const express = require('express');
const axios = require('axios');
const { S3Client } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const stream = require('stream'); // স্ট্রিমিং লাইব্রেরি

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

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    const r2Key = `storage/${fileId}.mp4`;
    const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${r2Key}`;

    try {
        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
        
        // ১. রেসপন্স স্ট্রীম হিসেবে নেওয়া (মেমোরি সেভ করবে)
        const response = await axios({
            method: 'get',
            url: gDriveUrl,
            responseType: 'stream',
            timeout: 0 // বড় ফাইলের জন্য টাইমআউট বন্ধ রাখা
        });

        // ২. সরাসরি পাস-থ্রু স্ট্রিম তৈরি
        const passThrough = new stream.PassThrough();
        response.data.pipe(passThrough);

        const parallelUploads3 = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key,
                Body: passThrough, // স্ট্রীম বডি ব্যবহার করা হয়েছে
                ContentType: response.headers['content-type'] || 'video/mp4'
            },
            // ৩. বড় ফাইল যেন RAM না খায় তাই চাঙ্ক সাইজ কমানো (৫ মেগাবাইট করে)
            queueSize: 1, 
            partSize: 1024 * 1024 * 5 
        });

        // আপলোড চলাকালীন PHP কে একটি মেসেজ দিন যাতে সে ওয়েট করে
        parallelUploads3.done().then(() => {
            console.log("Upload Completed for:", fileId);
        }).catch(err => {
            console.error("Upload Error:", err);
        });

        // PHP কে সাথে সাথে সাকসেস বা প্রসেসিং রেসপন্স দিন
        // প্রথমবার কল করলে এটি সরাসরি সাকসেস ডাটা রিটার্ন করবে
        res.json({
            status: "success",
            filename: `Movie_${fileId}`,
            size: response.headers['content-length'] || "Unknown",
            url: r2PublicUrl,
            r2_key: r2Key
        });

    } catch (error) {
        console.error("Process Error:", error.message);
        res.status(500).json({ status: "error", message: error.message });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
