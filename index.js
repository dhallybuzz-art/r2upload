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

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    const r2Key = `storage/${fileId}.mp4`;
    const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${r2Key}`;

    try {
        // ১. আগে চেক করি ফাইলটি R2 বাকেটে অলরেডি আছে কি না
        try {
            const headData = await s3Client.send(new HeadObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key
            }));
            
            // ফাইল থাকলে সাথে সাথে সাকসেস রেসপন্স দিন
            return res.json({
                status: "success",
                filename: `Movie_${fileId}`,
                size: headData.ContentLength,
                url: r2PublicUrl,
                r2_key: r2Key
            });
        } catch (e) {
            // ফাইল না থাকলে আপলোড প্রসেস শুরু হবে
            console.log("File not found in R2, starting upload...");
        }

        // ২. Google Drive থেকে স্ট্রীম রিকোয়েস্ট
        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
        
        const response = await axios({
            method: 'get',
            url: gDriveUrl,
            responseType: 'stream',
            timeout: 0
        });

        const passThrough = new stream.PassThrough();
        response.data.pipe(passThrough);

        // ৩. R2-তে স্মার্ট আপলোড
        const parallelUploads3 = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key,
                Body: passThrough,
                ContentType: response.headers['content-type'] || 'video/mp4'
            },
            queueSize: 1, // মেমোরি বাঁচাতে মাত্র ১টি চাঙ্ক কিউতে রাখা
            partSize: 1024 * 1024 * 5 // ৫ মেগাবাইট চাঙ্ক
        });

        // ৪. আপলোড সম্পূর্ণ হওয়া পর্যন্ত অপেক্ষা করা
        await parallelUploads3.done();
        console.log("Upload Completed for:", fileId);

        // ৫. সফল রেসপন্স
        res.json({
            status: "success",
            filename: `Movie_${fileId}`,
            size: response.headers['content-length'] || 0,
            url: r2PublicUrl,
            r2_key: r2Key
        });

    } catch (error) {
        console.error("Process Error:", error.message);
        
        // এরর হলে আপনার PHP যেন প্রসেসিং পেজেই থাকে তার ব্যবস্থা করা
        res.json({ 
            status: "processing", 
            message: "File is being prepared or transient error occurred. Refreshing...",
            error: error.message 
        });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
