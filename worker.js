const Queue = require('bull');
const axios = require('axios');
const { S3Client } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const stream = require('stream');

// Redis Connection (Heroku REDIS_URL ব্যবহার করবে)
const uploadQueue = new Queue('r2-upload-tasks', process.env.REDIS_URL);

// R2 Client Setup
const s3Client = new S3Client({
    region: "auto",
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY,
        secretAccessKey: process.env.R2_SECRET_KEY,
    },
});

uploadQueue.process(async (job) => {
    const { fileId, fileName, contentType } = job.data;
    console.log(`Worker starting: ${fileName}`);

    try {
        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
        
        // ১. গুগল ড্রাইভ থেকে ডাটা স্ট্রিম করা
        const response = await axios({
            method: 'get',
            url: gDriveUrl,
            responseType: 'stream',
            timeout: 0 // বড় ফাইলের জন্য আনলিমিটেড সময়
        });

        // ২. R2-তে স্ট্রিম সরাসরি আপলোড (Multipart Upload)
        const upload = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: fileName,
                Body: response.data.pipe(new stream.PassThrough()),
                ContentType: contentType || 'video/x-matroska',
                ContentDisposition: `attachment; filename="${fileName}"`
            },
            queueSize: 5, // Heroku-র কম র‍্যামের জন্য ১টি করে পার্ট আপলোড হবে
            partSize: 20 * 1024 * 1024 // ১০ মেগাবাইট করে পার্ট হবে
        });

        upload.on('httpUploadProgress', (progress) => {
            console.log(`Uploading ${fileName}: ${Math.round((progress.loaded / 1024 / 1024))} MB uploaded`);
        });

        await upload.done();
        console.log(`✅ Successfully uploaded: ${fileName}`);

    } catch (err) {
        console.error(`❌ Worker Error for ${fileName}:`, err.response ? `${err.response.status} - ${err.response.statusText}` : err.message);
        throw err; // এটি কিউকে পুনরায় চেষ্টার সুযোগ দেবে
    }
});

