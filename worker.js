const Queue = require('bull');
const axios = require('axios');
const { S3Client } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const stream = require('stream');

const uploadQueue = new Queue('upload-task', process.env.REDIS_URL);

const s3Client = new S3Client({
    region: "auto",
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY,
        secretAccessKey: process.env.R2_SECRET_KEY,
    },
});

uploadQueue.process(async (job) => {
    const { fileId, fileName } = job.data;
    console.log(`Worker processing: ${fileName}`);

    try {
        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;
        const response = await axios({ method: 'get', url: gDriveUrl, responseType: 'stream' });

        const upload = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: fileName,
                Body: response.data.pipe(new stream.PassThrough()),
                ContentDisposition: `attachment; filename="${fileName}"`
            },
            partSize: 10 * 1024 * 1024, // 10MB parts
            queueSize: 1 
        });

        await upload.done();
        console.log(`Worker finished: ${fileName}`);
    } catch (err) {
        console.error(`Worker error: ${err.message}`);
        throw err; // এটি কিউ-কে রিট্রাই করতে বলবে
    }
});