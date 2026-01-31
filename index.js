const express = require("express");
const axios = require("axios");
const stream = require("stream");
const { S3Client, HeadObjectCommand } = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");

const app = express();
const PORT = process.env.PORT || 3000;

/* ---------- Cloudflare R2 ---------- */
const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY,
    secretAccessKey: process.env.R2_SECRET_KEY,
  },
});

const uploading = new Set();

/* ---------- Utils ---------- */
function safeName(name) {
  return name.replace(/[^\w.\-()]/g, "_");
}

/* ---------- Routes ---------- */
app.get("/favicon.ico", (_, res) => res.sendStatus(204));

app.get("/:fileId", async (req, res) => {
  const fileId = req.params.fileId;
  if (!fileId || fileId.length < 15)
    return res.status(400).json({ error: "Invalid File ID" });

  let fileName = `file_${fileId}`;
  let fileSize = 0;

  /* ---------- 1. Get metadata ---------- */
  try {
    const meta = await axios.get(
      `https://www.googleapis.com/drive/v3/files/${fileId}`,
      {
        params: {
          fields: "name,size,mimeType",
          key: process.env.GDRIVE_API_KEY,
        },
      }
    );

    fileName = safeName(meta.data.name || fileName);
    fileSize = Number(meta.data.size || 0);
  } catch {
    fileName += ".bin";
  }

  const r2Key = fileName;
  const publicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(
    r2Key
  )}`;

  /* ---------- 2. Already exists? ---------- */
  try {
    const head = await s3.send(
      new HeadObjectCommand({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: r2Key,
      })
    );

    return res.json({
      status: "ready",
      filename: r2Key,
      size: head.ContentLength,
      url: publicUrl,
    });
  } catch (_) {}

  /* ---------- 3. Start upload ---------- */
  if (!uploading.has(fileId)) {
    uploading.add(fileId);

    (async () => {
      try {
        const gdriveStream = await axios({
          method: "GET",
          url: `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`,
          params: { key: process.env.GDRIVE_API_KEY },
          responseType: "stream",
          timeout: 0,
        });

        const upload = new Upload({
          client: s3,
          params: {
            Bucket: process.env.R2_BUCKET_NAME,
            Key: r2Key,
            Body: gdriveStream.data.pipe(new stream.PassThrough()),
            ContentType:
              gdriveStream.headers["content-type"] ||
              "application/octet-stream",
            ContentDisposition: `attachment; filename="${r2Key}"`,
          },
          queueSize: 4,
          partSize: 10 * 1024 * 1024,
        });

        await upload.done();
        console.log("Uploaded:", r2Key);
      } catch (err) {
        console.error("Upload failed:", err.message);
      } finally {
        uploading.delete(fileId);
      }
    })();
  }

  res.json({
    status: "processing",
    filename: r2Key,
    message: "Upload started. Refresh later.",
  });
});

/* ---------- Start ---------- */
app.listen(PORT, () =>
  console.log(`Server running on http://localhost:${PORT}`)
);
