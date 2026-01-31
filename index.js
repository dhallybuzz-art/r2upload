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

/* ---------- In-memory upload lock ---------- */
const active = new Set();

/* ---------- Utils ---------- */
const safeName = (name) => name.replace(/[^\w.\-()]/g, "_");

const formatSize = (bytes) => {
  if (!bytes || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(2) + " " + units[i];
};

/* ---------- Fix favicon 400 ---------- */
app.get("/favicon.ico", (req, res) => {
  res.status(204).end();
});

/* ---------- Main Route ---------- */
app.get("/:id", async (req, res) => {
  const id = req.params.id;
  if (!id || id.length < 15) {
    return res.status(400).json({ error: "Invalid File ID" });
  }

  let fileName = `file_${id}.bin`;
  let gdriveSize = 0;

  /* ---------- Google Drive metadata ---------- */
  try {
    const meta = await axios.get(
      `https://www.googleapis.com/drive/v3/files/${id}`,
      {
        params: {
          fields: "name,size",
          key: process.env.GDRIVE_API_KEY,
        },
      }
    );

    fileName = safeName(meta.data.name || fileName);
    gdriveSize = Number(meta.data.size || 0);
  } catch (_) {
    // metadata optional
  }

  const r2Key = fileName;
  const publicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(
    r2Key
  )}`;

  /* ---------- Check R2 ---------- */
  try {
    const head = await s3.send(
      new HeadObjectCommand({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: r2Key,
      })
    );

    const realSize =
      head.ContentLength && head.ContentLength > 1024 * 1024
        ? head.ContentLength
        : gdriveSize;

    return res.json({
      status: "ready",
      filename: r2Key,
      size: realSize,
      size_human: formatSize(realSize),
      url: publicUrl,
    });
  } catch (_) {
    // file not found → continue
  }

  /* ---------- Start upload (background) ---------- */
  if (!active.has(id)) {
    active.add(id);

    (async () => {
      try {
        const downloadUrl = `https://drive.usercontent.google.com/download?id=${id}&export=download&confirm=t`;

        const gstream = await axios({
          method: "GET",
          url: downloadUrl,
          responseType: "stream",
          timeout: 0,
        });

        const upload = new Upload({
          client: s3,
          params: {
            Bucket: process.env.R2_BUCKET_NAME,
            Key: r2Key,
            Body: gstream.data.pipe(new stream.PassThrough()),
            ContentType:
              gstream.headers["content-type"] ||
              "application/octet-stream",
            ContentDisposition: `attachment; filename="${r2Key}"`,
          },
          queueSize: 4,
          partSize: 10 * 1024 * 1024, // 10MB
        });

        await upload.done();
        console.log("Uploaded:", r2Key);
      } catch (err) {
        console.error("UPLOAD ERROR:", err.message);
      } finally {
        active.delete(id);
      }
    })();
  }

  return res.json({
    status: "processing",
    filename: r2Key,
    size: gdriveSize,
    size_human: formatSize(gdriveSize),
    message: "Upload started. Please refresh after some time.",
  });
});

/* ---------- Start server ---------- */
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
