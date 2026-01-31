const express = require("express");
const axios = require("axios");
const stream = require("stream");
const { S3Client, HeadObjectCommand } = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");

const app = express();
const PORT = process.env.PORT || 3000;

/* ---------- R2 ---------- */
const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY,
    secretAccessKey: process.env.R2_SECRET_KEY,
  },
});

const active = new Set();

const safe = (n) => n.replace(/[^\w.\-()]/g, "_");

app.get("/:id", async (req, res) => {
  const id = req.params.id;
  if (!id || id.length < 15)
    return res.status(400).json({ error: "Invalid ID" });

  let fileName = `file_${id}.bin`;
  let size = 0;

  /* ---------- Metadata (optional) ---------- */
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
    fileName = safe(meta.data.name || fileName);
    size = Number(meta.data.size || 0);
  } catch (_) {}

  const r2Key = fileName;
  const publicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(
    r2Key
  )}`;

  /* ---------- Already uploaded ---------- */
  try {
    const h = await s3.send(
      new HeadObjectCommand({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: r2Key,
      })
    );
    return res.json({
      status: "ready",
      filename: r2Key,
      size: h.ContentLength,
      url: publicUrl,
    });
  } catch (_) {}

  /* ---------- Upload ---------- */
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
          partSize: 10 * 1024 * 1024,
        });

        await upload.done();
        console.log("Uploaded:", r2Key);
      } catch (e) {
        console.error("UPLOAD ERROR:", e.message);
      } finally {
        active.delete(id);
      }
    })();
  }

  res.json({
    status: "processing",
    filename: r2Key,
    message: "Upload started. Refresh later.",
  });
});

app.listen(PORT, () =>
  console.log(`Server running on port ${PORT}`)
);
