const express = require("express");
const axios = require("axios");
const stream = require("stream");
const {
  S3Client,
  HeadObjectCommand,
  GetObjectCommand,
} = require("@aws-sdk/client-s3");
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
const safeName = (n) => n.replace(/[^\w.\-()]/g, "_");

const formatSize = (bytes) => {
  if (!bytes || bytes <= 0) return "0 B";
  const u = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(2) + " " + u[i];
};

/* ---------- favicon fix ---------- */
app.get("/favicon.ico", (_, res) => res.sendStatus(204));

/* ======================================================
   MAIN API — Upload trigger + status
   ====================================================== */
app.get("/:id", async (req, res) => {
  const id = req.params.id;
  if (!id || id.length < 15) {
    return res.status(400).json({ error: "Invalid File ID" });
  }

  let fileName = `file_${id}.bin`;
  let gdriveSize = 0;

  /* ---- Google Drive metadata ---- */
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
  } catch (_) {}

  const r2Key = fileName;

  /* ---- Check if already uploaded ---- */
  try {
    const head = await s3.send(
      new HeadObjectCommand({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: r2Key,
      })
    );

    // R2 metadata delay workaround
    const realSize =
      head.ContentLength && head.ContentLength > 1024 * 1024
        ? head.ContentLength
        : gdriveSize;

    return res.json({
      status: "ready",
      filename: r2Key,
      size: realSize,
      size_human: formatSize(realSize),
      download: `${req.protocol}://${req.get(
        "host"
      )}/download/${encodeURIComponent(r2Key)}`,
    });
  } catch (_) {
    // not uploaded yet
  }

  /* ---- Start background upload (only once) ---- */
  if (!active.has(id)) {
    active.add(id);

    (async () => {
      try {
        const gstream = await axios({
          method: "GET",
          url: `https://drive.usercontent.google.com/download?id=${id}&export=download&confirm=t`,
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

/* ======================================================
   DOWNLOAD PROXY — FULL RANGE SUPPORT (IDM FIX)
   ====================================================== */
app.get("/download/:key", async (req, res) => {
  const key = decodeURIComponent(req.params.key);

  try {
    const head = await s3.send(
      new HeadObjectCommand({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: key,
      })
    );

    const fileSize = head.ContentLength;
    const range = req.headers.range;

    /* ---- Full download (no Range header) ---- */
    if (!range) {
      const obj = await s3.send(
        new GetObjectCommand({
          Bucket: process.env.R2_BUCKET_NAME,
          Key: key,
        })
      );

      res.writeHead(200, {
        "Content-Length": fileSize,
        "Content-Type": obj.ContentType || "application/octet-stream",
        "Content-Disposition": `attachment; filename="${key}"`,
        "Accept-Ranges": "bytes",
      });

      return obj.Body.pipe(res);
    }

    /* ---- Range request (206 Partial Content) ---- */
    const parts = range.replace(/bytes=/, "").split("-");
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;

    if (start >= fileSize) {
      return res.status(416).send("Requested range not satisfiable");
    }

    const obj = await s3.send(
      new GetObjectCommand({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: key,
        Range: `bytes=${start}-${end}`,
      })
    );

    res.writeHead(206, {
      "Content-Range": `bytes ${start}-${end}/${fileSize}`,
      "Accept-Ranges": "bytes",
      "Content-Length": end - start + 1,
      "Content-Type": obj.ContentType || "application/octet-stream",
      "Content-Disposition": `attachment; filename="${key}"`,
    });

    obj.Body.pipe(res);
  } catch (err) {
    console.error("DOWNLOAD ERROR:", err.message);
    res.status(404).json({ error: "File not found" });
  }
});

/* ---------- Start server ---------- */
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
