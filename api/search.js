import { createClient } from "@libsql/client";

const client = createClient({
  url: process.env.TURSO_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (req.method === "OPTIONS") return res.status(200).end();

  const {
    q = "",
    key = "",
    bpmMin = "",
    bpmMax = "",
    filter = "all",
    sort = "artist",
    dir = "asc",
    page = "0",
    limit = "100",
  } = req.query;

  const conditions = [];
  const params = [];

  if (q) {
    conditions.push("(artist LIKE ? COLLATE NOCASE OR title LIKE ? COLLATE NOCASE)");
    params.push(`%${q}%`, `%${q}%`);
  }

  if (key) {
    const keys = key.split(",");
    const placeholders = keys.map(() => "?").join(",");
    conditions.push(`key_name IN (${placeholders})`);
    params.push(...keys);
  }

  if (bpmMin) {
    conditions.push("bpm >= ?");
    params.push(parseFloat(bpmMin));
  }
  if (bpmMax) {
    conditions.push("bpm <= ?");
    params.push(parseFloat(bpmMax));
  }

  if (filter === "both") {
    conditions.push("bpm IS NOT NULL AND key_name IS NOT NULL");
  } else if (filter === "bpm") {
    conditions.push("bpm IS NOT NULL");
  } else if (filter === "key") {
    conditions.push("key_name IS NOT NULL");
  }

  const where = conditions.length > 0 ? "WHERE " + conditions.join(" AND ") : "";

  const allowedSorts = { artist: "artist", title: "title", bpm: "bpm", key: "key_name" };
  const sortCol = allowedSorts[sort] || "artist";
  const sortDir = dir === "desc" ? "DESC" : "ASC";
  const offset = parseInt(page) * parseInt(limit);
  const lim = Math.min(parseInt(limit), 200);

  try {
    const [countResult, dataResult] = await Promise.all([
      client.execute({ sql: `SELECT COUNT(*) as total FROM tracks ${where}`, args: params }),
      client.execute({
        sql: `SELECT id, artist, title, bpm, key_name FROM tracks ${where} ORDER BY ${sortCol} ${sortDir} LIMIT ? OFFSET ?`,
        args: [...params, lim, offset],
      }),
    ]);

    const total = countResult.rows[0].total;
    const tracks = dataResult.rows.map((r) => ({
      id: r.id,
      artist: r.artist,
      title: r.title,
      bpm: r.bpm,
      key: r.key_name,
    }));

    res.status(200).json({ total, tracks, page: parseInt(page), limit: lim });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Database query failed" });
  }
}
