const { createClient } = require("@libsql/client");

const client = createClient({
  url: process.env.TURSO_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (req.method === "OPTIONS") return res.status(200).end();

  const {
    q = "",
    key = "",
    bpmMin = "",
    bpmMax = "",
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
    params.push(parseFloat(bpmMin) - 0.5);
  }
  if (bpmMax) {
    conditions.push("bpm < ?");
    params.push(parseFloat(bpmMax) + 0.5);
  }

  if (conditions.length === 0) {
    return res.status(400).json({ error: "Please provide at least one filter (search text, key, or BPM range)" });
  }

  const where = "WHERE " + conditions.join(" AND ");

  const allowedSorts = { artist: "artist", title: "title", bpm: "bpm", key: "key_name" };
  const sortCol = allowedSorts[sort] || "artist";
  const sortDir = dir === "desc" ? "DESC" : "ASC";
  const offset = parseInt(page) * parseInt(limit);
  const lim = Math.min(parseInt(limit), 200);

  try {
    const dataResult = await client.execute({
      sql: `SELECT id, artist, title, bpm, key_name FROM tracks ${where} ORDER BY ${sortCol} ${sortDir} LIMIT ? OFFSET ?`,
      args: [...params, lim + 1, offset],
    });

    const hasMore = dataResult.rows.length > lim;
    const rows = dataResult.rows.slice(0, lim);
    const tracks = rows.map((r) => ({
      id: r.id,
      artist: r.artist,
      title: r.title,
      bpm: r.bpm,
      key: r.key_name,
    }));

    res.status(200).json({ total: hasMore ? "many" : offset + tracks.length, tracks, page: parseInt(page), limit: lim, hasMore });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Database query failed" });
  }
};
