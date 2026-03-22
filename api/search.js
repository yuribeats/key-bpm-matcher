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
    conditions.push("ROUND(bpm) >= ?");
    params.push(Math.round(parseFloat(bpmMin)));
  }
  if (bpmMax) {
    conditions.push("ROUND(bpm) <= ?");
    params.push(Math.round(parseFloat(bpmMax)));
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
    // Fetch limit+1 to know if there are more results
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

    // Estimate total: if on first page and we got fewer than limit, exact count is known
    // Otherwise, indicate there are more
    let total;
    if (!hasMore && offset === 0) {
      total = tracks.length;
    } else if (!hasMore) {
      total = offset + tracks.length;
    } else {
      // Only run count if we have filters (smaller result set)
      if (conditions.length > 0) {
        try {
          const countResult = await client.execute({
            sql: `SELECT COUNT(*) as total FROM tracks ${where}`,
            args: params,
          });
          total = countResult.rows[0].total;
        } catch {
          total = offset + lim + 1;
        }
      } else {
        total = 5722225; // known total
      }
    }

    res.status(200).json({ total, tracks, page: parseInt(page), limit: lim, hasMore });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Database query failed" });
  }
};
