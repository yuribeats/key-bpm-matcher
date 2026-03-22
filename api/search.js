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
  let useFts = false;
  let hasKeyFilter = false;

  if (q) {
    useFts = true;
    conditions.push("id IN (SELECT rowid FROM tracks_fts WHERE tracks_fts MATCH ?)");
    params.push(`"${q.replace(/"/g, '""')}"`);
  }

  if (key) {
    hasKeyFilter = true;
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
    return res.status(200).json({ total: 0, tracks: [], page: 0, limit: 100, hasMore: false });
  }

  const where = "WHERE " + conditions.join(" AND ");

  const allowedSorts = { artist: "artist COLLATE NOCASE", title: "title COLLATE NOCASE", bpm: "bpm", key: "key_name" };
  const sortCol = allowedSorts[sort] || "artist COLLATE NOCASE";
  const sortDir = dir === "desc" ? "DESC" : "ASC";
  const offset = parseInt(page) * parseInt(limit);
  const lim = Math.min(parseInt(limit), 200);

  // Pick index hint to avoid temp sort on large result sets
  let indexHint = "";
  if (hasKeyFilter && !useFts) {
    if (sort === "artist" || sort === undefined) indexHint = "INDEXED BY idx_key_artist_bpm";
    else if (sort === "title") indexHint = "INDEXED BY idx_key_title_bpm";
  }

  try {
    let dataResult;
    try {
      dataResult = await client.execute({
        sql: `SELECT id, artist, title, bpm, key_name FROM tracks ${indexHint} ${where} ORDER BY ${sortCol} ${sortDir} LIMIT ? OFFSET ?`,
        args: [...params, lim + 1, offset],
      });
    } catch (ftsErr) {
      if (useFts) {
        const likeConditions = conditions.slice();
        const likeParams = params.slice();
        likeConditions[0] = "(artist LIKE ? COLLATE NOCASE OR title LIKE ? COLLATE NOCASE)";
        likeParams[0] = `%${q}%`;
        likeParams.splice(1, 0, `%${q}%`);
        const likeWhere = "WHERE " + likeConditions.join(" AND ");
        dataResult = await client.execute({
          sql: `SELECT id, artist, title, bpm, key_name FROM tracks ${likeWhere} ORDER BY ${sortCol} ${sortDir} LIMIT ? OFFSET ?`,
          args: [...likeParams, lim + 1, offset],
        });
      } else if (indexHint) {
        // Index hint failed, retry without it
        dataResult = await client.execute({
          sql: `SELECT id, artist, title, bpm, key_name FROM tracks ${where} ORDER BY ${sortCol} ${sortDir} LIMIT ? OFFSET ?`,
          args: [...params, lim + 1, offset],
        });
      } else {
        throw ftsErr;
      }
    }

    const hasMore = dataResult.rows.length > lim;
    const rows = dataResult.rows.slice(0, lim);
    const tracks = rows.map((r) => ({
      id: r.id,
      artist: r.artist,
      title: r.title,
      bpm: r.bpm,
      key: r.key_name,
    }));

    res.status(200).json({ total: hasMore ? -1 : offset + tracks.length, tracks, page: parseInt(page), limit: lim, hasMore });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Database query failed" });
  }
};
