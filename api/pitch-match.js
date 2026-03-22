const { createClient } = require("@libsql/client");

const client = createClient({
  url: process.env.TURSO_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

const NOTES = ["C", "Db", "D", "Eb", "E", "F", "Gb", "G", "Ab", "A", "Bb", "B"];

function transposeKey(keyName, semitones) {
  const parts = keyName.split(" ");
  const note = parts[0];
  const mode = parts.slice(1).join(" ");
  const idx = NOTES.indexOf(note);
  if (idx === -1) return null;
  const newIdx = ((idx - semitones) % 12 + 12) % 12;
  return NOTES[newIdx] + " " + mode;
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (req.method === "OPTIONS") return res.status(200).end();

  const {
    key = "",
    bpmMin = "",
    bpmMax = "",
    range = "3",
    limit = "10",
    hq = "",
    q = "",
  } = req.query;

  if (!key || (!bpmMin && !bpmMax)) {
    return res.status(400).json({ error: "key and bpm required" });
  }

  const bMin = bpmMin ? parseFloat(bpmMin) : parseFloat(bpmMax);
  const bMax = bpmMax ? parseFloat(bpmMax) : parseFloat(bpmMin);
  const semiRange = Math.min(Math.max(parseInt(range) || 3, 1), 6);
  const lim = Math.min(parseInt(limit) || 10, 50);

  const queries = [];

  for (let n = -semiRange; n <= semiRange; n++) {
    if (n === 0) continue;

    const sourceKey = transposeKey(key, n);
    if (!sourceKey) continue;

    const factor = Math.pow(2, n / 12);
    const srcBpmMin = bMin / factor;
    const srcBpmMax = bMax / factor;

    const conditions = ["key_name = ?", "bpm >= ?", "bpm < ?"];
    const params = [sourceKey, srcBpmMin - 0.05, srcBpmMax + 0.05];

    if (hq === "1") conditions.push("hq = 1");

    if (q) {
      conditions.push("id IN (SELECT rowid FROM tracks_fts WHERE tracks_fts MATCH ?)");
      params.push(`"${q.replace(/"/g, '""')}"`);
    }

    const where = "WHERE " + conditions.join(" AND ");
    let indexHint = "";
    if (!q) {
      indexHint = hq === "1" ? "INDEXED BY idx_hq_key_bpm" : "INDEXED BY idx_key_bpm";
    }

    queries.push({
      shift: n,
      sourceKey,
      sourceBpmMin: Math.round(srcBpmMin * 10) / 10,
      sourceBpmMax: Math.round(srcBpmMax * 10) / 10,
      targetKey: key,
      promise: client.execute({
        sql: `SELECT id, artist, title, bpm, key_name FROM tracks ${indexHint} ${where} ORDER BY bpm ASC LIMIT ?`,
        args: [...params, lim],
      }).catch(() =>
        client.execute({
          sql: `SELECT id, artist, title, bpm, key_name FROM tracks ${where} ORDER BY bpm ASC LIMIT ?`,
          args: [...params, lim],
        })
      ).catch(() => ({ rows: [] })),
    });
  }

  try {
    const settled = await Promise.all(queries.map((q) => q.promise));
    const results = queries.map((q, i) => ({
      shift: q.shift,
      sourceKey: q.sourceKey,
      sourceBpmMin: q.sourceBpmMin,
      sourceBpmMax: q.sourceBpmMax,
      targetKey: q.targetKey,
      tracks: settled[i].rows.map((r) => ({
        id: r.id,
        artist: r.artist,
        title: r.title,
        bpm: r.bpm,
        key: r.key_name,
      })),
    }));

    res.status(200).json({ results });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Query failed" });
  }
};
