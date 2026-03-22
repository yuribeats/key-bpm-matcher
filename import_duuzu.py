import json
import sqlite3
import re

KEY_NORMALIZE = {
    'Cmaj': 'C Major', 'Cmin': 'C Minor',
    'C#maj': 'Db Major', 'C#min': 'Db Minor',
    'Dbmaj': 'Db Major', 'Dbmin': 'Db Minor',
    'Dmaj': 'D Major', 'Dmin': 'D Minor',
    'D#maj': 'Eb Major', 'D#min': 'Eb Minor',
    'Ebmaj': 'Eb Major', 'Ebmin': 'Eb Minor',
    'Emaj': 'E Major', 'Emin': 'E Minor',
    'Fmaj': 'F Major', 'Fmin': 'F Minor',
    'F#maj': 'Gb Major', 'F#min': 'Gb Minor',
    'Gbmaj': 'Gb Major', 'Gbmin': 'Gb Minor',
    'Gmaj': 'G Major', 'Gmin': 'G Minor',
    'G#maj': 'Ab Major', 'G#min': 'Ab Minor',
    'Abmaj': 'Ab Major', 'Abmin': 'Ab Minor',
    'Amaj': 'A Major', 'Amin': 'A Minor',
    'A#maj': 'Bb Major', 'A#min': 'Bb Minor',
    'Bbmaj': 'Bb Major', 'Bbmin': 'Bb Minor',
    'Bmaj': 'B Major', 'Bmin': 'B Minor',
}

with open('duuzu.json') as f:
    data = json.load(f)

conn = sqlite3.connect('duuzu.db')
conn.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        title TEXT,
        bpm REAL,
        key_name TEXT
    )
""")
conn.execute("DELETE FROM tracks")

count = 0
for t in data:
    artist = (t.get('artist') or '').strip()
    title = (t.get('name') or '').strip()
    if not artist or not title:
        continue

    # Parse key
    key_name = None
    keys = t.get('mainKeys', [])
    if keys and keys[0]:
        raw = keys[0].strip()
        key_name = KEY_NORMALIZE.get(raw)

    # Parse BPM - take first number, handle "84.5/169" format
    bpm = None
    bpms = t.get('bpms', [])
    if bpms and bpms[0]:
        raw = bpms[0].strip().lstrip('~')
        match = re.match(r'(\d+\.?\d*)', raw)
        if match:
            bpm = float(match.group(1))

    if not key_name and not bpm:
        continue

    conn.execute(
        "INSERT INTO tracks (artist, title, bpm, key_name) VALUES (?, ?, ?, ?)",
        (artist, title, bpm, key_name)
    )
    count += 1

conn.execute("CREATE INDEX IF NOT EXISTS idx_at ON tracks(artist COLLATE NOCASE, title COLLATE NOCASE)")
conn.commit()

total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
with_both = conn.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchone()[0]
print(f"Duuzu DB: {total} tracks ({with_both} with both key+bpm)")

# Show key distribution
print("\nBy key:")
for row in conn.execute("SELECT key_name, COUNT(*) as cnt FROM tracks WHERE key_name IS NOT NULL GROUP BY key_name ORDER BY cnt DESC").fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
