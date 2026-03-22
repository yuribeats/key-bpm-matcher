import sqlite3
import json

conn = sqlite3.connect(":memory:")

# Load all three databases
conn.execute("ATTACH DATABASE 'bpm.db' AS bdb")
conn.execute("ATTACH DATABASE 'keys.db' AS kdb")
conn.execute("ATTACH DATABASE 'duuzu.db' AS ddb")

# Strategy: build a unified view
# 1. Start with BPM db (has bpm, year, genre, duration)
# 2. Join key db (has key)
# 3. Join duuzu db (has key + bpm)
# 4. Add duuzu-only tracks
# 5. Add key-only tracks

conn.execute("""
    CREATE TABLE combined (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        title TEXT,
        bpm REAL,
        duration TEXT,
        year INTEGER,
        genre TEXT,
        key_name TEXT
    )
""")

# Step 1: BPM db (deduped) + key db + duuzu
conn.execute("""
    INSERT INTO combined (artist, title, bpm, duration, year, genre, key_name)
    SELECT
        b.artist,
        b.title,
        b.bpm,
        b.duration,
        b.year,
        b.genre,
        COALESCE(d.key_name, k.key_name) as key_name
    FROM (
        SELECT artist, title, MIN(bpm) as bpm, duration, year, genre
        FROM bdb.tracks
        GROUP BY artist COLLATE NOCASE, title COLLATE NOCASE
    ) b
    LEFT JOIN (
        SELECT artist, title, key_name, MAX(popularity) as popularity
        FROM kdb.tracks
        GROUP BY artist COLLATE NOCASE, title COLLATE NOCASE
    ) k ON b.artist = k.artist COLLATE NOCASE AND b.title = k.title COLLATE NOCASE
    LEFT JOIN (
        SELECT artist, title, key_name, bpm
        FROM ddb.tracks
        GROUP BY artist COLLATE NOCASE, title COLLATE NOCASE
    ) d ON b.artist = d.artist COLLATE NOCASE AND b.title = d.title COLLATE NOCASE
""")

bpm_count = conn.execute("SELECT COUNT(*) FROM combined").fetchone()[0]
print(f"After BPM db merge: {bpm_count}")

# Step 2: Add duuzu-only tracks (not in BPM db)
conn.execute("""
    INSERT INTO combined (artist, title, bpm, key_name)
    SELECT d.artist, d.title, d.bpm, d.key_name
    FROM (
        SELECT artist, title, bpm, key_name
        FROM ddb.tracks
        GROUP BY artist COLLATE NOCASE, title COLLATE NOCASE
    ) d
    LEFT JOIN (
        SELECT DISTINCT artist, title FROM bdb.tracks
    ) b ON d.artist = b.artist COLLATE NOCASE AND d.title = b.title COLLATE NOCASE
    WHERE b.artist IS NULL
""")

after_duuzu = conn.execute("SELECT COUNT(*) FROM combined").fetchone()[0]
print(f"After duuzu merge: {after_duuzu} (+{after_duuzu - bpm_count} duuzu-only)")

# Step 3: Add key-only tracks (not in BPM db or duuzu)
conn.execute("""
    INSERT INTO combined (artist, title, key_name)
    SELECT k.artist, k.title, k.key_name
    FROM (
        SELECT artist, title, key_name
        FROM kdb.tracks
        GROUP BY artist COLLATE NOCASE, title COLLATE NOCASE
    ) k
    LEFT JOIN combined c ON k.artist = c.artist COLLATE NOCASE AND k.title = c.title COLLATE NOCASE
    WHERE c.artist IS NULL
""")

total = conn.execute("SELECT COUNT(*) FROM combined").fetchone()[0]
print(f"After key-only merge: {total}")

# Stats
with_both = conn.execute("SELECT COUNT(*) FROM combined WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchone()[0]
bpm_only = conn.execute("SELECT COUNT(*) FROM combined WHERE bpm IS NOT NULL AND key_name IS NULL").fetchone()[0]
key_only = conn.execute("SELECT COUNT(*) FROM combined WHERE bpm IS NULL AND key_name IS NOT NULL").fetchone()[0]

print(f"\nFinal combined DB: {total} tracks")
print(f"  BPM + Key: {with_both}")
print(f"  BPM only:  {bpm_only}")
print(f"  Key only:  {key_only}")

# Write to disk
disk = sqlite3.connect("combined.db")
conn.execute("CREATE INDEX idx_bpm ON combined(bpm)")
conn.execute("CREATE INDEX idx_key ON combined(key_name)")
conn.execute("CREATE INDEX idx_artist ON combined(artist COLLATE NOCASE)")

# Dump to disk db
disk.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT, title TEXT, bpm REAL, duration TEXT,
        year INTEGER, genre TEXT, key_name TEXT
    )
""")
disk.execute("DELETE FROM tracks")
rows = conn.execute("SELECT artist, title, bpm, duration, year, genre, key_name FROM combined").fetchall()
disk.executemany(
    "INSERT INTO tracks (artist, title, bpm, duration, year, genre, key_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
    rows
)
disk.execute("CREATE INDEX IF NOT EXISTS idx_bpm ON tracks(bpm)")
disk.execute("CREATE INDEX IF NOT EXISTS idx_key ON tracks(key_name)")
disk.execute("CREATE INDEX IF NOT EXISTS idx_artist ON tracks(artist COLLATE NOCASE)")
disk.commit()
disk.close()

# Export JSON
data = []
for r in rows:
    data.append({
        "artist": r[0], "title": r[1], "bpm": r[2],
        "duration": r[3], "year": r[4], "genre": r[5], "key": r[6]
    })

with open("tracks.json", "w") as f:
    json.dump(data, f)

mb = round(len(json.dumps(data)) / 1024 / 1024, 1)
print(f"\nExported {len(data)} tracks to tracks.json ({mb} MB)")

conn.close()
