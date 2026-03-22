import sqlite3
import json
import os

conn = sqlite3.connect(":memory:")

# Attach all source databases
conn.execute("ATTACH DATABASE 'bpm.db' AS bdb")        # Waterloo BPM
conn.execute("ATTACH DATABASE 'keys.db' AS kdb")        # SongKeyFinder
conn.execute("ATTACH DATABASE 'duuzu.db' AS ddb")       # duuzu
conn.execute("ATTACH DATABASE 'kaggle.db' AS kagdb")    # Kaggle Spotify
conn.execute("ATTACH DATABASE 'musicoset.db' AS mdb")   # MusicOSet

HAS_AB = os.path.exists("acousticbrainz.db")
if HAS_AB:
    conn.execute("ATTACH DATABASE 'acousticbrainz.db' AS abdb")  # AcousticBrainz
    print("AcousticBrainz db found, including it")

# Create staging table - dump everything in, then dedupe
conn.execute("""
    CREATE TABLE stage (
        artist TEXT,
        title TEXT,
        bpm REAL,
        key_name TEXT,
        duration TEXT,
        year INTEGER,
        genre TEXT,
        source TEXT
    )
""")

# 1. Waterloo BPM db (has bpm, year, genre, duration - no key)
conn.execute("""
    INSERT INTO stage (artist, title, bpm, duration, year, genre, source)
    SELECT artist, title, bpm, duration, year, genre, 'waterloo'
    FROM bdb.tracks
""")
print(f"After Waterloo: {conn.execute('SELECT COUNT(*) FROM stage').fetchone()[0]}")

# 2. SongKeyFinder (has key - no bpm)
conn.execute("""
    INSERT INTO stage (artist, title, key_name, source)
    SELECT artist, title, key_name, 'songkeyfinder'
    FROM kdb.tracks
""")
print(f"After SongKeyFinder: {conn.execute('SELECT COUNT(*) FROM stage').fetchone()[0]}")

# 3. duuzu (has key + bpm)
conn.execute("""
    INSERT INTO stage (artist, title, bpm, key_name, source)
    SELECT artist, title, bpm, key_name, 'duuzu'
    FROM ddb.tracks
""")
print(f"After duuzu: {conn.execute('SELECT COUNT(*) FROM stage').fetchone()[0]}")

# 4. Kaggle Spotify (has key + bpm + genre)
conn.execute("""
    INSERT INTO stage (artist, title, bpm, key_name, genre, source)
    SELECT artist, title, bpm, key_name, genre, 'kaggle'
    FROM kagdb.tracks
""")
print(f"After Kaggle: {conn.execute('SELECT COUNT(*) FROM stage').fetchone()[0]}")

# 5. MusicOSet (has key + bpm)
conn.execute("""
    INSERT INTO stage (artist, title, bpm, key_name, source)
    SELECT artist, title, bpm, key_name, 'musicoset'
    FROM mdb.tracks
""")
print(f"After MusicOSet: {conn.execute('SELECT COUNT(*) FROM stage').fetchone()[0]}")

# 6. AcousticBrainz (has key + bpm, if available)
if HAS_AB:
    conn.execute("""
        INSERT INTO stage (artist, title, bpm, key_name, source)
        SELECT artist, title, bpm, key_name, 'acousticbrainz'
        FROM abdb.tracks
    """)
    print(f"After AcousticBrainz: {conn.execute('SELECT COUNT(*) FROM stage').fetchone()[0]}")

# Now dedupe: group by normalized artist+title, pick best data
# Priority for key: duuzu > songkeyfinder > kaggle > musicoset > acousticbrainz
# Priority for bpm: waterloo > duuzu > kaggle > musicoset > acousticbrainz
print("\nDeduplicating...")

conn.execute("""
    CREATE TABLE combined (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        title TEXT,
        bpm REAL,
        key_name TEXT,
        duration TEXT,
        year INTEGER,
        genre TEXT,
        hq INTEGER DEFAULT 0
    )
""")

conn.execute("""
    INSERT INTO combined (artist, title, bpm, key_name, duration, year, genre, hq)
    SELECT
        -- Pick the longest artist string as canonical
        MAX(artist) as artist,
        MAX(title) as title,
        -- BPM: prefer waterloo, then duuzu, then kaggle, then musicoset, then acousticbrainz
        COALESCE(
            MAX(CASE WHEN source = 'waterloo' THEN bpm END),
            MAX(CASE WHEN source = 'duuzu' THEN bpm END),
            MAX(CASE WHEN source = 'kaggle' THEN bpm END),
            MAX(CASE WHEN source = 'musicoset' THEN bpm END),
            MAX(CASE WHEN source = 'acousticbrainz' THEN bpm END)
        ) as bpm,
        -- Key: prefer duuzu (has minor), then songkeyfinder, then kaggle, then musicoset, then acousticbrainz
        COALESCE(
            MAX(CASE WHEN source = 'duuzu' THEN key_name END),
            MAX(CASE WHEN source = 'songkeyfinder' THEN key_name END),
            MAX(CASE WHEN source = 'kaggle' THEN key_name END),
            MAX(CASE WHEN source = 'musicoset' THEN key_name END),
            MAX(CASE WHEN source = 'acousticbrainz' THEN key_name END)
        ) as key_name,
        MAX(duration) as duration,
        MAX(year) as year,
        MAX(genre) as genre,
        -- hq = 1 if any non-AcousticBrainz source contributed
        MAX(CASE WHEN source != 'acousticbrainz' THEN 1 ELSE 0 END) as hq
    FROM stage
    GROUP BY LOWER(TRIM(artist)), LOWER(TRIM(title))
""")

total = conn.execute("SELECT COUNT(*) FROM combined").fetchone()[0]
with_both = conn.execute("SELECT COUNT(*) FROM combined WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchone()[0]
bpm_only = conn.execute("SELECT COUNT(*) FROM combined WHERE bpm IS NOT NULL AND key_name IS NULL").fetchone()[0]
key_only = conn.execute("SELECT COUNT(*) FROM combined WHERE bpm IS NULL AND key_name IS NOT NULL").fetchone()[0]

hq_count = conn.execute("SELECT COUNT(*) FROM combined WHERE hq = 1").fetchone()[0]
ab_only = conn.execute("SELECT COUNT(*) FROM combined WHERE hq = 0").fetchone()[0]

print(f"\nFinal combined DB: {total} unique tracks")
print(f"  BPM + Key: {with_both}")
print(f"  BPM only:  {bpm_only}")
print(f"  Key only:  {key_only}")
print(f"  High-quality (non-AB source): {hq_count}")
print(f"  AcousticBrainz only: {ab_only}")

# Write to disk
print("\nWriting combined.db...")
disk = sqlite3.connect("combined.db")
disk.execute("DROP TABLE IF EXISTS tracks")
disk.execute("""
    CREATE TABLE tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT, title TEXT, bpm REAL, duration TEXT,
        year INTEGER, genre TEXT, key_name TEXT, hq INTEGER DEFAULT 0
    )
""")
rows = conn.execute("SELECT artist, title, bpm, duration, year, genre, key_name, hq FROM combined").fetchall()
disk.executemany(
    "INSERT INTO tracks (artist, title, bpm, duration, year, genre, key_name, hq) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    rows
)
disk.execute("CREATE INDEX idx_bpm ON tracks(bpm)")
disk.execute("CREATE INDEX idx_key ON tracks(key_name)")
disk.execute("CREATE INDEX idx_artist ON tracks(artist COLLATE NOCASE)")
disk.commit()
disk.close()

# Export JSON
print("Writing tracks.json...")
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

# Key distribution
print("\nTop keys:")
for row in conn.execute("SELECT key_name, COUNT(*) as cnt FROM combined WHERE key_name IS NOT NULL GROUP BY key_name ORDER BY cnt DESC LIMIT 15").fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
