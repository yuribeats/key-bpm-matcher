"""
AcousticBrainz Import — Run in Google Colab
============================================
1. Open https://colab.research.google.com
2. New notebook > paste this entire file into a cell
3. Run it (~30-60 min depending on download speed)
4. Download the output acousticbrainz.db from the file browser

This script:
- Downloads AcousticBrainz lowlevel feature CSVs (tonal + rhythm = ~1.8GB compressed)
- Downloads MusicBrainz recording metadata dump for artist/title lookup
- Streams and parses without storing full uncompressed data
- Outputs a SQLite db with artist, title, key, bpm
"""

import subprocess
import os
import sqlite3
import csv
import json
import io
import tarfile
import time

# Install zstd if not present
subprocess.run(["apt-get", "install", "-y", "zstd"], capture_output=True)

WORK = "/content/acousticbrainz"
os.makedirs(WORK, exist_ok=True)
os.chdir(WORK)

DB = "acousticbrainz.db"
conn = sqlite3.connect(DB)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")

conn.execute("""
    CREATE TABLE IF NOT EXISTS tonal (
        recording_id TEXT PRIMARY KEY,
        key_key TEXT,
        key_scale TEXT
    )
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS rhythm (
        recording_id TEXT PRIMARY KEY,
        bpm REAL
    )
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS recordings (
        recording_id TEXT PRIMARY KEY,
        artist TEXT,
        title TEXT
    )
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        title TEXT,
        bpm REAL,
        key_name TEXT
    )
""")
conn.commit()

BASE = "https://data.metabrainz.org/pub/musicbrainz/acousticbrainz/dumps/acousticbrainz-lowlevel-features-20220623"

KEY_NORMALIZE = {
    "C": "C", "C#": "Db", "Db": "Db", "D": "D", "D#": "Eb", "Eb": "Eb",
    "E": "E", "F": "F", "F#": "Gb", "Gb": "Gb", "G": "G", "G#": "Ab",
    "Ab": "Ab", "A": "A", "A#": "Bb", "Bb": "Bb", "B": "B"
}

# ============================================================
# STEP 1: Download and parse tonal features (key data)
# ============================================================
print("=" * 60)
print("STEP 1: Downloading tonal features (843MB)...")
print("=" * 60)

tonal_file = "acousticbrainz-lowlevel-features-20220623-tonal.tar.zst"
if not os.path.exists(tonal_file):
    subprocess.run(["curl", "-L", "-o", tonal_file, f"{BASE}/{tonal_file}"], check=True)

print("Extracting and parsing tonal features...")
# Decompress zstd, then read tar
proc = subprocess.Popen(["zstd", "-d", tonal_file, "--stdout"], stdout=subprocess.PIPE)
tar = tarfile.open(fileobj=proc.stdout, mode="r|")

tonal_count = 0
batch = []
for member in tar:
    if not member.isfile():
        continue
    f = tar.extractfile(member)
    if f is None:
        continue

    try:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
        for row in reader:
            rid = row.get("recording_id") or row.get("mbid") or ""
            rid = rid.strip()
            if not rid:
                # Try to get recording_id from filename
                # Format: lowlevel/UUID.json or similar
                name = member.name
                if "/" in name:
                    name = name.split("/")[-1]
                rid = name.replace(".json", "").replace(".csv", "").strip()

            key_key = row.get("key_key", row.get("tonal.key_key", "")).strip()
            key_scale = row.get("key_scale", row.get("tonal.key_scale", "")).strip()

            if not rid or not key_key:
                continue

            batch.append((rid, key_key, key_scale))
            if len(batch) >= 10000:
                conn.executemany("INSERT OR IGNORE INTO tonal VALUES (?, ?, ?)", batch)
                conn.commit()
                tonal_count += len(batch)
                batch = []
                if tonal_count % 500000 == 0:
                    print(f"  Tonal: {tonal_count:,} records...")
    except Exception as e:
        continue

if batch:
    conn.executemany("INSERT OR IGNORE INTO tonal VALUES (?, ?, ?)", batch)
    conn.commit()
    tonal_count += len(batch)

proc.wait()
tar.close()
print(f"  Tonal done: {tonal_count:,} records")

# ============================================================
# STEP 2: Download and parse rhythm features (BPM data)
# ============================================================
print("\n" + "=" * 60)
print("STEP 2: Downloading rhythm features (1GB)...")
print("=" * 60)

rhythm_file = "acousticbrainz-lowlevel-features-20220623-rhythm.tar.zst"
if not os.path.exists(rhythm_file):
    subprocess.run(["curl", "-L", "-o", rhythm_file, f"{BASE}/{rhythm_file}"], check=True)

print("Extracting and parsing rhythm features...")
proc = subprocess.Popen(["zstd", "-d", rhythm_file, "--stdout"], stdout=subprocess.PIPE)
tar = tarfile.open(fileobj=proc.stdout, mode="r|")

rhythm_count = 0
batch = []
for member in tar:
    if not member.isfile():
        continue
    f = tar.extractfile(member)
    if f is None:
        continue

    try:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
        for row in reader:
            rid = row.get("recording_id") or row.get("mbid") or ""
            rid = rid.strip()
            if not rid:
                name = member.name
                if "/" in name:
                    name = name.split("/")[-1]
                rid = name.replace(".json", "").replace(".csv", "").strip()

            bpm_str = row.get("bpm", row.get("rhythm.bpm", "")).strip()
            if not rid or not bpm_str:
                continue

            try:
                bpm = float(bpm_str)
                if bpm <= 0:
                    continue
            except ValueError:
                continue

            batch.append((rid, bpm))
            if len(batch) >= 10000:
                conn.executemany("INSERT OR IGNORE INTO rhythm VALUES (?, ?)", batch)
                conn.commit()
                rhythm_count += len(batch)
                batch = []
                if rhythm_count % 500000 == 0:
                    print(f"  Rhythm: {rhythm_count:,} records...")
    except Exception as e:
        continue

if batch:
    conn.executemany("INSERT OR IGNORE INTO rhythm VALUES (?, ?)", batch)
    conn.commit()
    rhythm_count += len(batch)

proc.wait()
tar.close()
print(f"  Rhythm done: {rhythm_count:,} records")

# ============================================================
# STEP 3: Get MusicBrainz recording metadata (artist + title)
# ============================================================
print("\n" + "=" * 60)
print("STEP 3: Downloading MusicBrainz recording metadata...")
print("=" * 60)

# The canonical dump is huge. Use the MusicBrainz API instead for the recordings
# we actually have. But that's millions of lookups...
#
# Alternative: download the musicbrainz canonical dump CSV
# https://data.metabrainz.org/pub/musicbrainz/data/fullexport/
#
# Actually, the most efficient approach: download the MB canonical data dump
# which has recording_id -> artist_credit_name, recording_name

MB_DUMP_URL = "https://data.metabrainz.org/pub/musicbrainz/data/fullexport/"

# Check latest dump
print("Finding latest MusicBrainz dump...")
import urllib.request

# Get the list of recording IDs we need
tonal_ids = set(r[0] for r in conn.execute("SELECT recording_id FROM tonal").fetchall())
rhythm_ids = set(r[0] for r in conn.execute("SELECT recording_id FROM rhythm").fetchall())
all_ids = tonal_ids | rhythm_ids
print(f"  Need metadata for {len(all_ids):,} recording IDs")

# For efficiency, use the MusicBrainz API with batch lookups
# Rate limit: 1 req/sec, but we can use the JSON web service
# which accepts multiple IDs via inc=

# Actually, the most practical approach for millions of records:
# Download the mbdump files which contain recording + artist_credit tables

# Let's try a different approach: use the musicbrainz-canonical-dump
# which is a single CSV with recording_id, artist, title
CANONICAL_URL = "https://data.metabrainz.org/pub/musicbrainz/canonical_data/canonical_musicbrainz_data.csv.bz2"

canonical_file = "canonical_musicbrainz_data.csv.bz2"
if not os.path.exists(canonical_file):
    print(f"Downloading MusicBrainz canonical data...")
    subprocess.run(["curl", "-L", "-o", canonical_file, CANONICAL_URL], check=True)

print("Parsing MusicBrainz canonical data...")
import bz2

rec_count = 0
batch = []
with bz2.open(canonical_file, "rt", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader, None)
    print(f"  Header: {header}")

    # Find column indices
    if header:
        cols = {h.strip().lower(): i for i, h in enumerate(header)}
    else:
        cols = {}

    rid_col = cols.get("recording_id", cols.get("recording_mbid", cols.get("id", 0)))
    artist_col = cols.get("artist_credit_name", cols.get("artist", cols.get("artist_name", 1)))
    title_col = cols.get("recording_name", cols.get("title", cols.get("name", 2)))

    for row in reader:
        if len(row) <= max(rid_col, artist_col, title_col):
            continue

        rid = row[rid_col].strip()
        if rid not in all_ids:
            continue

        artist = row[artist_col].strip()
        title = row[title_col].strip()

        if not artist or not title:
            continue

        batch.append((rid, artist, title))
        if len(batch) >= 10000:
            conn.executemany("INSERT OR IGNORE INTO recordings VALUES (?, ?, ?)", batch)
            conn.commit()
            rec_count += len(batch)
            batch = []
            if rec_count % 100000 == 0:
                print(f"  Recordings: {rec_count:,} matched...")

if batch:
    conn.executemany("INSERT OR IGNORE INTO recordings VALUES (?, ?, ?)", batch)
    conn.commit()
    rec_count += len(batch)

print(f"  Recordings matched: {rec_count:,}")

# ============================================================
# STEP 4: Join everything into final tracks table
# ============================================================
print("\n" + "=" * 60)
print("STEP 4: Building final tracks table...")
print("=" * 60)

conn.execute("DELETE FROM tracks")
conn.execute("""
    INSERT INTO tracks (artist, title, bpm, key_name)
    SELECT
        r.artist,
        r.title,
        rh.bpm,
        CASE
            WHEN t.key_key IS NOT NULL THEN t.key_key || ' ' ||
                CASE WHEN t.key_scale = 'major' THEN 'Major'
                     WHEN t.key_scale = 'minor' THEN 'Minor'
                     ELSE t.key_scale END
            ELSE NULL
        END as key_name
    FROM recordings r
    LEFT JOIN tonal t ON r.recording_id = t.recording_id
    LEFT JOIN rhythm rh ON r.recording_id = rh.recording_id
    WHERE t.key_key IS NOT NULL OR rh.bpm IS NOT NULL
""")
conn.commit()

conn.execute("CREATE INDEX IF NOT EXISTS idx_at ON tracks(artist COLLATE NOCASE, title COLLATE NOCASE)")
conn.commit()

total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
with_both = conn.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchone()[0]
bpm_only = conn.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL AND key_name IS NULL").fetchone()[0]
key_only = conn.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NULL AND key_name IS NOT NULL").fetchone()[0]

print(f"\nAcousticBrainz DB: {total:,} tracks")
print(f"  BPM + Key: {with_both:,}")
print(f"  BPM only:  {bpm_only:,}")
print(f"  Key only:  {key_only:,}")

# Show some samples
print("\nSample tracks:")
for row in conn.execute("SELECT artist, title, bpm, key_name FROM tracks ORDER BY RANDOM() LIMIT 10").fetchall():
    print(f"  {row[0]} - {row[1]} | {row[2]} BPM | {row[3]}")

# ============================================================
# STEP 5: Clean up intermediate tables, compact db
# ============================================================
print("\n" + "=" * 60)
print("STEP 5: Cleaning up...")
print("=" * 60)

conn.execute("DROP TABLE IF EXISTS tonal")
conn.execute("DROP TABLE IF EXISTS rhythm")
conn.execute("DROP TABLE IF EXISTS recordings")
conn.execute("VACUUM")
conn.close()

db_size = os.path.getsize(DB) / 1024 / 1024
print(f"\nFinal db size: {db_size:.1f} MB")
print(f"\nDONE! Download {DB} from the Colab file browser (left sidebar).")
print("Then drop it into ~/Desktop/Projects/key-bpm-matcher-2026-03-21/")
print("and run: python3 build_combined.py")
