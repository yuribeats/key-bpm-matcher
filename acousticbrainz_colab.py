"""
AcousticBrainz Import — Run in Google Colab
============================================
Cell 1: !pip install zstandard
Cell 2: paste everything below this line
"""

import subprocess
import os
import sqlite3
import csv
import io
import tarfile
import time

os.system("apt-get install -y zstd > /dev/null 2>&1")

WORK = "/content/acousticbrainz"
os.makedirs(WORK, exist_ok=True)
os.chdir(WORK)

DB = "acousticbrainz.db"
conn = sqlite3.connect(DB)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")

conn.execute("CREATE TABLE IF NOT EXISTS tonal (mbid TEXT PRIMARY KEY, key_key TEXT, key_scale TEXT, key_strength REAL)")
conn.execute("CREATE TABLE IF NOT EXISTS rhythm (mbid TEXT PRIMARY KEY, bpm REAL)")
conn.execute("CREATE TABLE IF NOT EXISTS recordings (mbid TEXT PRIMARY KEY, artist TEXT, title TEXT)")
conn.execute("CREATE TABLE IF NOT EXISTS tracks (id INTEGER PRIMARY KEY AUTOINCREMENT, artist TEXT, title TEXT, bpm REAL, key_name TEXT, key_strength REAL)")
conn.commit()

AB_BASE = "https://data.metabrainz.org/pub/musicbrainz/acousticbrainz/dumps/acousticbrainz-lowlevel-features-20220623"
MB_BASE = "https://data.metabrainz.org/pub/musicbrainz/canonical_data/musicbrainz-canonical-dump-20260317-080003"

def download(url, filename):
    if not os.path.exists(filename):
        print(f"  Downloading {filename}...")
        subprocess.run(["curl", "-L", "-o", filename, url], check=True)
    else:
        print(f"  {filename} already exists, skipping download")

# ============================================================
# STEP 1: Tonal features (key data) — 843MB compressed
# Format: mbid, submission_offset, key_key, key_scale, ...
# ============================================================
print("=" * 60)
print("STEP 1: Tonal features (key data)")
print("=" * 60)

tonal_file = "tonal.tar.zst"
download(f"{AB_BASE}/acousticbrainz-lowlevel-features-20220623-tonal.tar.zst", tonal_file)

if conn.execute("SELECT COUNT(*) FROM tonal").fetchone()[0] == 0:
    print("  Extracting and parsing...")
    proc = subprocess.Popen(["zstd", "-d", tonal_file, "--stdout"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    tar = tarfile.open(fileobj=proc.stdout, mode="r|")

    count = 0
    batch = []
    for member in tar:
        if not member.isfile():
            continue
        f = tar.extractfile(member)
        if f is None:
            continue
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
        for row in reader:
            mbid = row.get("mbid", "").strip()
            key_key = row.get("key_key", "").strip()
            key_scale = row.get("key_scale", "").strip()
            key_strength_str = row.get("key_strength", "").strip()
            if not mbid or not key_key:
                continue
            try:
                key_strength = float(key_strength_str) if key_strength_str else None
            except ValueError:
                key_strength = None
            batch.append((mbid, key_key, key_scale, key_strength))
            if len(batch) >= 50000:
                conn.executemany("INSERT OR IGNORE INTO tonal VALUES (?, ?, ?, ?)", batch)
                conn.commit()
                count += len(batch)
                batch = []
                print(f"  {count:,} tonal records...", end="\r")
    if batch:
        conn.executemany("INSERT OR IGNORE INTO tonal VALUES (?, ?, ?, ?)", batch)
        conn.commit()
        count += len(batch)
    proc.wait()
    tar.close()
    print(f"  Tonal done: {count:,} records       ")
else:
    print(f"  Already loaded: {conn.execute('SELECT COUNT(*) FROM tonal').fetchone()[0]:,}")

# ============================================================
# STEP 2: Rhythm features (BPM data) — 1GB compressed
# Format: mbid, submission_offset, bpm, ...
# ============================================================
print("\n" + "=" * 60)
print("STEP 2: Rhythm features (BPM data)")
print("=" * 60)

rhythm_file = "rhythm.tar.zst"
download(f"{AB_BASE}/acousticbrainz-lowlevel-features-20220623-rhythm.tar.zst", rhythm_file)

if conn.execute("SELECT COUNT(*) FROM rhythm").fetchone()[0] == 0:
    print("  Extracting and parsing...")
    proc = subprocess.Popen(["zstd", "-d", rhythm_file, "--stdout"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    tar = tarfile.open(fileobj=proc.stdout, mode="r|")

    count = 0
    batch = []
    for member in tar:
        if not member.isfile():
            continue
        f = tar.extractfile(member)
        if f is None:
            continue
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
        for row in reader:
            mbid = row.get("mbid", "").strip()
            bpm_str = row.get("bpm", "").strip()
            if not mbid or not bpm_str:
                continue
            try:
                bpm = float(bpm_str)
                if bpm <= 0:
                    continue
            except ValueError:
                continue
            batch.append((mbid, bpm))
            if len(batch) >= 50000:
                conn.executemany("INSERT OR IGNORE INTO rhythm VALUES (?, ?)", batch)
                conn.commit()
                count += len(batch)
                batch = []
                print(f"  {count:,} rhythm records...", end="\r")
    if batch:
        conn.executemany("INSERT OR IGNORE INTO rhythm VALUES (?, ?)", batch)
        conn.commit()
        count += len(batch)
    proc.wait()
    tar.close()
    print(f"  Rhythm done: {count:,} records       ")
else:
    print(f"  Already loaded: {conn.execute('SELECT COUNT(*) FROM rhythm').fetchone()[0]:,}")

# ============================================================
# STEP 3: MusicBrainz canonical metadata (artist + title) — 2GB
# Format: id, artist_credit_id, artist_mbids, artist_credit_name,
#          release_mbid, release_name, recording_mbid, recording_name, ...
# ============================================================
print("\n" + "=" * 60)
print("STEP 3: MusicBrainz metadata (artist + title)")
print("=" * 60)

# Get the set of mbids we need
tonal_ids = set(r[0] for r in conn.execute("SELECT mbid FROM tonal").fetchall())
rhythm_ids = set(r[0] for r in conn.execute("SELECT mbid FROM rhythm").fetchall())
all_ids = tonal_ids | rhythm_ids
print(f"  Need metadata for {len(all_ids):,} unique recording IDs")

mb_file = "canonical.tar.zst"
download(f"{MB_BASE}/musicbrainz-canonical-dump-20260317-080003.tar.zst", mb_file)

if conn.execute("SELECT COUNT(*) FROM recordings").fetchone()[0] == 0:
    print("  Extracting and matching recordings...")
    proc = subprocess.Popen(["zstd", "-d", mb_file, "--stdout"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    tar = tarfile.open(fileobj=proc.stdout, mode="r|")

    count = 0
    matched = 0
    batch = []
    for member in tar:
        if not member.isfile() or not member.name.endswith(".csv"):
            continue
        f = tar.extractfile(member)
        if f is None:
            continue
        print(f"  Parsing {member.name}...")
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
        for row in reader:
            count += 1
            mbid = row.get("recording_mbid", "").strip()
            if mbid not in all_ids:
                if count % 5000000 == 0:
                    print(f"  Scanned {count:,} rows, matched {matched:,}...", end="\r")
                continue
            artist = row.get("artist_credit_name", "").strip()
            title = row.get("recording_name", "").strip()
            if not artist or not title:
                continue
            batch.append((mbid, artist, title))
            matched += 1
            if len(batch) >= 10000:
                conn.executemany("INSERT OR IGNORE INTO recordings VALUES (?, ?, ?)", batch)
                conn.commit()
                batch = []
                print(f"  Scanned {count:,} rows, matched {matched:,}...", end="\r")
    if batch:
        conn.executemany("INSERT OR IGNORE INTO recordings VALUES (?, ?, ?)", batch)
        conn.commit()
    proc.wait()
    tar.close()
    print(f"  Metadata done: scanned {count:,} rows, matched {matched:,}       ")
else:
    print(f"  Already loaded: {conn.execute('SELECT COUNT(*) FROM recordings').fetchone()[0]:,}")

# ============================================================
# STEP 4: Join into final tracks table
# ============================================================
print("\n" + "=" * 60)
print("STEP 4: Building final tracks table")
print("=" * 60)

conn.execute("DELETE FROM tracks")
conn.execute("""
    INSERT INTO tracks (artist, title, bpm, key_name, key_strength)
    SELECT
        r.artist,
        r.title,
        rh.bpm,
        CASE WHEN t.key_key IS NOT NULL
            THEN t.key_key || ' ' || CASE
                WHEN t.key_scale = 'major' THEN 'Major'
                WHEN t.key_scale = 'minor' THEN 'Minor'
                ELSE t.key_scale END
            ELSE NULL END,
        t.key_strength
    FROM recordings r
    LEFT JOIN tonal t ON r.mbid = t.mbid
    LEFT JOIN rhythm rh ON r.mbid = rh.mbid
    WHERE t.key_key IS NOT NULL OR rh.bpm IS NOT NULL
""")
conn.commit()

conn.execute("CREATE INDEX IF NOT EXISTS idx_at ON tracks(artist COLLATE NOCASE, title COLLATE NOCASE)")
conn.commit()

total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
with_both = conn.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchone()[0]
print(f"\n  Total tracks: {total:,}")
print(f"  With BPM + Key: {with_both:,}")

print("\n  Sample tracks:")
for row in conn.execute("SELECT artist, title, bpm, key_name FROM tracks ORDER BY RANDOM() LIMIT 10").fetchall():
    bpm_str = f"{row[2]:.1f}" if row[2] else "-"
    print(f"    {row[0]} - {row[1]} | {bpm_str} BPM | {row[3] or '-'}")

# ============================================================
# STEP 5: Clean up, compact
# ============================================================
print("\n" + "=" * 60)
print("STEP 5: Cleanup")
print("=" * 60)

conn.execute("DROP TABLE IF EXISTS tonal")
conn.execute("DROP TABLE IF EXISTS rhythm")
conn.execute("DROP TABLE IF EXISTS recordings")
conn.execute("VACUUM")
conn.close()

db_size = os.path.getsize(DB) / 1024 / 1024
print(f"  Final db: {db_size:.1f} MB")
print(f"\n  DONE! Download acousticbrainz.db from the file browser (left sidebar).")
print(f"  Drop it into your project folder and run: python3 build_combined.py")
