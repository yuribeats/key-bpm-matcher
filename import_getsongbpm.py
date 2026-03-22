"""
Scrape GetSongBPM API for key+BPM data.
Requires: GETSONGBPM_KEY env var

Strategy: iterate through artist search A-Z, then fetch songs per artist.
"""
import os
import sys
import time
import requests
import sqlite3

API_KEY = os.environ.get("GETSONGBPM_KEY")
if not API_KEY:
    print("Set GETSONGBPM_KEY env var first")
    sys.exit(1)

BASE = "https://api.getsongbpm.com"
DB = "getsongbpm.db"

conn = sqlite3.connect(DB)
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

KEY_MAP = {
    "C": "C", "C#": "Db", "Db": "Db", "D": "D", "D#": "Eb", "Eb": "Eb",
    "E": "E", "F": "F", "F#": "Gb", "Gb": "Gb", "G": "G", "G#": "Ab",
    "Ab": "Ab", "A": "A", "A#": "Bb", "Bb": "Bb", "B": "B"
}

OPEN_KEY_MAP = {
    "1d": "Ab Minor", "1m": "B Major",
    "2d": "Eb Minor", "2m": "Gb Major",
    "3d": "Bb Minor", "3m": "Db Major",
    "4d": "F Minor", "4m": "Ab Major",
    "5d": "C Minor", "5m": "Eb Major",
    "6d": "G Minor", "6m": "Bb Major",
    "7d": "D Minor", "7m": "F Major",
    "8d": "A Minor", "8m": "C Major",
    "9d": "E Minor", "9m": "G Major",
    "10d": "B Minor", "10m": "D Major",
    "11d": "Gb Minor", "11m": "A Major",
    "12d": "Db Minor", "12m": "E Major",
}

def parse_key(song):
    """Parse key from song response, using open_key for major/minor distinction."""
    open_key = (song.get("open_key") or "").strip().lower()
    if open_key and open_key in OPEN_KEY_MAP:
        return OPEN_KEY_MAP[open_key]

    raw = (song.get("key_of") or "").strip()
    if not raw:
        return None
    note = KEY_MAP.get(raw)
    if note:
        return f"{note} Major"
    return None

headers = {"User-Agent": "KeyBPMMatcher/1.0"}
seen_artists = set()
total = 0

# Search artists alphabetically and by common prefixes
queries = list("abcdefghijklmnopqrstuvwxyz")
queries += [f"{a}{b}" for a in "abcdefghijklmnopqrstuvwxyz" for b in "aeiou"]

for q in queries:
    # Search for artists
    try:
        r = requests.get(f"{BASE}/search/", params={
            "api_key": API_KEY, "type": "artist", "lookup": q
        }, headers=headers, timeout=15)
        time.sleep(1)

        if r.status_code != 200:
            print(f"  Artist search '{q}': HTTP {r.status_code}")
            continue

        data = r.json()
        artists = data.get("search", [])
        if not artists:
            continue

    except Exception as e:
        print(f"  ERROR artist search '{q}': {e}")
        continue

    for artist_info in artists:
        artist_id = artist_info.get("id")
        artist_name = artist_info.get("name", "").strip()
        if not artist_id or artist_id in seen_artists:
            continue
        seen_artists.add(artist_id)

        # Get artist's songs
        try:
            r2 = requests.get(f"{BASE}/artist/", params={
                "api_key": API_KEY, "id": artist_id
            }, headers=headers, timeout=15)
            time.sleep(1)

            if r2.status_code != 200:
                continue

            artist_data = r2.json().get("artist", {})
            songs = artist_data.get("songs", [])

            batch = []
            for s in songs:
                title = (s.get("title") or "").strip()
                if not title:
                    continue

                bpm = None
                try:
                    bpm = float(s.get("tempo", 0))
                    if bpm <= 0:
                        bpm = None
                except (ValueError, TypeError):
                    pass

                key_name = parse_key(s)

                if not bpm and not key_name:
                    continue

                batch.append((artist_name, title, bpm, key_name))

            if batch:
                conn.executemany(
                    "INSERT INTO tracks (artist, title, bpm, key_name) VALUES (?, ?, ?, ?)",
                    batch
                )
                conn.commit()
                total += len(batch)

        except Exception as e:
            print(f"  ERROR artist '{artist_name}': {e}")
            continue

    sys.stdout.write(f"\r  Query '{q}': {len(seen_artists)} artists, {total} tracks")
    sys.stdout.flush()

conn.execute("CREATE INDEX IF NOT EXISTS idx_at ON tracks(artist COLLATE NOCASE, title COLLATE NOCASE)")
conn.commit()

final = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
with_both = conn.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchone()[0]
print(f"\n\nGetSongBPM DB: {final} tracks ({with_both} with both key+bpm)")
conn.close()
