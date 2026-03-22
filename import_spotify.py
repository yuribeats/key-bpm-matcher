"""
Bulk import from Spotify API using audio-features batch endpoint.
Requires: SPOTIFY_CLIENT_ID + SPOTIFY_CLIENT_SECRET env vars

Strategy:
1. Search for tracks across genres/years/popularity ranges
2. Batch fetch audio features (100 tracks per request)
3. Store in spotify.db
"""
import os
import sys
import time
import requests
import sqlite3

CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
if not CLIENT_ID or not CLIENT_SECRET:
    print("Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET env vars first")
    sys.exit(1)

PITCH_CLASS = {0:'C',1:'Db',2:'D',3:'Eb',4:'E',5:'F',6:'Gb',7:'G',8:'Ab',9:'A',10:'Bb',11:'B'}

DB = "spotify.db"
conn = sqlite3.connect(DB)
conn.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        spotify_id TEXT UNIQUE,
        artist TEXT,
        title TEXT,
        bpm REAL,
        key_name TEXT,
        year INTEGER
    )
""")

def get_token():
    r = requests.post("https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET))
    r.raise_for_status()
    token = r.json()["access_token"]
    return token, time.time() + 3500  # refresh before 1hr expiry

token, token_expires = get_token()

def api(endpoint, params=None):
    global token, token_expires
    if time.time() > token_expires:
        token, token_expires = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"https://api.spotify.com/v1/{endpoint}", headers=headers, params=params, timeout=15)
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", 5))
        print(f"\n  Rate limited, waiting {wait}s...")
        time.sleep(wait)
        return api(endpoint, params)
    r.raise_for_status()
    return r.json()

# Search strategy: query by year ranges and genre seeds
GENRES = [
    "pop", "rock", "hip-hop", "r-n-b", "electronic", "dance", "indie",
    "alternative", "metal", "jazz", "classical", "country", "soul", "funk",
    "reggae", "blues", "latin", "punk", "folk", "disco", "house", "techno",
    "ambient", "grunge", "new-wave", "ska", "gospel", "world-music"
]

YEAR_RANGES = [
    "1960-1969", "1970-1974", "1975-1979", "1980-1984", "1985-1989",
    "1990-1994", "1995-1999", "2000-2004", "2005-2009", "2010-2014",
    "2015-2019", "2020-2026"
]

ALPHA = list("abcdefghijklmnopqrstuvwxyz")

total = 0
seen_ids = set()

# Load existing IDs to avoid re-fetching
for row in conn.execute("SELECT spotify_id FROM tracks").fetchall():
    seen_ids.add(row[0])
total = len(seen_ids)
print(f"Resuming with {total} existing tracks")

def process_batch(track_ids, track_info):
    """Fetch audio features for a batch of track IDs and store."""
    global total
    new_ids = [tid for tid in track_ids if tid not in seen_ids]
    if not new_ids:
        return

    # Batch audio features - max 100 per request
    for i in range(0, len(new_ids), 100):
        chunk = new_ids[i:i+100]
        try:
            features = api("audio-features", {"ids": ",".join(chunk)})
            time.sleep(0.35)
        except Exception as e:
            print(f"\n  Audio features error: {e}")
            continue

        batch = []
        for feat in (features.get("audio_features") or []):
            if not feat:
                continue
            tid = feat["id"]
            if tid in seen_ids:
                continue
            seen_ids.add(tid)

            info = track_info.get(tid, {})
            artist = info.get("artist", "")
            title = info.get("title", "")
            year = info.get("year")

            key_int = feat.get("key", -1)
            mode_int = feat.get("mode", -1)
            tempo = feat.get("tempo", 0)

            key_name = None
            if key_int >= 0 and key_int <= 11:
                note = PITCH_CLASS[key_int]
                quality = "Major" if mode_int == 1 else "Minor"
                key_name = f"{note} {quality}"

            bpm = tempo if tempo > 0 else None

            if not artist or not title:
                continue
            if not bpm and not key_name:
                continue

            batch.append((tid, artist, title, bpm, key_name, year))

        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO tracks (spotify_id, artist, title, bpm, key_name, year) VALUES (?, ?, ?, ?, ?, ?)",
                batch
            )
            conn.commit()
            total += len(batch)

def search_tracks(query, limit=50):
    """Search Spotify and return track IDs + metadata."""
    try:
        data = api("search", {"q": query, "type": "track", "limit": limit, "market": "US"})
        time.sleep(0.35)
    except Exception as e:
        print(f"\n  Search error for '{query}': {e}")
        return [], {}

    items = data.get("tracks", {}).get("items", [])
    ids = []
    info = {}
    for item in items:
        tid = item["id"]
        artists = ", ".join(a["name"] for a in item.get("artists", []))
        title = item.get("name", "")
        year = None
        release = item.get("album", {}).get("release_date", "")
        if release:
            try:
                year = int(release[:4])
            except ValueError:
                pass
        ids.append(tid)
        info[tid] = {"artist": artists, "title": title, "year": year}
    return ids, info

# Phase 1: Search by genre + year
print("Phase 1: Genre + Year searches")
for genre in GENRES:
    for yr in YEAR_RANGES:
        query = f"genre:{genre} year:{yr}"
        ids, info = search_tracks(query)
        if ids:
            process_batch(ids, info)
        sys.stdout.write(f"\r  {genre} {yr}: {total} tracks total")
        sys.stdout.flush()
    print()

# Phase 2: Search by letter + genre
print("\nPhase 2: Letter + Genre searches")
for letter in ALPHA:
    for genre in GENRES[:10]:  # top genres only
        query = f"{letter} genre:{genre}"
        ids, info = search_tracks(query)
        if ids:
            process_batch(ids, info)
    sys.stdout.write(f"\r  Letter '{letter}': {total} tracks total")
    sys.stdout.flush()
print()

# Phase 3: Popular playlists and featured tracks
print("\nPhase 3: Browse categories")
try:
    cats = api("browse/categories", {"limit": 50, "country": "US"})
    for cat in cats.get("categories", {}).get("items", []):
        cat_id = cat["id"]
        try:
            playlists = api(f"browse/categories/{cat_id}/playlists", {"limit": 10, "country": "US"})
            for pl in playlists.get("playlists", {}).get("items", []) or []:
                if not pl:
                    continue
                pl_id = pl["id"]
                try:
                    pl_tracks = api(f"playlists/{pl_id}/tracks", {"limit": 100, "market": "US"})
                    ids = []
                    info = {}
                    for item in pl_tracks.get("items", []):
                        track = item.get("track")
                        if not track or not track.get("id"):
                            continue
                        tid = track["id"]
                        artists = ", ".join(a["name"] for a in track.get("artists", []))
                        title = track.get("name", "")
                        year = None
                        release = track.get("album", {}).get("release_date", "")
                        if release:
                            try:
                                year = int(release[:4])
                            except ValueError:
                                pass
                        ids.append(tid)
                        info[tid] = {"artist": artists, "title": title, "year": year}
                    if ids:
                        process_batch(ids, info)
                    time.sleep(0.35)
                except Exception:
                    pass
            sys.stdout.write(f"\r  Category '{cat_id}': {total} tracks total")
            sys.stdout.flush()
        except Exception:
            pass
except Exception as e:
    print(f"  Browse error: {e}")

print()

conn.execute("CREATE INDEX IF NOT EXISTS idx_at ON tracks(artist COLLATE NOCASE, title COLLATE NOCASE)")
conn.commit()

final = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
with_both = conn.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchone()[0]
print(f"\nSpotify DB: {final} tracks ({with_both} with both key+bpm)")
conn.close()
