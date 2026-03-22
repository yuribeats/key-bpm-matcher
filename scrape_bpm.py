import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import sys

BASE = "https://cs.uwaterloo.ca/~dtompkin/music/bpm"
DB = "bpm.db"

conn = sqlite3.connect(DB)
c = conn.cursor()
c.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        title TEXT,
        duration TEXT,
        bpm REAL,
        year INTEGER,
        genre TEXT
    )
""")
c.execute("DELETE FROM tracks")
conn.commit()

total = 0

for bpm_page in range(80, 204):
    url = f"{BASE}/{bpm_page}.html"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            print(f"  SKIP {bpm_page} (status {r.status_code})")
            continue
    except Exception as e:
        print(f"  ERROR {bpm_page}: {e}")
        continue

    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.find_all("tr")

    count = 0
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 7:
            continue

        text = [cell.get_text(strip=True) for cell in cells]

        # Table structure: play button, artist, title, time, bpm, year, genre, disc-track, details
        # First cell is often a play button (empty or icon)
        # Try to find the BPM value to orient ourselves
        bpm_val = None
        bpm_idx = None
        for i, t in enumerate(text):
            try:
                val = float(t)
                if 70 <= val <= 210:
                    bpm_val = val
                    bpm_idx = i
                    break
            except ValueError:
                continue

        if bpm_val is None:
            continue

        # BPM is typically at index 4 (0-indexed), with artist at 1, title at 2, time at 3
        # But let's use the bpm_idx to figure out the layout
        artist_idx = bpm_idx - 3
        title_idx = bpm_idx - 2
        time_idx = bpm_idx - 1
        year_idx = bpm_idx + 1
        genre_idx = bpm_idx + 2

        if artist_idx < 0 or genre_idx >= len(text):
            continue

        artist = text[artist_idx]
        title = text[title_idx]
        duration = text[time_idx]
        genre = text[genre_idx]

        year_text = text[year_idx]
        try:
            year = int(year_text)
        except ValueError:
            year = None

        if not artist or not title:
            continue

        c.execute(
            "INSERT INTO tracks (artist, title, duration, bpm, year, genre) VALUES (?, ?, ?, ?, ?, ?)",
            (artist, title, duration, bpm_val, year, genre),
        )
        count += 1

    total += count
    print(f"  {bpm_page} BPM: {count} tracks")
    conn.commit()
    time.sleep(0.3)

conn.close()
print(f"\nDONE: {total} tracks in {DB}")
