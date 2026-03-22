import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import sys

BASE = "https://www.songkeyfinder.com/songs-in-key"
DB = "keys.db"

KEYS = [
    ("a-major", "A Major"),
    ("a-sharp,b-flat-major", "Bb Major"),
    ("b-major", "B Major"),
    ("c-major", "C Major"),
    ("c-sharp,d-flat-major", "Db Major"),
    ("d-major", "D Major"),
    ("d-sharp,e-flat-major", "Eb Major"),
    ("e-major", "E Major"),
    ("f-major", "F Major"),
    ("f-sharp,g-flat-major", "Gb Major"),
    ("g-major", "G Major"),
    ("g-sharp,a-flat-major", "Ab Major"),
]

conn = sqlite3.connect(DB)
c = conn.cursor()
c.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        title TEXT,
        key_name TEXT,
        popularity INTEGER
    )
""")
c.execute("DELETE FROM tracks")
conn.commit()

total = 0
headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

for slug, key_name in KEYS:
    page = 1
    key_count = 0
    while True:
        url = f"{BASE}/{slug}?page={page}"
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                break
        except Exception as e:
            print(f"  ERROR {key_name} p{page}: {e}")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.find_all("tr")

        batch = []
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            artist = cells[0].get_text(strip=True)
            title = cells[1].get_text(strip=True)
            pop_text = cells[2].get_text(strip=True)
            if not artist or not title or artist == "Artist":
                continue
            try:
                pop = int(pop_text)
            except ValueError:
                pop = 0
            batch.append((artist, title, key_name, pop))

        if not batch:
            break

        c.executemany(
            "INSERT INTO tracks (artist, title, key_name, popularity) VALUES (?, ?, ?, ?)",
            batch,
        )
        key_count += len(batch)
        conn.commit()

        sys.stdout.write(f"\r  {key_name}: {key_count} tracks (page {page})")
        sys.stdout.flush()

        page += 1
        time.sleep(0.3)

    total += key_count
    print(f"\r  {key_name}: {key_count} tracks (done)          ")

conn.close()
print(f"\nDONE: {total} tracks in {DB}")
