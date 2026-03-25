import csv
import sqlite3

PITCH_CLASS = {0:'C',1:'Db',2:'D',3:'Eb',4:'E',5:'F',6:'Gb',7:'G',8:'Ab',9:'A',10:'Bb',11:'B'}

conn = sqlite3.connect('kaggle.db')
conn.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        title TEXT,
        bpm REAL,
        key_name TEXT,
        genre TEXT
    )
""")
conn.execute("DELETE FROM tracks")

count = 0
with open('/tmp/kaggle/dataset.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    batch = []
    for row in reader:
        artist = (row.get('artists') or '').strip()
        title = (row.get('track_name') or '').strip()
        if not artist or not title:
            continue

        # Parse key
        key_int = row.get('key', '')
        mode_int = row.get('mode', '')
        key_name = None
        try:
            ki = int(key_int)
            mi = int(mode_int)
            if ki >= 0 and ki <= 11:
                note = PITCH_CLASS[ki]
                quality = 'Major' if mi == 1 else 'Minor'
                key_name = f"{note} {quality}"
        except (ValueError, KeyError):
            pass

        # Parse BPM
        bpm = None
        try:
            bpm = float(row.get('tempo', ''))
            if bpm <= 0:
                bpm = None
        except ValueError:
            pass

        genre = (row.get('track_genre') or '').strip()

        if not key_name and not bpm:
            continue

        batch.append((artist, title, bpm, key_name, genre))
        if len(batch) >= 5000:
            conn.executemany("INSERT INTO tracks (artist, title, bpm, key_name, genre) VALUES (?, ?, ?, ?, ?)", batch)
            conn.commit()
            count += len(batch)
            batch = []

    if batch:
        conn.executemany("INSERT INTO tracks (artist, title, bpm, key_name, genre) VALUES (?, ?, ?, ?, ?)", batch)
        conn.commit()
        count += len(batch)

conn.execute("CREATE INDEX IF NOT EXISTS idx_at ON tracks(artist COLLATE NOCASE, title COLLATE NOCASE)")
conn.commit()

total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
with_both = conn.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchone()[0]
print(f"Kaggle DB: {total} tracks ({with_both} with both key+bpm)")

# Dedupe count
unique = conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artist, title FROM tracks)").fetchone()[0]
print(f"Unique artist+title combos: {unique}")

conn.close()
