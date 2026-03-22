import sqlite3
import os

src = sqlite3.connect("combined.db")
dst = sqlite3.connect("indexed.db")

dst.execute("PRAGMA journal_mode=WAL")
dst.execute("PRAGMA synchronous=NORMAL")

dst.execute("""
    CREATE TABLE tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT, title TEXT, bpm REAL, key_name TEXT, hq INTEGER DEFAULT 0
    )
""")

print("Copying tracks with both bpm and key...")
rows = src.execute("SELECT artist, title, bpm, key_name, hq FROM tracks WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchall()
print(f"  {len(rows):,} tracks")

dst.executemany("INSERT INTO tracks (artist, title, bpm, key_name, hq) VALUES (?, ?, ?, ?, ?)", rows)
dst.commit()

hq = dst.execute("SELECT COUNT(*) FROM tracks WHERE hq = 1").fetchone()[0]
ab = dst.execute("SELECT COUNT(*) FROM tracks WHERE hq = 0").fetchone()[0]
print(f"  High-quality: {hq:,}")
print(f"  AcousticBrainz only: {ab:,}")

print("Creating indexes...")
dst.execute("CREATE INDEX idx_artist ON tracks(artist COLLATE NOCASE)")
dst.execute("CREATE INDEX idx_bpm ON tracks(bpm)")
dst.execute("CREATE INDEX idx_key_name ON tracks(key_name)")
dst.execute("CREATE INDEX idx_bpm_artist ON tracks(bpm, artist COLLATE NOCASE)")
dst.execute("CREATE INDEX idx_key_artist ON tracks(key_name, artist COLLATE NOCASE)")
dst.execute("CREATE INDEX idx_key_bpm ON tracks(key_name, bpm)")
dst.execute("CREATE INDEX idx_key_artist_bpm ON tracks(key_name, artist COLLATE NOCASE, bpm)")
dst.execute("CREATE INDEX idx_key_title_bpm ON tracks(key_name, title COLLATE NOCASE, bpm)")
dst.execute("CREATE INDEX idx_hq ON tracks(hq)")
dst.execute("CREATE INDEX idx_hq_key_artist_bpm ON tracks(hq, key_name, artist COLLATE NOCASE, bpm)")
dst.execute("CREATE INDEX idx_hq_key_bpm ON tracks(hq, key_name, bpm)")
dst.commit()

print("Creating FTS5 index...")
dst.execute("CREATE VIRTUAL TABLE tracks_fts USING fts5(artist, title, content=tracks, content_rowid=id)")
dst.execute("INSERT INTO tracks_fts(tracks_fts) VALUES('rebuild')")
dst.commit()

count = dst.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
print(f"Total: {count:,} tracks")

src.close()
dst.close()

size = os.path.getsize("indexed.db") / 1024 / 1024
print(f"Database size: {size:.1f} MB")
print("Done! Now run: turso db destroy key-bpm-matcher --yes && turso db create key-bpm-matcher --from-file indexed.db --wait")
