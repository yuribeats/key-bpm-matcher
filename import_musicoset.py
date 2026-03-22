import csv
import sqlite3

PITCH_CLASS = {0:'C',1:'Db',2:'D',3:'Eb',4:'E',5:'F',6:'Gb',7:'G',8:'Ab',9:'A',10:'Bb',11:'B'}

# Load acoustic features by song_id
features = {}
with open('/tmp/musicoset_features/musicoset_songfeatures/acoustic_features.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        sid = row.get('song_id', '').strip()
        if not sid:
            continue
        key_int = row.get('key', '')
        mode_int = row.get('mode', '')
        tempo = row.get('tempo', '')

        key_name = None
        try:
            ki = int(key_int)
            mi = int(mode_int)
            if 0 <= ki <= 11:
                note = PITCH_CLASS[ki]
                quality = 'Major' if mi == 1 else 'Minor'
                key_name = f"{note} {quality}"
        except (ValueError, KeyError):
            pass

        bpm = None
        try:
            bpm = float(tempo)
            if bpm <= 0:
                bpm = None
        except ValueError:
            pass

        features[sid] = (key_name, bpm)

# Load song metadata
conn = sqlite3.connect('musicoset.db')
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

count = 0
with open('/tmp/musicoset_meta/musicoset_metadata/songs.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        sid = row.get('song_id', '').strip()
        title = row.get('song_name', '').strip()
        artists_raw = row.get('artists', '').strip()

        # Parse artists dict string like "{'id': 'Name'}"
        # Extract just the name values
        import re
        names = re.findall(r"':\s*'([^']+)'", artists_raw)
        artist = ', '.join(names) if names else ''

        if not artist or not title or sid not in features:
            continue

        key_name, bpm = features[sid]
        if not key_name and not bpm:
            continue

        conn.execute(
            "INSERT INTO tracks (artist, title, bpm, key_name) VALUES (?, ?, ?, ?)",
            (artist, title, bpm, key_name)
        )
        count += 1

conn.execute("CREATE INDEX IF NOT EXISTS idx_at ON tracks(artist COLLATE NOCASE, title COLLATE NOCASE)")
conn.commit()

total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
with_both = conn.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL AND key_name IS NOT NULL").fetchone()[0]
print(f"MusicOSet DB: {total} tracks ({with_both} with both key+bpm)")
conn.close()
