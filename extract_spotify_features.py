"""
Extract tempo, key, mode from Anna's Archive Spotify audio features.
Run on a VPS with the torrent downloading.

Watches a directory for new .json/.jsonl/.zst files, extracts only
tempo/key/mode/track_id, writes to SQLite, deletes processed files.

Usage:
  python3 extract_spotify_features.py /mnt/volume/audio_features
"""

import sqlite3
import json
import os
import sys
import glob
import time
import subprocess

WATCH_DIR = sys.argv[1] if len(sys.argv) > 1 else "/mnt/volume/audio_features"
DB_PATH = "/mnt/volume/spotify_features.db"

# Spotify key mapping: pitch class integer -> note name
KEY_MAP = {0: "C", 1: "Db", 2: "D", 3: "Eb", 4: "E", 5: "F",
           6: "Gb", 7: "G", 8: "Ab", 9: "A", 10: "Bb", 11: "B"}
MODE_MAP = {1: "Major", 0: "Minor"}

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("""
    CREATE TABLE IF NOT EXISTS features (
        track_id TEXT PRIMARY KEY,
        tempo REAL,
        key_name TEXT,
        key_num INTEGER,
        mode INTEGER
    )
""")
conn.commit()

processed_count = conn.execute("SELECT COUNT(*) FROM features").fetchone()[0]
print(f"Already processed: {processed_count:,} tracks")


def extract_from_json(data):
    """Extract tempo/key/mode from a Spotify audio features/analysis JSON."""
    rows = []

    # Handle different possible formats
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # Single track or nested
        if "track" in data:
            items = [data]
        elif "audio_features" in data:
            items = data["audio_features"] if isinstance(data["audio_features"], list) else [data["audio_features"]]
        elif "tempo" in data:
            items = [data]
        else:
            # JSONL-style container - check for aacid format
            if "metadata" in data and "record" in data.get("metadata", {}):
                rec = data["metadata"]["record"]
                if isinstance(rec, dict) and "tempo" in rec:
                    items = [rec]
                else:
                    return rows
            else:
                return rows
    else:
        return rows

    for item in items:
        if not isinstance(item, dict):
            continue

        # Try to get track ID from various fields
        track_id = (item.get("id") or item.get("track_id") or
                    item.get("uri", "").split(":")[-1] if "uri" in item else None)

        tempo = item.get("tempo")
        key_num = item.get("key")
        mode = item.get("mode")

        if tempo is None or key_num is None or mode is None:
            # Try nested 'track' object
            track = item.get("track", {})
            if isinstance(track, dict):
                track_id = track_id or track.get("id")

            # Try 'audio_features' nested
            af = item.get("audio_features", item)
            if isinstance(af, dict):
                tempo = tempo or af.get("tempo")
                key_num = key_num if key_num is not None else af.get("key")
                mode = mode if mode is not None else af.get("mode")

        if tempo is not None and key_num is not None and mode is not None:
            try:
                tempo = float(tempo)
                key_num = int(key_num)
                mode = int(mode)
                if key_num < 0 or key_num > 11 or tempo <= 0:
                    continue
                note = KEY_MAP.get(key_num, "?")
                mode_str = MODE_MAP.get(mode, "?")
                key_name = f"{note} {mode_str}"
                rows.append((track_id or "", tempo, key_name, key_num, mode))
            except (ValueError, TypeError):
                continue

    return rows


def process_file(filepath):
    """Process a single file (JSON, JSONL, or .zst compressed)."""
    rows = []

    try:
        if filepath.endswith(".zst"):
            # Decompress and process
            proc = subprocess.run(["zstd", "-d", filepath, "--stdout"],
                                  capture_output=True, timeout=300)
            content = proc.stdout.decode("utf-8", errors="replace")
        else:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()

        # Try as single JSON first
        try:
            data = json.loads(content)
            rows = extract_from_json(data)
        except json.JSONDecodeError:
            # Try as JSONL (one JSON object per line)
            for line in content.split("\n"):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    rows.extend(extract_from_json(data))
                except json.JSONDecodeError:
                    continue

    except Exception as e:
        print(f"  Error processing {filepath}: {e}")

    return rows


def main():
    print(f"Watching: {WATCH_DIR}")
    print(f"Database: {DB_PATH}")
    print()

    processed_files = set()
    total_extracted = processed_count

    while True:
        # Find all processable files
        patterns = ["**/*.json", "**/*.jsonl", "**/*.zst", "**/*.jsonl.zst"]
        files = []
        for pat in patterns:
            files.extend(glob.glob(os.path.join(WATCH_DIR, pat), recursive=True))

        new_files = [f for f in files if f not in processed_files]

        if not new_files:
            print(f"\r  Waiting for files... ({total_extracted:,} tracks extracted)", end="", flush=True)
            time.sleep(10)
            continue

        for filepath in sorted(new_files):
            size_mb = os.path.getsize(filepath) / 1024 / 1024
            print(f"\n  Processing: {os.path.basename(filepath)} ({size_mb:.1f} MB)")

            rows = process_file(filepath)

            if rows:
                conn.executemany(
                    "INSERT OR IGNORE INTO features (track_id, tempo, key_name, key_num, mode) VALUES (?, ?, ?, ?, ?)",
                    rows
                )
                conn.commit()
                total_extracted += len(rows)
                print(f"    Extracted {len(rows):,} tracks (total: {total_extracted:,})")

            processed_files.add(filepath)

            # Delete processed file to free disk
            try:
                os.remove(filepath)
                print(f"    Deleted {os.path.basename(filepath)}")
            except OSError:
                pass

        # Print stats periodically
        count = conn.execute("SELECT COUNT(*) FROM features").fetchone()[0]
        db_size = os.path.getsize(DB_PATH) / 1024 / 1024
        print(f"\n  DB: {count:,} tracks, {db_size:.1f} MB")


if __name__ == "__main__":
    main()
