#!/usr/bin/env python3
"""
Generate markers.json from stations CSV and system coordinates JSONL
Maps station types to edastro pin icons and logs unmatched stations to stderr
Uses SQLite for fast indexed lookups
"""

import json
import csv
import sys
import sqlite3
import os
from pathlib import Path

# Type to pin mapping based on edastro.com documentation
TYPE_TO_PIN = {
    "Asteroid base": "asteroidbase",
    "Coriolis Starport": "coriolis",
    "Dodec Starport": "dodec",
    "Drake-Class Carrier": "carrier",
    "Ocellus Starport": "ocellus",
    "Orbis Starport": "orbis",
    "Outpost": "outpost",
    "Planetary Outpost": "planetarybase",
}

# Default pin color for unknown types
DEFAULT_PIN = "orange"


def build_systems_db(jsonl_path, db_path):
    """
    Build SQLite database from JSONL file with indexed system names
    """
    print(f"Building systems database from JSONL...", file=sys.stderr)
    
    # Create/overwrite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table with index on name
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS systems (
            name TEXT PRIMARY KEY COLLATE NOCASE,
            x REAL,
            y REAL,
            z REAL
        )
    ''')
    
    # Clear existing data
    cursor.execute('DELETE FROM systems')
    
    # Load and insert data
    count = 0
    try:
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if line.strip():
                    try:
                        system = json.loads(line)
                        name = system.get("name", "").strip()
                        coords = system.get("coords", {})
                        
                        if name and coords:
                            cursor.execute(
                                'INSERT OR REPLACE INTO systems (name, x, y, z) VALUES (?, ?, ?, ?)',
                                (name, coords.get("x"), coords.get("y"), coords.get("z"))
                            )
                            count += 1
                            
                            # Batch commit every 10k records
                            if count % 10000 == 0:
                                conn.commit()
                                print(f"  Processed {count} systems...", file=sys.stderr)
                    except json.JSONDecodeError as e:
                        print(f"Warning: Failed to parse JSONL line {line_num}: {e}", file=sys.stderr)
                        continue
    except FileNotFoundError:
        print(f"Error: JSONL file not found: {jsonl_path}", file=sys.stderr)
        sys.exit(1)
    
    conn.commit()
    conn.close()
    
    print(f"✓ Database created with {count} systems", file=sys.stderr)
    return count


def ensure_systems_db(jsonl_path, db_path):
    """
    Ensure systems database exists and is up-to-date.
    Rebuild if JSONL is newer than DB.
    Returns the connection object.
    """
    jsonl_mtime = os.path.getmtime(jsonl_path)
    
    if os.path.exists(db_path):
        db_mtime = os.path.getmtime(db_path)
        if db_mtime >= jsonl_mtime:
            # Database is up-to-date
            print(f"Using existing database", file=sys.stderr)
            return sqlite3.connect(db_path)
    
    # Need to build/rebuild database
    build_systems_db(jsonl_path, db_path)
    return sqlite3.connect(db_path)


def get_system_coords(cursor, system_name):
    """
    Query system coordinates from database.
    Returns dict with x, y, z or None if not found.
    """
    cursor.execute(
        'SELECT x, y, z FROM systems WHERE name = ?',
        (system_name,)
    )
    row = cursor.fetchone()
    if row:
        return {"x": row[0], "y": row[1], "z": row[2]}
    return None


def get_pin_color(station_type):
    """Get the pin color for a station type"""
    return TYPE_TO_PIN.get(station_type, DEFAULT_PIN)


def generate_markers(csv_path, db_path, output_path, markers_url=None):
    """
    Generate markers JSON from CSV stations and SQLite database
    """
    # Open database connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    markers = []
    unmatched = []
    matched_count = 0
    
    # Process CSV
    print("Processing stations from CSV...", file=sys.stderr)
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("Name", "").strip()
                system_name = row.get("System Name", "").strip()
                station_type = row.get("Type", "").strip()
                
                # Query database for system
                coords = get_system_coords(cursor, system_name)
                
                if coords:
                    pin = get_pin_color(station_type)
                    
                    marker = {
                        "pin": pin,
                        "text": f"{system_name}\n{name}\nType : {station_type}",
                        "x": coords.get("x"),
                        "y": coords.get("y"),
                        "z": coords.get("z")
                    }
                    markers.append(marker)
                    matched_count += 1
                else:
                    unmatched.append({
                        "name": name,
                        "system": system_name,
                        "type": station_type
                    })
    except FileNotFoundError:
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()
    
    # Log unmatched stations
    if unmatched:
        print(f"\n⚠ {len(unmatched)} unmatched stations:", file=sys.stderr)
        for station in unmatched[:50]:  # Show first 50
            print(f"  - '{station['name']}' in system '{station['system']}' ({station['type']})", 
                  file=sys.stderr)
        if len(unmatched) > 50:
            print(f"  ... and {len(unmatched) - 50} more", file=sys.stderr)
    
    # Write markers JSON
    output = {"markers": markers}
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    # Generate edastro.com URL if markers_url provided
    edastro_url = None
    if markers_url:
        from urllib.parse import quote
        edastro_url = f"https://edastro.com/galmap/?custom={quote(markers_url, safe=':/?=&')}"
    
    # Print summary
    total = matched_count + len(unmatched)
    print(f"\n✓ Markers generated successfully!", file=sys.stderr)
    print(f"  Total stations: {total}", file=sys.stderr)
    print(f"  Matched: {matched_count}", file=sys.stderr)
    print(f"  Unmatched: {len(unmatched)}", file=sys.stderr)
    print(f"  Output: {output_path}", file=sys.stderr)
    
    if edastro_url:
        print(f"\n🗺 Galmap URL:", file=sys.stderr)
        print(f"  {edastro_url}", file=sys.stderr)


if __name__ == "__main__":
    # Paths
    workspace_dir = Path(__file__).parent
    csv_file = workspace_dir / "stations-search-33043E22-E969-11F0-BC90-D8AF259F7FA5-1.csv"
    jsonl_file = workspace_dir / "systemsWithCoordinates.jsonl"
    db_file = workspace_dir / "systems.db"
    output_file = workspace_dir / "markers.json"
    
    # URL to the markers.json file on GitHub
    markers_url = "https://raw.githubusercontent.com/JYF/edmap/refs/heads/main/markers.json"
    
    # Ensure database is up-to-date
    print("Checking systems database...", file=sys.stderr)
    ensure_systems_db(jsonl_file, db_file)
    
    # Generate markers
    generate_markers(csv_file, db_file, output_file, markers_url)
