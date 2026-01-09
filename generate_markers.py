#!/usr/bin/env python3
"""
Generate markers.json from stations CSV and system coordinates JSONL
Maps station types to edastro pin icons and logs unmatched stations to stderr
Uses DuckDB for ultra-fast indexed lookups with compression
"""

import json
import csv
import sys
import duckdb
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
    Build DuckDB database from JSONL file with indexed system names
    """
    print(f"Building systems database from JSONL...", file=sys.stderr)
 
    try:
        # Create DuckDB connection
        conn = duckdb.connect(db_path)
     
        # Read JSONL directly with DuckDB (very fast)
        print(f"  Reading {jsonl_path}...", file=sys.stderr)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS systems AS
            SELECT 
                LOWER(json.name) as name,
                json.coords.x as x,
                json.coords.y as y,
                json.coords.z as z
            FROM read_ndjson('{jsonl_path}') as json
            WHERE json.name IS NOT NULL AND json.coords IS NOT NULL
        """)
     
        # Create index on name for fast lookups
        print(f"  Creating index on name...", file=sys.stderr)
        conn.execute('CREATE INDEX IF NOT EXISTS idx_name ON systems(name)')
     
        # Get count
        count = conn.execute('SELECT COUNT(*) as cnt FROM systems').fetchall()[0][0]
        conn.close()
     
        print(f"✓ Database created with {count} systems", file=sys.stderr)
        return count
     
    except FileNotFoundError:
        print(f"Error: JSONL file not found: {jsonl_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error building database: {e}", file=sys.stderr)
        sys.exit(1)


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
            return duckdb.connect(db_path)
 
    # Need to build/rebuild database
    build_systems_db(jsonl_path, db_path)
    return duckdb.connect(db_path)


def get_system_coords(conn, system_name):
    """
    Query system coordinates from database.
    Returns dict with x, y, z or None if not found.
    """
    result = conn.execute(
        'SELECT x, y, z FROM systems WHERE name = ?',
        [system_name.lower()]
    ).fetchall()
    
    if result:
        row = result[0]
        return {"x": row[0], "y": row[1], "z": row[2]}
    return None


def get_pin_color(station_type):
    """Get the pin color for a station type"""
    return TYPE_TO_PIN.get(station_type, DEFAULT_PIN)


def generate_markers(csv_path, db_path, output_path, markers_url=None):
    """
    Generate markers JSON from CSV stations and DuckDB database
    """
    # Open database connection
    conn = duckdb.connect(db_path)
    
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
                coords = get_system_coords(conn, system_name)
                
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
    db_file = workspace_dir / "systems.duckdb"
    output_file = workspace_dir / "markers.json"

    # URL to the markers.json file on GitHub
    markers_url = "https://raw.githubusercontent.com/JYF/edmap/refs/heads/main/markers.json"
 
    # Ensure database is up-to-date
    print("Checking systems database...", file=sys.stderr)
    ensure_systems_db(jsonl_file, db_file)
 
    # Generate markers
    generate_markers(csv_file, db_file, output_file, markers_url)
