#!/usr/bin/env python3
"""
Generate markers.json from stations CSV and system coordinates JSONL
Maps station types to edastro pin icons and logs unmatched stations to stderr
"""

import json
import csv
import sys
from pathlib import Path
from collections import defaultdict

# Type to pin mapping based on edastro.com documentation
TYPE_TO_PIN = {
    "Asteroid base": "asteroidbase",
    "Coriolis Starport": "coriolis",
    "Dodec Starport": "dodec",  # fallback to orange if not supported
    "Drake-Class Carrier": "carrier",
    "Ocellus Starport": "ocellus",
    "Orbis Starport": "orbis",
    "Outpost": "outpost",
    "Planetary Outpost": "planetarybase",
}

# Default pin color for unknown types
DEFAULT_PIN = "orange"


def load_systems_from_jsonl(jsonl_path):
    """
    Load systems from JSONL file into a dictionary indexed by system name (lowercase)
    Returns: dict with lowercase system name as key, coordinates dict as value
    """
    systems = {}
    try:
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if line.strip():
                    try:
                        system = json.loads(line)
                        name = system.get("name", "").lower()
                        if name and "coords" in system:
                            systems[name] = system["coords"]
                    except json.JSONDecodeError as e:
                        print(f"Warning: Failed to parse JSONL line {line_num}: {e}", file=sys.stderr)
                        continue
    except FileNotFoundError:
        print(f"Error: JSONL file not found: {jsonl_path}", file=sys.stderr)
        sys.exit(1)
    
    return systems


def get_pin_color(station_type):
    """Get the pin color for a station type"""
    return TYPE_TO_PIN.get(station_type, DEFAULT_PIN)


def generate_markers(csv_path, jsonl_path, output_path):
    """
    Generate markers JSON from CSV stations and JSONL coordinates
    """
    # Load systems
    print("Loading systems from JSONL...", file=sys.stderr)
    systems = load_systems_from_jsonl(jsonl_path)
    print(f"Loaded {len(systems)} systems", file=sys.stderr)
    
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
                
                # Try to find system (case-insensitive)
                system_key = system_name.lower()
                
                if system_key in systems:
                    coords = systems[system_key]
                    pin = get_pin_color(station_type)
                    
                    marker = {
                        "pin": pin,
                        "text": name,
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
    
    # Log unmatched stations
    if unmatched:
        print(f"\n⚠ {len(unmatched)} unmatched stations:", file=sys.stderr)
        for station in unmatched:
            print(f"  - '{station['name']}' in system '{station['system']}' ({station['type']})", 
                  file=sys.stderr)
    
    # Write markers JSON
    output = {"markers": markers}
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    # Print summary
    total = matched_count + len(unmatched)
    print(f"\n✓ Markers generated successfully!", file=sys.stderr)
    print(f"  Total stations: {total}", file=sys.stderr)
    print(f"  Matched: {matched_count}", file=sys.stderr)
    print(f"  Unmatched: {len(unmatched)}", file=sys.stderr)
    print(f"  Output: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    # Paths
    workspace_dir = Path(__file__).parent
    csv_file = workspace_dir / "stations-search-33043E22-E969-11F0-BC90-D8AF259F7FA5-1.csv"
    jsonl_file = workspace_dir / "edastro_systems7days.jsonl"
    output_file = workspace_dir / "markers.json"
    
    generate_markers(csv_file, jsonl_file, output_file)
