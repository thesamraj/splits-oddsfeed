#!/usr/bin/env python3
"""
Analyze BoltOdds schema from captured JSONL files
"""
import json
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, Any

def analyze_files(file_patterns):
    """Analyze schema from JSONL files"""
    schema = defaultdict(lambda: {
        "count": 0,
        "fields": defaultdict(int),
        "example": None,
        "sports": set(),
        "books": set(),
        "markets": set()
    })
    
    total_frames = 0
    
    # Process files
    for pattern in file_patterns:
        for file_path in Path("data/bolt/raw").glob(pattern):
            print(f"Processing {file_path}...")
            with open(file_path) as f:
                for line in f:
                    try:
                        frame = json.loads(line)
                        # Remove capture metadata
                        frame.pop("_capture", None)
                        
                        # Get action type
                        action = frame.get("action", "unknown")
                        schema[action]["count"] += 1
                        total_frames += 1
                        
                        # Track fields
                        for key in frame.keys():
                            schema[action]["fields"][key] += 1
                        
                        # Track sports, books, markets
                        if "data" in frame and isinstance(frame["data"], dict):
                            sport = frame["data"].get("sport")
                            book = frame["data"].get("sportsbook")
                            if sport:
                                schema[action]["sports"].add(sport)
                            if book:
                                schema[action]["books"].add(book)
                            
                            # Track markets
                            if "outcomes" in frame["data"]:
                                for outcome_name in frame["data"]["outcomes"].keys():
                                    # Extract market type from outcome name
                                    if "Moneyline" in outcome_name:
                                        schema[action]["markets"].add("Moneyline")
                                    elif "Spread" in outcome_name:
                                        schema[action]["markets"].add("Spread")
                                    elif "Total" in outcome_name or "Over" in outcome_name or "Under" in outcome_name:
                                        schema[action]["markets"].add("Total")
                        
                        # Store example
                        if not schema[action]["example"] and action != "socket_connected":
                            schema[action]["example"] = frame
                    
                    except json.JSONDecodeError:
                        continue
    
    return schema, total_frames

def generate_schema_map(schema, total_frames):
    """Generate SCHEMA_MAP.md"""
    output = []
    output.append("# BoltOdds Schema Map (Analyzed)")
    output.append("")
    output.append(f"Total frames analyzed: {total_frames}")
    output.append("")
    
    # Sort by frequency
    sorted_actions = sorted(schema.items(), key=lambda x: -x[1]["count"])
    
    for action, info in sorted_actions:
        output.append(f"## Action: `{action}`")
        output.append("")
        output.append(f"**Count:** {info['count']} ({info['count']*100/max(total_frames,1):.1f}%)")
        output.append("")
        
        # Sports coverage
        if info["sports"]:
            output.append(f"**Sports:** {', '.join(sorted(info['sports']))}")
        
        # Books coverage  
        if info["books"]:
            output.append(f"**Books:** {', '.join(sorted(info['books']))}")
        
        # Markets
        if info["markets"]:
            output.append(f"**Markets:** {', '.join(sorted(info['markets']))}")
        
        output.append("")
        output.append("**Fields:**")
        
        # Sort fields by frequency
        for field, count in sorted(info["fields"].items(), key=lambda x: (-x[1], x[0])):
            req = "required" if count == info["count"] else f"optional ({count*100/info['count']:.1f}%)"
            output.append(f"- `{field}`: {req}")
        
        # Add example
        if info["example"]:
            output.append("")
            output.append("**Example:**")
            output.append("```json")
            # Truncate large examples
            example = info["example"]
            if "data" in example and isinstance(example["data"], dict):
                if "outcomes" in example["data"] and len(str(example["data"]["outcomes"])) > 500:
                    # Show just first outcome
                    outcomes = example["data"]["outcomes"]
                    first_key = list(outcomes.keys())[0] if outcomes else None
                    if first_key:
                        example["data"]["outcomes"] = {
                            first_key: outcomes[first_key],
                            "...": "truncated"
                        }
            output.append(json.dumps(example, indent=2))
            output.append("```")
        
        output.append("")
    
    return "\n".join(output)

if __name__ == "__main__":
    # Analyze latest capture
    patterns = ["frames_*_bare.jsonl", "frames_*_filtered.jsonl"]
    
    schema, total_frames = analyze_files(patterns)
    
    # Generate output
    content = generate_schema_map(schema, total_frames)
    
    # Save
    output_file = Path("docs/bolt_teardown/SCHEMA_MAP.md")
    output_file.write_text(content)
    
    print(f"\n✓ Schema map updated: {output_file}")
    print(f"  Total frames: {total_frames}")
    print(f"  Action types: {len(schema)}")
    
    # Summary
    for action, info in sorted(schema.items(), key=lambda x: -x[1]["count"])[:5]:
        print(f"  - {action}: {info['count']} frames")
        if info["sports"]:
            print(f"    Sports: {len(info['sports'])} unique")
        if info["books"]:
            print(f"    Books: {len(info['books'])} unique")