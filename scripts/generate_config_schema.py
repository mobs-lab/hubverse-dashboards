#!/usr/bin/env python3
"""
Generate JSON Schema from Pydantic Configuration Models
"""

import json
import sys
from pathlib import Path

try:
    from yaml_config_processor_pydantic import export_json_schema
except ImportError:
    print("Error: Could not import yaml_config_processor_pydantic.")
    print("Make sure pydantic is installed: pip install pydantic")
    sys.exit(1)


def main():
    """Generate and export the configuration JSON Schema"""
    output_dir = Path(__file__).parent.parent  # Project root
    output_file = output_dir / "config_schema.json"

    print("\n" + "=" * 80)
    print("HUBVERSE DASHBOARD - JSON SCHEMA GENERATOR")
    print("=" * 80 + "\n")

    # Export the schema
    export_json_schema(str(output_file))

    # Load and display some stats
    with open(output_file, "r") as f:
        schema = json.load(f)

    print("\n" + "=" * 80)
    print("SCHEMA STATISTICS")
    print("=" * 80)
    print(f"Schema Title: {schema.get('title', 'N/A')}")
    print(f"Properties: {len(schema.get('properties', {}))}")
    print(f"Required Fields: {len(schema.get('required', []))}")
    print(f"Schema Version: JSON Schema Draft {schema.get('$schema', 'N/A').split('/')[-1]}")
    print(f"\nOutput File: {output_file}")
    print("=" * 80 + "\n")

    # Display property summary
    print("Configuration Properties:")
    print("-" * 80)
    for prop_name, prop_data in sorted(schema.get("properties", {}).items()):
        prop_type = prop_data.get("type", prop_data.get("anyOf", [{}])[0].get("type", "complex"))
        description = prop_data.get("description", "")
        required = "*" if prop_name in schema.get("required", []) else " "
        print(f"  [{required}] {prop_name:40s} {prop_type:15s}")
        if description:
            print(f"      -> {description[:70]}...")
    print("=" * 80 + "\n")

    print("[OK] JSON Schema generated successfully!")
    print("\n")


if __name__ == "__main__":
    main()
