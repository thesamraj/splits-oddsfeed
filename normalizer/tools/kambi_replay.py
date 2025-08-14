#!/usr/bin/env python3
"""Kambi replay tool for testing normalizer with captured artifacts."""

import sys
import json
import redis
import argparse
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="Replay Kambi artifacts")
    parser.add_argument("file_path", help="Path to Kambi artifact JSON file")
    parser.add_argument("--direct", action="store_true", help="Direct insertion (not implemented)")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis URL")

    args = parser.parse_args()

    if not args.file_path:
        print("ERROR: file_path required", file=sys.stderr)
        sys.exit(1)

    try:
        with open(args.file_path, "r") as f:
            payload = json.load(f)
    except Exception as e:
        print(f"ERROR: Failed to read {args.file_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # Create envelope
    envelope = {
        "source": "kambi",
        "url": payload.get("__url", ""),
        "status": 200,
        "ts": datetime.utcnow().isoformat() + "Z",
        "data": payload,
    }

    # Publish to Redis
    try:
        r = redis.Redis.from_url(args.redis_url)
        r.publish("odds.raw.kambi", json.dumps(envelope))
        print(f"Published {args.file_path}")
    except Exception as e:
        print(f"ERROR: Redis publish failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
