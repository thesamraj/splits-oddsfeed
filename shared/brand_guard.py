import json
import pathlib

ALLOW = {"betrivers"}
p = pathlib.Path("config/brands.allowlist.json")
if p.exists():
    try:
        ALLOW = set(json.loads(p.read_text()).get("brands", ["betrivers"]))
    except Exception:
        ALLOW = {"betrivers"}


def allowed(brand: str) -> bool:
    return (brand or "").lower() in ALLOW
