#!/usr/bin/env python3
"""Attach official PyPI artifact hashes to the existing resolved versions; never upgrade a version."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
from urllib.request import urlopen

path = Path(__file__).resolve().parents[1] / "requirements.txt"
versions = re.findall(r"^([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+-]+)", path.read_text(), re.M)

def hashes(requirement):
    name, version = requirement
    with urlopen(f"https://pypi.org/pypi/{name}/{version}/json", timeout=20) as response:
        metadata = json.load(response)
    values = sorted({artifact["digests"]["sha256"] for artifact in metadata["urls"] if not artifact.get("yanked")})
    if not values or any(not re.fullmatch(r"[a-f0-9]{64}", value) for value in values):
        raise RuntimeError(f"No valid released artifact hashes for {name}")
    return name + "==" + version + " \\\n" + " \\\n".join("    --hash=sha256:" + value for value in values)

with ThreadPoolExecutor(max_workers=4) as pool:
    lines = list(pool.map(hashes, versions))
path.write_text("# Resolved gateway dependencies for CPython 3.11; versions are reviewed in requirements.in.\n# Official PyPI artifact hashes cover published ARM64 and development-platform artifacts.\n# Regenerate hashes without upgrading versions: python raspberry-pi/scripts/lock-gateway-dependencies.py\n" + "\n".join(lines) + "\n")
print(f"Locked {len(lines)} existing dependency versions to official artifact hashes")
