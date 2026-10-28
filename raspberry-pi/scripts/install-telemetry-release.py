"""Install a checksummed gateway bundle alongside the active release.

Run as root on the commissioned Pi. The bundle contains src/, tests/,
examples/, requirements.txt, wheels/ and SHA256SUMS. Existing credentials,
broker settings, firmware, queues and the original virtualenv are untouched.
"""
import hashlib
import grp
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def main():
    if os.geteuid() != 0:
        raise SystemExit("Run with sudo python3 " + str(Path(__file__).resolve()))
    bundle = Path(__file__).resolve().parent
    for line in (bundle / "SHA256SUMS").read_text().splitlines():
        expected, name = line.split("  ", 1)
        path = bundle / name
        if not path.resolve().is_relative_to(bundle) or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise SystemExit("Bundle checksum mismatch: " + name)
    app = Path("/opt/smart-factory-edge/raspberry-pi")
    dropin = Path("/etc/systemd/system/smart-factory-edge.service.d/40-telemetry.conf")
    policy_env = Path("/etc/smart-factory-iot/telemetry.env")
    if dropin.exists() or policy_env.exists():
        raise SystemExit("Telemetry override already exists; review it before installing another release")
    release = app / "releases" / ("telemetry-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    release.mkdir(parents=True, mode=0o750)
    shutil.chown(release.parent, user="root", group="smartfactory")
    release.parent.chmod(0o750)
    shutil.chown(release, user="root", group="smartfactory")
    for name in ("src", "tests", "examples"):
        shutil.copytree(bundle / name, release / name)
    shutil.copy2(bundle / "requirements.txt", release / "requirements.txt")
    shutil.copy2(bundle / "SHA256SUMS", release / "SHA256SUMS")
    # Copy first, then upgrade only the independent copy. Its python -m pip
    # uses the new prefix even though copied console-script shebangs refer to
    # the old virtualenv; runtime always invokes the explicit Python path.
    shutil.copytree(app / ".venv", release / ".venv", symlinks=True)
    python = str(release / ".venv/bin/python")
    run(python, "-c", "import sys,pathlib; assert pathlib.Path(sys.prefix).resolve()==pathlib.Path(sys.argv[1]).resolve(), 'Copied virtualenv prefix mismatch'", str(release / ".venv"))
    run(python, "-m", "pip", "install", "--disable-pip-version-check", "--no-index", "--find-links", str(bundle / "wheels"), "-r", str(release / "requirements.txt"))
    run(python, "-m", "pip", "check")
    run(python, "-c", "import importlib.metadata as m,pathlib; "
        "pins=[line.split('==') for line in pathlib.Path('requirements.txt').read_text().splitlines() if line and not line.startswith('#')]; "
        "assert all(m.version(name)==version for name,version in pins), 'Installed dependency versions differ from audited requirements'", cwd=release)
    service_gid = grp.getgrnam("smartfactory").gr_gid
    for directory, dirs, files in os.walk(release):
        for path in [Path(directory), *(Path(directory) / name for name in dirs + files)]:
            os.chown(path, 0, service_gid, follow_symlinks=False)
            if not path.is_symlink():
                path.chmod(0o750 if path.is_dir() or path.stat().st_mode & 0o111 else 0o640)
    run("runuser", "-u", "smartfactory", "--", python, "-m", "unittest", "discover", "-s", str(release / "tests"), "-v", env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    rollback = release / "rollback.sh"
    rollback.write_text("#!/bin/sh\nset -eu\nrm -f '" + str(dropin) + "' '" + str(policy_env) + "'\nsystemctl daemon-reload\nsystemctl restart smart-factory-edge\nsystemctl is-active smart-factory-edge\n")
    rollback.chmod(0o700)
    dropin.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    try:
        policy_env.write_text("TELEMETRY_COALESCING_ENABLED=true\nTELEMETRY_SNAPSHOT_SECONDS=60\nHEARTBEAT_INTERVAL_SECONDS=60\n")
        shutil.chown(policy_env, user="root", group="smartfactory")
        policy_env.chmod(0o640)
        dropin.write_text("[Service]\nEnvironmentFile=" + str(policy_env) + "\nExecStart=\nExecStart=" + python + " " + str(release / "src/sensor_gateway.py") + "\n")
        dropin.chmod(0o644)
        run("systemctl", "daemon-reload")
        run("systemctl", "restart", "smart-factory-edge")
        time.sleep(5)
        run("systemctl", "is-active", "smart-factory-edge")
    except BaseException:
        print("Cutover failed; restoring the previous service configuration", flush=True)
        run("/bin/sh", str(rollback))
        raise
    print("Installed release: " + str(release), flush=True)
    print("Rollback: sudo /bin/sh " + str(rollback), flush=True)
    print("Broker settings, TLS credentials, hardware firmware and delivery queue were preserved.", flush=True)


if __name__ == "__main__":
    main()
