"""Qualify and activate a checksummed gateway release on a commissioned Pi.

The bundle retains raspberry-pi/{src,tests,examples,scripts,requirements.txt}
beside fixtures/, wheels/, SOURCE_REVISION, this installer and SHA256SUMS.
Only a managed ExecStart override changes at cutover. Existing environment
files, credentials, broker settings, firmware, queues and releases stay intact.
"""
import argparse
import base64
import grp
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone


SERVICE = "smart-factory-edge"
APP = Path("/opt/smart-factory-edge/raspberry-pi")
DROPIN = Path("/etc/systemd/system/smart-factory-edge.service.d/60-gateway-release.conf")
MANAGED_HEADER = "# Managed Smart Factory gateway release.\n"
COPY_DIRS = ("src", "tests", "examples", "scripts")
VERSION_CHECK = """import importlib.metadata as metadata, json, sys
pins = json.loads(sys.argv[1])
wrong = [name for name, version in pins.items() if metadata.version(name) != version]
if wrong:
    raise SystemExit('Installed dependency versions differ from audited requirements: ' + ', '.join(wrong))
"""


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def requirement_pins(text):
    """Read the supported fully pinned, SHA-256-hashed pip lock format."""
    pins = {}
    pending = ""
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        continued = line.endswith("\\")
        pending += (line[:-1] if continued else line) + " "
        if continued:
            continue
        tokens = pending.split()
        pending = ""
        pin = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9_.-]*)==([A-Za-z0-9][A-Za-z0-9_.+!-]*)", tokens[0])
        if not pin or len(tokens) < 2 or any(not re.fullmatch(r"--hash=sha256:[a-f0-9]{64}", token) for token in tokens[1:]):
            raise ValueError("Requirements must contain exact versions and SHA-256 hashes only")
        name, version = pin.groups()
        name = re.sub(r"[-_.]+", "-", name).lower()
        if name in pins:
            raise ValueError("Duplicate pinned dependency: " + name)
        pins[name] = version
    if pending or not pins:
        raise ValueError("Incomplete or empty hashed requirements")
    return pins


def verify_bundle(bundle):
    """Every copied input, including fixtures and wheels, must be checksummed."""
    bundle = bundle.resolve()
    listed = set()
    for line in (bundle / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([a-f0-9]{64})  ([A-Za-z0-9_./+-]+)", line)
        if not match:
            raise ValueError("Invalid bundle checksum manifest")
        expected, name = match.groups()
        path = bundle / name
        if Path(name).is_absolute() or ".." in Path(name).parts or name in listed or name == "SHA256SUMS":
            raise ValueError("Invalid or duplicate bundle path: " + name)
        if not path.resolve().is_relative_to(bundle) or path.is_symlink() or not path.is_file():
            raise ValueError("Invalid bundle file: " + name)
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError("Bundle checksum mismatch: " + name)
        listed.add(name)
    files = set()
    for path in bundle.rglob("*"):
        if path.is_symlink():
            raise ValueError("Bundle symlinks are not supported")
        if path.is_file() and path != bundle / "SHA256SUMS":
            files.add(path.relative_to(bundle).as_posix())
    if files != listed:
        raise ValueError("Bundle contains files missing from the checksum manifest")
    required = {
        "SOURCE_REVISION", "install-telemetry-release.py", "raspberry-pi/requirements.txt",
        "raspberry-pi/src/sensor_gateway.py", "raspberry-pi/scripts/install-telemetry-release.py",
        "fixtures/connectivity/industrial-v1.json", "fixtures/telemetry/provenance-v1.json",
    }
    if not required.issubset(listed) or any(not (bundle / "raspberry-pi" / name).is_dir() for name in COPY_DIRS):
        raise ValueError("Bundle must retain the documented repository and shared-fixture layout")
    if not any(name.startswith("wheels/") and name.endswith(".whl") for name in listed):
        raise ValueError("Bundle has no offline wheels")
    revision = (bundle / "SOURCE_REVISION").read_text(encoding="ascii").strip()
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("SOURCE_REVISION must be the full reviewed Git commit")
    if (bundle / "install-telemetry-release.py").read_bytes() != (bundle / "raspberry-pi/scripts/install-telemetry-release.py").read_bytes():
        raise ValueError("Bundle installer and staged installer differ")
    return revision, requirement_pins((bundle / "raspberry-pi/requirements.txt").read_text(encoding="utf-8"))


def command_paths(output):
    """Accept one explicit Python/script ExecStart; reject wrappers and escapes."""
    entry = re.fullmatch(r"\{\s*path=([^;{}]+)\s*;\s*argv\[\]=([^;{}]+)\s*;[^{}]*\}", output.strip())
    if not entry:
        raise ValueError("Inspect ExecStart: expected one explicit virtualenv Python and sensor_gateway.py")
    executable, argv = entry.groups()
    executable = executable.strip()
    args = argv.split()
    if len(args) != 2 or args[0] != executable:
        raise ValueError("Inspect ExecStart: wrappers or additional arguments need a reviewed manual cutover")
    if any(not re.fullmatch(r"/[A-Za-z0-9_./+-]+", arg) or ".." in Path(arg).parts for arg in args):
        raise ValueError("Inspect ExecStart: only explicit unescaped absolute paths are supported")
    python, script = map(Path, args)
    if python.parent.name != "bin" or not re.fullmatch(r"python(?:3(?:\.\d+)?)?", python.name) or script.name != "sensor_gateway.py":
        raise ValueError("Inspect ExecStart: expected a virtualenv Python and sensor_gateway.py")
    return python, script


def active_command(service):
    result = run("systemctl", "show", "--property=ExecStart", "--value", service, capture_output=True, text=True)
    return command_paths(result.stdout)


def snapshot_file(path):
    if path.is_symlink():
        raise ValueError("Managed override must not be a symlink")
    if not path.exists():
        return None
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("Managed override must be a regular file")
    return {
        "content": base64.b64encode(path.read_bytes()).decode("ascii"),
        "uid": info.st_uid, "gid": info.st_gid, "mode": stat.S_IMODE(info.st_mode),
    }


def atomic_write(path, contents, *, mode, uid=0, gid=0):
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + "-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as target:
            os.fchown(target.fileno(), uid, gid)
            os.fchmod(target.fileno(), mode)
            target.write(contents)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def restore_override(state, *, restart):
    dropin = Path(state["dropin"])
    if not dropin.is_file() or dropin.is_symlink() or hashlib.sha256(dropin.read_bytes()).hexdigest() != state["installedSha256"]:
        raise ValueError("Managed override changed after this release; review before rollback")
    previous = state["previousOverride"]
    if previous is None:
        dropin.unlink()
    else:
        atomic_write(dropin, base64.b64decode(previous["content"]), mode=previous["mode"], uid=previous["uid"], gid=previous["gid"])
    service = state["service"]
    run("systemctl", "daemon-reload")
    if active_command(service) != tuple(map(Path, state["previousCommand"])):
        raise ValueError("Previous effective ExecStart was not restored; inspect systemd before restarting")
    if restart:
        run("systemctl", "restart", service)
        run("systemctl", "is-active", service)


def service_directory(path, service_gid):
    if not path.exists():
        path.mkdir(parents=True, mode=0o750)
        os.chown(path, 0, service_gid)
        path.chmod(0o750)


def install(bundle, *, app=APP, dropin=DROPIN, service=SERVICE):
    bundle = bundle.resolve()
    revision, pins = verify_bundle(bundle)
    previous_command = active_command(service)
    source_venv = previous_command[0].parent.parent
    if not (source_venv / "pyvenv.cfg").is_file() or not previous_command[1].is_file():
        raise ValueError("Active ExecStart does not identify an existing virtualenv and gateway script")
    previous_override = snapshot_file(dropin)
    if previous_override is not None and not dropin.read_text(encoding="utf-8").startswith(MANAGED_HEADER):
        raise ValueError("60-gateway-release.conf already contains an unmanaged override; review it first")
    print("Current ExecStart: " + " ".join(map(str, previous_command)), flush=True)
    service_gid = grp.getgrnam("smartfactory").gr_gid
    service_directory(app, service_gid)
    service_directory(app / "releases", service_gid)
    prefix = "gateway-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + revision[:12] + "-"
    release = Path(tempfile.mkdtemp(prefix=prefix, dir=app / "releases"))
    pi = release / "raspberry-pi"
    pi.mkdir()
    for name in COPY_DIRS:
        shutil.copytree(bundle / "raspberry-pi" / name, pi / name)
    shutil.copytree(bundle / "fixtures", release / "fixtures")
    shutil.copy2(bundle / "raspberry-pi/requirements.txt", pi / "requirements.txt")
    for name in ("SOURCE_REVISION", "SHA256SUMS"):
        shutil.copy2(bundle / name, release / name)
    # Copy the actual active environment, including older installation paths.
    # Invoke its new Python explicitly: copied console-script shebangs may still
    # refer to the previous environment. Neither old source nor its venv changes.
    shutil.copytree(source_venv, pi / ".venv", symlinks=True)
    python = str(pi / ".venv/bin" / previous_command[0].name)
    run(python, "-c", "import sys,pathlib; assert pathlib.Path(sys.prefix).resolve()==pathlib.Path(sys.argv[1]).resolve(), 'Copied virtualenv prefix mismatch'", str(pi / ".venv"))
    run(python, "-m", "pip", "install", "--disable-pip-version-check", "--require-hashes", "--no-index", "--find-links", str(bundle / "wheels"), "-r", str(pi / "requirements.txt"))
    run(python, "-m", "pip", "check")
    run(python, "-c", VERSION_CHECK, json.dumps(pins, sort_keys=True))
    for directory, dirs, files in os.walk(release):
        for path in [Path(directory), *(Path(directory) / name for name in dirs + files)]:
            os.chown(path, 0, service_gid, follow_symlinks=False)
            if not path.is_symlink():
                path.chmod(0o750 if path.is_dir() or path.stat().st_mode & 0o111 else 0o640)
    test_env = {name: os.environ[name] for name in ("PATH", "LANG", "LC_ALL", "TZ") if name in os.environ}
    test_env.update(PYTHONDONTWRITEBYTECODE="1", CONNECTIVITY_PROTOCOL_TESTS="0")
    run("runuser", "-u", "smartfactory", "--", python, "-m", "unittest", "discover", "-s", str(pi / "tests"), "-v", cwd=pi, env=test_env)
    # Qualification must not overwrite a service change made while tests ran.
    if active_command(service) != previous_command or snapshot_file(dropin) != previous_override:
        raise ValueError("Active service changed during qualification; no cutover was attempted")
    contents = (MANAGED_HEADER + "[Service]\nExecStart=\nExecStart=" + python + " " + str(pi / "src/sensor_gateway.py") + "\n").encode("utf-8")
    state = {
        "service": service, "dropin": str(dropin), "previousOverride": previous_override,
        "previousCommand": list(map(str, previous_command)), "installedSha256": hashlib.sha256(contents).hexdigest(),
    }
    state_path = release / "rollback-state.json"
    atomic_write(state_path, json.dumps(state).encode("utf-8"), mode=0o600)
    rollback = release / "rollback.sh"
    rollback.write_text("#!/bin/sh\nset -eu\nexec " + shlex.quote(sys.executable) + " " + shlex.quote(str(pi / "scripts/install-telemetry-release.py")) + " --rollback " + shlex.quote(str(state_path)) + "\n", encoding="utf-8")
    rollback.chmod(0o700)
    dropin.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    restarted = False
    installed = False
    try:
        atomic_write(dropin, contents, mode=0o644)
        installed = True
        run("systemctl", "daemon-reload")
        if active_command(service) != (Path(python), pi / "src/sensor_gateway.py"):
            raise ValueError("Another systemd override wins; new release was not restarted")
        restarted = True
        run("systemctl", "restart", service)
        time.sleep(5)
        run("systemctl", "is-active", service)
    except BaseException:
        if installed:
            print("Cutover failed; restoring the previous service configuration", flush=True)
            restore_override(state, restart=restarted)
        raise
    print("Installed source revision: " + revision, flush=True)
    print("Installed release: " + str(release), flush=True)
    print("Rollback: sudo /bin/sh " + str(rollback), flush=True)
    print("Existing environment files, credentials, broker settings, firmware and delivery queue were preserved.", flush=True)
    return release


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rollback", type=Path, help="restore the prior managed override from this release's protected rollback state")
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise SystemExit("Run with sudo python3 " + str(Path(__file__).resolve()))
    if args.rollback:
        restore_override(json.loads(args.rollback.read_text(encoding="utf-8")), restart=True)
    else:
        install(Path(__file__).resolve().parent)


if __name__ == "__main__":
    main()
