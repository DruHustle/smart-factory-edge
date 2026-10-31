"""Release qualification in temporary directories; systemd is always simulated."""
import base64
from contextlib import ExitStack, redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
import zipfile


PI = Path(__file__).resolve().parents[1]
REPO = PI.parent
SCRIPT = PI / "scripts/install-telemetry-release.py"
spec = importlib.util.spec_from_file_location("gateway_release_installer", SCRIPT)
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


def manifest(bundle):
    entries = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(bundle).as_posix()}"
               for path in sorted(bundle.rglob("*")) if path.is_file() and path.name != "SHA256SUMS"]
    (bundle / "SHA256SUMS").write_text("\n".join(entries) + "\n")


def service_output(python, script):
    return f"{{ path={python} ; argv[]={python} {script} ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }}\n"


def wheel_fixture(path):
    """A real, local-only wheel exercises pip's hash enforcement offline."""
    files = {
        "installer_probe/__init__.py": b"VERSION = '1.2.3'\n",
        "installer_probe-1.2.3.dist-info/METADATA": b"Metadata-Version: 2.1\nName: installer-probe\nVersion: 1.2.3\n",
        "installer_probe-1.2.3.dist-info/WHEEL": b"Wheel-Version: 1.0\nGenerator: isolated-installer-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    rows = [f"{name},sha256={base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip('=')},{len(data)}" for name, data in files.items()]
    rows.append("installer_probe-1.2.3.dist-info/RECORD,,")
    files["installer_probe-1.2.3.dist-info/RECORD"] = ("\n".join(rows) + "\n").encode()
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return hashlib.sha256(path.read_bytes()).hexdigest()


class RequirementTests(unittest.TestCase):
    def test_current_hashed_lock_and_continuations(self):
        pins = installer.requirement_pins((PI / "requirements.txt").read_text())
        self.assertEqual(pins["asyncua"], "2.0.1")
        self.assertEqual(pins["pymodbus"], "3.15.0")
        self.assertEqual(pins["paho-mqtt"], "2.1.0")
        self.assertGreater(len(pins), 5)

    def test_lock_rejects_unhashed_unpinned_duplicate_or_incomplete_entries(self):
        valid = "demo_driver==1.2.3 --hash=sha256:" + "a" * 64
        cases = ("", "demo==1.2.3", "demo>=1 --hash=sha256:" + "a" * 64,
                 valid + "\nDemo-Driver==1.2.3 --hash=sha256:" + "b" * 64,
                 valid + " \\\n", valid + " --index-url https://example.invalid", "--hash=sha256:" + "a" * 64)
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValueError):
                installer.requirement_pins(value)

    def test_exact_installed_version_check_uses_distribution_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            info = Path(directory) / "installer_probe-1.2.3.dist-info"
            info.mkdir()
            (info / "METADATA").write_text("Name: installer-probe\nVersion: 1.2.3\n")
            env = {**os.environ, "PYTHONPATH": directory, "PYTHONDONTWRITEBYTECODE": "1"}
            for version, succeeds in (("1.2.3", True), ("9.9.9", False)):
                result = subprocess.run([sys.executable, "-c", installer.VERSION_CHECK, json.dumps({"installer-probe": version})], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, succeeds)
                if not succeeds:
                    self.assertIn("differ from audited requirements", result.stderr)

    def test_effective_exec_start_requires_explicit_python_and_script(self):
        python = Path("/opt/legacy-iot-factory/raspberry-pi/.venv/bin/python3.11")
        script = Path("/opt/legacy-iot-factory/raspberry-pi/src/sensor_gateway.py")
        self.assertEqual(installer.command_paths(service_output(python, script)), (python, script))
        bad = ("", service_output("/usr/bin/env", script), service_output(python, str(script) + " --flag"),
               service_output(python, script) * 2, service_output("/opt/a\\x20b/bin/python", script))
        for value in bad:
            with self.subTest(value=value), self.assertRaises(ValueError):
                installer.command_paths(value)


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bundle = self.root / "bundle"
        self.app = self.root / "opt/smart-factory-edge/raspberry-pi"
        self.dropin = self.root / "etc/systemd/system/smart-factory-edge.service.d/60-gateway-release.conf"
        self.dropin.parent.mkdir(parents=True)
        self.legacy = self.root / "opt/legacy-iot-factory/releases/active"
        self.source_venv = self.legacy / ".venv"
        (self.source_venv / "bin").mkdir(parents=True)
        (self.source_venv / "bin/python").write_text("placeholder")
        (self.source_venv / "pyvenv.cfg").write_text("home = /usr/bin\n")
        (self.source_venv / "active-environment-marker").write_text("keep active environment")
        (self.legacy / "src").mkdir()
        (self.legacy / "src/sensor_gateway.py").write_text("# existing service\n")
        self.original_command = (self.source_venv / "bin/python", self.legacy / "src/sensor_gateway.py")
        self.current_base = self.original_command
        self.protected = {
            self.dropin.parent / "40-telemetry.conf": b"[Service]\nEnvironmentFile=/etc/smart-factory-iot/telemetry.env\n",
            self.root / "etc/smart-factory-iot/telemetry.env": b"TELEMETRY_SNAPSHOT_SECONDS=45\n",
            self.root / "etc/smart-factory-iot/edge.env": b"DEVICE_ID=commissioned-gateway\n",
            self.root / "etc/smart-factory-iot/connectivity-secrets.json": b'{"credentials":{},"collectors":{}}',
            self.root / "var/lib/smart-factory-iot/connectivity.json": b'{"revision":7}',
            self.root / "var/lib/smart-factory-iot/delivery.sqlite3": b"existing queued records",
        }
        for path, content in self.protected.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            path.chmod(0o600 if path.suffix == ".json" else 0o640)
        self.protected_states = {path: installer.snapshot_file(path) for path in self.protected}
        for name in installer.COPY_DIRS:
            (self.bundle / "raspberry-pi" / name).mkdir(parents=True)
        for relative in ("fixtures/connectivity/industrial-v1.json", "fixtures/telemetry/provenance-v1.json"):
            target = self.bundle / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO / relative, target)
        shutil.copy2(SCRIPT, self.bundle / "install-telemetry-release.py")
        shutil.copy2(SCRIPT, self.bundle / "raspberry-pi/scripts/install-telemetry-release.py")
        (self.bundle / "raspberry-pi/src/sensor_gateway.py").write_text("# new release\n")
        (self.bundle / "raspberry-pi/examples/example.json").write_text("{}")
        (self.bundle / "SOURCE_REVISION").write_text("a" * 40 + "\n")
        (self.bundle / "wheels").mkdir()
        digest = wheel_fixture(self.bundle / "wheels/installer_probe-1.2.3-py3-none-any.whl")
        (self.bundle / "raspberry-pi/requirements.txt").write_text("installer-probe==1.2.3 \\\n    --hash=sha256:" + digest + "\n")
        (self.bundle / "raspberry-pi/tests/test_staged_fixture.py").write_text('''import json
from pathlib import Path
import unittest
class StagedFixtureTest(unittest.TestCase):
    def test_shared_repository_fixtures(self):
        root = Path(__file__).resolve().parents[2]
        profile = json.loads((root / "fixtures/connectivity/industrial-v1.json").read_text())
        self.assertEqual(profile["configuration"]["schemaVersion"], "industrial-connectivity/v1")
        self.assertTrue(json.loads((root / "fixtures/telemetry/provenance-v1.json").read_text())["cases"])
''')
        manifest(self.bundle)
        self.commands = []
        self.real_python = False
        self.fail_tests = False
        self.fail_health_once = False
        self.shadow_override = False
        self.change_during_tests = False
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(installer, "run", side_effect=self.run_command))
        self.stack.enter_context(patch.object(installer.grp, "getgrnam", return_value=types.SimpleNamespace(gr_gid=os.getgid())))
        self.stack.enter_context(patch.object(installer.os, "chown"))
        self.fchown = self.stack.enter_context(patch.object(installer.os, "fchown"))
        self.stack.enter_context(patch.object(installer.time, "sleep"))
        self.stack.enter_context(redirect_stdout(io.StringIO()))

    def run_command(self, *args, **kwargs):
        self.commands.append(args)
        if args[:2] == ("systemctl", "show"):
            command = self.current_base
            if self.dropin.exists() and not self.shadow_override:
                line = [line for line in self.dropin.read_text().splitlines() if line.startswith("ExecStart=") and line != "ExecStart="][-1]
                command = tuple(map(Path, line.split("=", 1)[1].split()))
            return types.SimpleNamespace(stdout=service_output(*command))
        if args[:2] == ("systemctl", "is-active") and self.fail_health_once:
            self.fail_health_once = False
            raise subprocess.CalledProcessError(1, args)
        if args[0] == "systemctl":
            return types.SimpleNamespace(returncode=0)
        if args[0] == "runuser":
            self.assertEqual(kwargs["env"]["CONNECTIVITY_PROTOCOL_TESTS"], "0")
            if self.fail_tests:
                raise subprocess.CalledProcessError(1, args)
            command = list(args[4:])
            if not self.real_python:
                command[0] = sys.executable
            result = subprocess.run(command, check=True, capture_output=True, text=True, **kwargs)
            self.assertIn("Ran 1 test", result.stderr)
            if self.change_during_tests:
                self.current_base = (self.original_command[0], self.legacy / "different/src/sensor_gateway.py")
            return result
        if self.real_python:
            return subprocess.run(args, check=True, capture_output=True, text=True, **kwargs)
        return types.SimpleNamespace(returncode=0)

    def install(self):
        return installer.install(self.bundle, app=self.app, dropin=self.dropin)

    def assert_protected_unchanged(self):
        self.assertEqual({path: installer.snapshot_file(path) for path in self.protected}, self.protected_states)
        self.assertEqual((self.source_venv / "active-environment-marker").read_text(), "keep active environment")

    def restarts(self):
        return [args for args in self.commands if args[:2] == ("systemctl", "restart")]

    def test_first_release_keeps_repository_layout_and_legacy_active_environment(self):
        release = self.install()
        self.assertEqual((release / "raspberry-pi/.venv/active-environment-marker").read_text(), "keep active environment")
        self.assertTrue((release / "fixtures/connectivity/industrial-v1.json").is_file())
        self.assertTrue((release / "fixtures/telemetry/provenance-v1.json").is_file())
        self.assertEqual(stat.S_IMODE((release / "rollback-state.json").stat().st_mode), 0o600)
        self.assertEqual(len(self.restarts()), 1)
        self.assertTrue(any("--require-hashes" in args and "--no-index" in args for args in self.commands))
        self.assert_protected_unchanged()
        installer.restore_override(json.loads((release / "rollback-state.json").read_text()), restart=True)
        self.assertFalse(self.dropin.exists())
        self.assertEqual(len(self.restarts()), 2)
        self.assert_protected_unchanged()

    def test_upgrade_rollback_restores_previous_override_bytes_and_metadata(self):
        self.dropin.write_text(installer.MANAGED_HEADER + "[Service]\nExecStart=\nExecStart=" + " ".join(map(str, self.original_command)) + "\n")
        self.dropin.chmod(0o600)
        original = installer.snapshot_file(self.dropin)
        release = self.install()
        installer.restore_override(json.loads((release / "rollback-state.json").read_text()), restart=True)
        self.assertEqual(installer.snapshot_file(self.dropin), original)
        self.assertEqual(self.fchown.call_args.args[1:], (original["uid"], original["gid"]))
        self.assert_protected_unchanged()

    def test_failed_staged_tests_never_change_service_or_restart(self):
        self.fail_tests = True
        with self.assertRaises(subprocess.CalledProcessError):
            self.install()
        self.assertFalse(self.dropin.exists())
        self.assertFalse(self.restarts())
        self.assertNotIn(("systemctl", "daemon-reload"), self.commands)
        self.assert_protected_unchanged()

    def test_failed_health_restores_prior_service_and_restarts_it(self):
        self.fail_health_once = True
        with self.assertRaises(subprocess.CalledProcessError):
            self.install()
        self.assertFalse(self.dropin.exists())
        self.assertEqual(len(self.restarts()), 2)
        self.assertEqual(installer.active_command(installer.SERVICE), self.original_command)
        self.assert_protected_unchanged()

    def test_later_override_wins_aborts_before_any_restart(self):
        self.shadow_override = True
        with self.assertRaisesRegex(ValueError, "Another systemd override wins"):
            self.install()
        self.assertFalse(self.dropin.exists())
        self.assertFalse(self.restarts())
        self.assert_protected_unchanged()

    def test_service_change_during_qualification_aborts_before_cutover(self):
        self.change_during_tests = True
        with self.assertRaisesRegex(ValueError, "changed during qualification"):
            self.install()
        self.assertFalse(self.dropin.exists())
        self.assertFalse(self.restarts())

    def test_stale_rollback_does_not_overwrite_a_newer_operator_change(self):
        release = self.install()
        changed = self.dropin.read_bytes() + b"# subsequent reviewed change\n"
        self.dropin.write_bytes(changed)
        with self.assertRaisesRegex(ValueError, "changed after this release"):
            installer.restore_override(json.loads((release / "rollback-state.json").read_text()), restart=True)
        self.assertEqual(self.dropin.read_bytes(), changed)
        self.assertEqual(len(self.restarts()), 1)

    def test_manifest_rejects_unchecked_or_missing_shared_fixtures(self):
        extra = self.bundle / "fixtures/connectivity/unchecked.json"
        extra.write_text("{}")
        with self.assertRaisesRegex(ValueError, "missing from the checksum manifest"):
            self.install()
        extra.unlink()
        (self.bundle / "fixtures/connectivity/industrial-v1.json").unlink()
        manifest(self.bundle)
        with self.assertRaisesRegex(ValueError, "shared-fixture layout"):
            self.install()
        self.assertFalse(self.commands)

    def test_real_offline_hashed_wheel_install_and_staged_fixture_gate(self):
        shutil.rmtree(self.source_venv)
        subprocess.run([sys.executable, "-m", "venv", str(self.source_venv)], check=True, capture_output=True, text=True)
        (self.source_venv / "active-environment-marker").write_text("keep active environment")
        self.real_python = True
        release = self.install()
        python = release / "raspberry-pi/.venv/bin/python"
        result = subprocess.run([str(python), "-c", "import installer_probe; print(installer_probe.VERSION)"], check=True, capture_output=True, text=True)
        self.assertEqual(result.stdout.strip(), "1.2.3")
        self.assert_protected_unchanged()


if __name__ == "__main__":
    unittest.main()
