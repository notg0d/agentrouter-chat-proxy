r"""
service.py -- Headless background supervisor & auto-updater runner.

Designed to be launched by `pythonw.exe` (Windows GUI subsystem) at user logon
or via `.\start-proxy.ps1 -Background` so NO console window or Windows Terminal
popup is ever created. Child `git`, `pip`, and `uvicorn` processes on Windows
are spawned with `CREATE_NO_WINDOW`, and output is written to a rotating log
file (`logs/proxy.log`).
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import updater

ROOT = Path(__file__).resolve().parent
_CREATE_NO_WINDOW = 0x08000000 if sys.platform == "win32" else 0
_DETACHED_PROCESS = 0x00000008 if sys.platform == "win32" else 0
_CREATE_NEW_PROCESS_GROUP = 0x00000200 if sys.platform == "win32" else 0


class RotatingFileLogger:
    """Thread-safe size-bounded file logger with single-backup rotation."""

    def __init__(self, log_path: Path, max_kb: int = 1024) -> None:
        self.log_path = log_path
        self.max_bytes = max(64, max_kb) * 1024
        self._lock = threading.Lock()
        self._line_count = 0
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def _rotate_if_needed(self) -> None:
        try:
            if self.log_path.is_file() and self.log_path.stat().st_size > self.max_bytes:
                backup = Path(f"{self.log_path}.1")
                if backup.exists():
                    backup.unlink(missing_ok=True)
                self.log_path.replace(backup)
        except OSError:
            pass

    def write(self, level: str, message: str) -> None:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{stamp} [{level}] {message}\n"
        with self._lock:
            if self._line_count % 50 == 0:
                self._rotate_if_needed()
            self._line_count += 1
            try:
                with self.log_path.open("a", encoding="utf-8", errors="replace") as fh:
                    fh.write(line)
            except OSError:
                pass

    def write_raw(self, raw_line: str) -> None:
        clean = raw_line.rstrip("\r\n")
        if not clean:
            return
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            if self._line_count % 50 == 0:
                self._rotate_if_needed()
            self._line_count += 1
            try:
                with self.log_path.open("a", encoding="utf-8", errors="replace") as fh:
                    fh.write(f"{stamp} {clean}\n")
            except OSError:
                pass


def _resolve_worker_python() -> str:
    """Return python.exe (non-windowed via CREATE_NO_WINDOW) to host uvicorn."""
    if sys.platform == "win32":
        venv_py = ROOT / ".venv" / "Scripts" / "python.exe"
        if venv_py.is_file():
            return str(venv_py)
        sibling = Path(sys.executable).with_name("python.exe")
        if sibling.is_file():
            return str(sibling)
    else:
        venv_py = ROOT / ".venv" / "bin" / "python"
        if venv_py.is_file():
            return str(venv_py)
    return sys.executable


def _resolve_service_python() -> str:
    """Return pythonw.exe on Windows so self-restarts remain windowless."""
    if sys.platform == "win32":
        venv_pyw = ROOT / ".venv" / "Scripts" / "pythonw.exe"
        if venv_pyw.is_file():
            return str(venv_pyw)
        sibling = Path(sys.executable).with_name("pythonw.exe")
        if sibling.is_file():
            return str(sibling)
    return sys.executable


def _subprocess_kwargs(*, pipe_output: bool = False) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "cwd": str(ROOT),
        "stdin": subprocess.DEVNULL,
    }
    if pipe_output:
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.STDOUT
        kwargs["text"] = True
        kwargs["encoding"] = "utf-8"
        kwargs["errors"] = "replace"
        kwargs["bufsize"] = 1
    else:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    if _CREATE_NO_WINDOW:
        kwargs["creationflags"] = _CREATE_NO_WINDOW
    return kwargs


def _is_loopback_host(host: str) -> bool:
    cleaned = (host or "").strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    if cleaned.lower() in ("", "localhost", "127.0.0.1", "::1"):
        return True
    try:
        return ipaddress.ip_address(cleaned).is_loopback
    except ValueError:
        return False


def _probe_connect_host(bind_host: str) -> str:
    """Return a local connectable address for probing a bound listener."""
    cleaned = (bind_host or "").strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    if cleaned in ("", "0.0.0.0", "*"):
        return "127.0.0.1"
    if cleaned in ("::", "::0"):
        return "::1"
    return cleaned


def _format_url_host(host: str) -> str:
    return f"[{host}]" if ":" in host and not host.startswith("[") else host


def _probe_our_health(port: int, host: str = "127.0.0.1", timeout_s: float = 3.0) -> bool:
    connect_host = _probe_connect_host(host)
    url = f"http://{_format_url_host(connect_host)}:{port}/_health"
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
            return data.get("status") == "ok" and data.get("listen") == f"{host}:{port}"
    except (urllib.error.URLError, OSError, ValueError):
        return False


def _is_port_busy(port: int, host: str = "127.0.0.1") -> bool:
    connect_host = _probe_connect_host(host)
    family = socket.AF_INET6 if ":" in connect_host else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            return s.connect_ex((connect_host, port)) == 0
    except OSError:
        return False


def _ensure_dependencies(worker_python: str, logger: RotatingFileLogger) -> bool:
    importlib_check = subprocess.run(
        [
            worker_python,
            "-c",
            "import importlib.util, sys; "
            "sys.exit(3 if any(importlib.util.find_spec(m) is None for m in ('fastapi','uvicorn','httpx')) else 0)",
        ],
        **_subprocess_kwargs(pipe_output=True),
        timeout=30,
    )
    if importlib_check.returncode == 0:
        return True

    req_path = ROOT / "requirements.txt"
    if not req_path.is_file():
        logger.write("fail ", "missing dependencies (fastapi/uvicorn/httpx) and requirements.txt not found")
        return False

    logger.write("warn ", "missing dependencies (fastapi/uvicorn/httpx); installing from requirements.txt")
    cmd = [worker_python, "-m", "pip", "install", "--disable-pip-version-check", "-q", "-r", str(req_path)]
    proc = subprocess.run(cmd, **_subprocess_kwargs(pipe_output=True), timeout=180)
    if proc.returncode != 0:
        logger.write("fail ", f"pip install failed (exit {proc.returncode}): {(proc.stdout or '')[:300]}")
        return False
    logger.write("ok   ", "dependencies installed")
    return True


def _stop_child(proc: Optional[subprocess.Popen[Any]]) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
            proc.wait(timeout=3.0)
        except Exception:
            pass
    except Exception:
        pass


def _respawn_self(args: argparse.Namespace) -> None:
    exe = _resolve_service_python()
    cmd = [
        exe,
        str(ROOT / "service.py"),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--max-log-kb",
        str(args.max_log_kb),
        "--skip-start-update",
    ]
    if args.verbose:
        cmd.append("--verbose")
    if args.no_auto_update:
        cmd.append("--no-auto-update")

    if sys.platform == "win32":
        subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            creationflags=_DETACHED_PROCESS | _CREATE_NEW_PROCESS_GROUP | _CREATE_NO_WINDOW,
        )
    else:
        os.execv(exe, cmd)


def _write_pid_file(pid_path: Path) -> None:
    try:
        pid_path.parent.mkdir(parents=True, exist_ok=True)
        pid_path.write_text(f"{os.getpid()}\n", encoding="utf-8")
    except OSError:
        pass


def _remove_pid_file(pid_path: Path) -> None:
    try:
        if pid_path.is_file() and pid_path.read_text(encoding="utf-8").strip() == str(os.getpid()):
            pid_path.unlink(missing_ok=True)
    except OSError:
        pass


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Headless background supervisor for AgentRouter Codex proxy.")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind address (default: 127.0.0.1 loopback only; pass 0.0.0.0 explicitly for Docker/LAN).",
    )
    parser.add_argument("--port", type=int, default=int(os.environ.get("PROXY_PORT", "8788")))
    parser.add_argument("--max-log-kb", type=int, default=1024)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--no-auto-update", action="store_true")
    parser.add_argument("--skip-start-update", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    args.host = (args.host or "").strip() or "127.0.0.1"
    if not (1 <= args.port <= 65535):
        parser.error("--port must be between 1 and 65535")
    return args


def main(argv: Optional[list[str]] = None) -> int:
    updater.load_env_file(ROOT)
    args = _parse_args(argv)
    os.environ["PROXY_PORT"] = str(args.port)
    if args.verbose:
        os.environ["PROXY_VERBOSE"] = "1"
    os.environ.setdefault("PYTHONIOENCODING", "utf-8:replace")

    logger = RotatingFileLogger(ROOT / "logs" / "proxy.log", max_kb=args.max_log_kb)
    pid_path = ROOT / "logs" / "service.pid"

    if _is_port_busy(args.port, args.host):
        if _probe_our_health(args.port, args.host):
            logger.write("ok   ", f"proxy already healthy on {args.host}:{args.port} -- exiting cleanly")
            return 0
        logger.write(
            "fail ",
            f"port {args.port} is already occupied (not matching {args.host}:{args.port}) -- leaving it untouched",
        )
        return 2

    if not _is_loopback_host(args.host):
        logger.write(
            "warn ",
            f"non-loopback bind enabled ({args.host}:{args.port}) -- "
            "restrict access with a firewall and avoid exposing a shared fallback API key",
        )

    worker_python = _resolve_worker_python()
    if not _ensure_dependencies(worker_python, logger):
        return 1

    auto_update = (not args.no_auto_update) and updater.is_auto_update_enabled()
    if auto_update and updater.auto_update_on_start() and not args.skip_start_update:
        up_res = updater.apply_update(ROOT, python_exe=worker_python)
        status = up_res.get("status")
        if up_res.get("applied"):
            changed = ", ".join(up_res.get("changed_files", [])) or "files updated"
            logger.write(
                "ok   ",
                f"auto-update applied {(up_res.get('old_commit') or '')[:7]} -> "
                f"{(up_res.get('local_commit') or '')[:7]} ({changed})",
            )
            _respawn_self(args)
            return 0
        if status not in ("up_to_date", "not_git_repo", "no_remote"):
            logger.write("warn ", f"startup auto-update check: {status} ({up_res.get('error') or ''})")

    _write_pid_file(pid_path)

    stop_event = threading.Event()
    restart_for_update = threading.Event()
    child_lock = threading.Lock()
    active_child: dict[str, Optional[subprocess.Popen[Any]]] = {"proc": None}

    def handle_signal(_signum: int, _frame: Any) -> None:
        stop_event.set()
        with child_lock:
            _stop_child(active_child["proc"])

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                signal.signal(sig, handle_signal)
            except (ValueError, OSError):
                pass

    def update_loop() -> None:
        interval = updater.get_update_interval_s()
        logger.write("info ", f"background auto-updater enabled (interval={interval}s)")
        while not stop_event.wait(interval):
            try:
                res = updater.apply_update(ROOT, python_exe=worker_python)
            except Exception as exc:
                logger.write("warn ", f"auto-updater error: {type(exc).__name__}: {exc}")
                continue
            if res.get("applied"):
                changed = ", ".join(res.get("changed_files", [])) or "files updated"
                logger.write(
                    "ok   ",
                    f"auto-update applied {(res.get('old_commit') or '')[:7]} -> "
                    f"{(res.get('local_commit') or '')[:7]} ({changed}); restarting service",
                )
                restart_for_update.set()
                with child_lock:
                    _stop_child(active_child["proc"])
                return
            st = res.get("status")
            if st not in ("up_to_date", "not_git_repo", "no_remote"):
                logger.write("warn ", f"auto-updater status={st} ({res.get('error') or ''})")

    if auto_update:
        t = threading.Thread(target=update_loop, name="proxy-auto-updater", daemon=True)
        t.start()

    backoff = 5
    max_backoff = 60
    rapid_failures = 0

    try:
        while not stop_event.is_set() and not restart_for_update.is_set():
            started_at = time.monotonic()
            cmd = [
                worker_python,
                "-m",
                "uvicorn",
                "proxy:app",
                "--host",
                args.host,
                "--port",
                str(args.port),
                "--timeout-keep-alive",
                "600",
                "--log-level",
                "warning",
                "--no-access-log",
            ]
            logger.write("start", f"starting headless uvicorn on {args.host}:{args.port}")
            proc = subprocess.Popen(cmd, **_subprocess_kwargs(pipe_output=True))
            with child_lock:
                active_child["proc"] = proc

            try:
                assert proc.stdout is not None
                for line in proc.stdout:
                    logger.write_raw(line)
                    if stop_event.is_set() or restart_for_update.is_set():
                        break
            finally:
                with child_lock:
                    _stop_child(proc)
                    active_child["proc"] = None

            if stop_event.is_set() or restart_for_update.is_set():
                break

            code = proc.returncode if proc.returncode is not None else "unknown"
            ran_for = time.monotonic() - started_at
            logger.write("warn ", f"uvicorn exited with code {code} after {ran_for:.0f}s")

            if _is_port_busy(args.port, args.host):
                logger.write("fail ", f"port {args.port} occupied while restarting -- stopping")
                return 2

            if ran_for >= 60.0:
                backoff = 5
                rapid_failures = 0
            else:
                rapid_failures += 1

            if rapid_failures >= 10:
                logger.write("fail ", f"uvicorn failed {rapid_failures} times rapidly -- exiting")
                return 1

            logger.write("warn ", f"restarting in {backoff}s (rapid failures: {rapid_failures})")
            if stop_event.wait(backoff):
                break
            backoff = min(backoff * 2, max_backoff)

        if restart_for_update.is_set() and not stop_event.is_set():
            _remove_pid_file(pid_path)
            _respawn_self(args)
            return 0
        return 0
    finally:
        stop_event.set()
        with child_lock:
            _stop_child(active_child["proc"])
        _remove_pid_file(pid_path)


if __name__ == "__main__":
    raise SystemExit(main())
