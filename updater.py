r"""
updater.py -- Safe, non-interactive fast-forward auto-updater.

Guards against data loss and broken deploys:
  * Checks only against the tracked upstream (or origin/<branch>) using --ff-only.
  * Refuses to overwrite uncommitted edits to tracked files (`dirty_worktree`).
  * Runs `py_compile` pre-flight on pulled Python files before signaling a
    restart, rolling back cleanly with `git reset --hard <old_sha>` if compilation
    fails.
  * Syncs dependencies only when `requirements.txt` changed.
  * Disables terminal credential prompts (`GIT_TERMINAL_PROMPT=0`) and hides
    console windows on Windows (`CREATE_NO_WINDOW`).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

_CREATE_NO_WINDOW = 0x08000000 if sys.platform == "win32" else 0

_CORE_PYTHON_FILES = ("proxy.py", "service.py", "updater.py")


def load_env_file(repo_root: Path) -> None:
    """Populate unset environment variables from <repo_root>/.env."""
    env_path = repo_root / ".env"
    if not env_path.is_file():
        return
    try:
        with env_path.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip("\"'")
                if key:
                    os.environ.setdefault(key, val)
    except OSError:
        pass


def is_auto_update_enabled() -> bool:
    raw = os.environ.get("PROXY_AUTO_UPDATE", "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def auto_update_on_start() -> bool:
    raw = os.environ.get("PROXY_AUTO_UPDATE_ON_START", "1").strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def get_update_interval_s() -> int:
    try:
        val = int(os.environ.get("PROXY_AUTO_UPDATE_INTERVAL_S", "3600"))
    except ValueError:
        val = 3600
    return max(60, val)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_cmd(
    args: list[str],
    cwd: Path,
    timeout_s: float = 20.0,
) -> tuple[int, str, str]:
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GCM_INTERACTIVE"] = "never"
    kwargs: dict[str, Any] = {
        "cwd": str(cwd),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "encoding": "utf-8",
        "errors": "replace",
        "timeout": timeout_s,
        "env": env,
    }
    if _CREATE_NO_WINDOW:
        kwargs["creationflags"] = _CREATE_NO_WINDOW
    try:
        proc = subprocess.run(args, **kwargs)
        return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()
    except FileNotFoundError as exc:
        return 127, "", str(exc)
    except subprocess.TimeoutExpired:
        return 124, "", f"timed out after {timeout_s:.0f}s"
    except OSError as exc:
        return 1, "", str(exc)


def _git(repo_root: Path, *git_args: str, timeout_s: float = 20.0) -> tuple[int, str, str]:
    return _run_cmd(["git", *git_args], cwd=repo_root, timeout_s=timeout_s)


def get_state_path(repo_root: Path) -> Path:
    return repo_root / "logs" / "update_state.json"


def write_update_state(repo_root: Path, state: dict[str, Any]) -> None:
    path = get_state_path(repo_root)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        tmp.replace(path)
    except OSError:
        pass


def _resolve_tracking(repo_root: Path) -> tuple[Optional[str], Optional[str], Optional[str], str]:
    rc, branch, _ = _git(repo_root, "symbolic-ref", "--short", "HEAD")
    if rc != 0 or not branch:
        return None, None, None, "detached_head"

    rc, upstream_full, _ = _git(repo_root, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
    if rc == 0 and "/" in upstream_full:
        remote, remote_branch = upstream_full.split("/", 1)
        return branch, remote, remote_branch, ""

    rc, remotes_out, _ = _git(repo_root, "remote")
    remotes = [r.strip() for r in remotes_out.splitlines() if r.strip()]
    if not remotes:
        return branch, None, None, "no_remote"
    remote = os.environ.get("PROXY_AUTO_UPDATE_REMOTE", "").strip() or (
        "origin" if "origin" in remotes else remotes[0]
    )
    return branch, remote, branch, ""


def check_for_update(repo_root: Path, timeout_s: float = 20.0) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    result: dict[str, Any] = {
        "checked_at": _utc_now_iso(),
        "status": "unknown",
        "update_available": False,
        "local_commit": None,
        "remote_commit": None,
        "branch": None,
        "remote": None,
        "commits_behind": 0,
        "error": None,
    }

    rc, _, err = _git(repo_root, "rev-parse", "--is-inside-work-tree")
    if rc == 127:
        result["status"] = "git_missing"
        result["error"] = err or "git executable not found"
        return result
    if rc != 0:
        result["status"] = "not_git_repo"
        return result

    _, local_sha, _ = _git(repo_root, "rev-parse", "HEAD")
    result["local_commit"] = local_sha or None

    branch, remote, remote_branch, err_status = _resolve_tracking(repo_root)
    result["branch"] = branch
    result["remote"] = remote
    if err_status or not remote or not remote_branch:
        result["status"] = err_status or "no_remote"
        return result

    rc, _, err = _git(repo_root, "fetch", "--quiet", remote, remote_branch, timeout_s=timeout_s)
    if rc != 0:
        result["status"] = "fetch_failed"
        result["error"] = (err or "git fetch failed")[:240]
        return result

    target_ref = f"refs/remotes/{remote}/{remote_branch}"
    rc, remote_sha, err = _git(repo_root, "rev-parse", target_ref)
    if rc != 0 or not remote_sha:
        rc, remote_sha, err = _git(repo_root, "rev-parse", "FETCH_HEAD")
        if rc != 0 or not remote_sha:
            result["status"] = "remote_ref_missing"
            result["error"] = (err or "remote ref not found")[:240]
            return result
        target_ref = "FETCH_HEAD"

    result["remote_commit"] = remote_sha
    result["target_ref"] = target_ref

    if local_sha == remote_sha:
        result["status"] = "up_to_date"
        return result

    rc, _, _ = _git(repo_root, "merge-base", "--is-ancestor", local_sha, remote_sha)
    if rc != 0:
        result["status"] = "diverged"
        return result

    rc, count_str, _ = _git(repo_root, "rev-list", "--count", f"{local_sha}..{remote_sha}")
    try:
        behind = int(count_str) if rc == 0 else 1
    except ValueError:
        behind = 1

    result["status"] = "update_available"
    result["update_available"] = True
    result["commits_behind"] = behind
    return result


def _py_compile_files(repo_root: Path, python_exe: str, files: Iterable[str]) -> tuple[bool, str]:
    targets = []
    seen: set[str] = set()
    for rel in files:
        if not rel.endswith(".py") or rel in seen:
            continue
        p = repo_root / rel
        if p.is_file():
            targets.append(str(p))
            seen.add(rel)
    if not targets:
        return True, ""
    rc, out, err = _run_cmd([python_exe, "-m", "py_compile", *targets], cwd=repo_root, timeout_s=30.0)
    if rc != 0:
        return False, (err or out or "py_compile failed")[:240]
    return True, ""


def apply_update(
    repo_root: Path,
    python_exe: str = sys.executable,
    timeout_s: float = 45.0,
) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    check = check_for_update(repo_root, timeout_s=min(timeout_s, 25.0))
    out: dict[str, Any] = {
        **check,
        "applied": False,
        "restart_required": False,
        "changed_files": [],
        "deps_synced": False,
    }

    if check["status"] != "update_available":
        out.pop("target_ref", None)
        write_update_state(repo_root, out)
        return out

    old_sha = str(check["local_commit"])
    target_ref = str(check.get("target_ref") or "FETCH_HEAD")

    rc, dirty_out, _ = _git(repo_root, "status", "--porcelain", "--untracked-files=no")
    if rc != 0 or dirty_out:
        out["status"] = "dirty_worktree"
        out["error"] = "uncommitted edits in tracked files; skipping auto-update"
        out.pop("target_ref", None)
        write_update_state(repo_root, out)
        return out

    rc, _, err = _git(repo_root, "merge", "--ff-only", target_ref, timeout_s=timeout_s)
    if rc != 0:
        _git(repo_root, "merge", "--abort")
        out["status"] = "merge_failed"
        out["error"] = (err or "git merge --ff-only failed")[:240]
        out.pop("target_ref", None)
        write_update_state(repo_root, out)
        return out

    _, new_sha, _ = _git(repo_root, "rev-parse", "HEAD")
    _, diff_out, _ = _git(repo_root, "diff", "--name-only", old_sha, new_sha)
    changed = [line.strip() for line in diff_out.splitlines() if line.strip()]

    compile_set = list(dict.fromkeys([*_CORE_PYTHON_FILES, *[f for f in changed if f.endswith(".py")]]))
    ok_compile, compile_err = _py_compile_files(repo_root, python_exe, compile_set)
    if not ok_compile:
        _git(repo_root, "reset", "--hard", old_sha)
        out["status"] = "rolled_back"
        out["error"] = f"py_compile failed after pull; rolled back ({compile_err})"
        out.pop("target_ref", None)
        write_update_state(repo_root, out)
        return out

    req_path = repo_root / "requirements.txt"
    if "requirements.txt" in changed and req_path.is_file():
        rc_pip, out_pip, err_pip = _run_cmd(
            [python_exe, "-m", "pip", "install", "--disable-pip-version-check", "-q", "-r", str(req_path)],
            cwd=repo_root,
            timeout_s=120.0,
        )
        if rc_pip == 0:
            out["deps_synced"] = True
        else:
            out["error"] = f"pip install failed: {(err_pip or out_pip)[:200]}"

    out["status"] = "updated"
    out["applied"] = True
    out["restart_required"] = True
    out["old_commit"] = old_sha
    out["local_commit"] = new_sha
    out["changed_files"] = changed
    out.pop("target_ref", None)
    write_update_state(repo_root, out)
    return out


def main(argv: Optional[list[str]] = None) -> int:
    repo_root = Path(__file__).resolve().parent
    load_env_file(repo_root)

    parser = argparse.ArgumentParser(description="Safe auto-updater for the AgentRouter Codex proxy.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="Check for available upstream updates without changing files.")
    mode.add_argument("--apply", action="store_true", help="Fetch and fast-forward apply any clean update (default).")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON.")
    args = parser.parse_args(argv)

    if args.check:
        res = check_for_update(repo_root)
        write_update_state(repo_root, res)
    else:
        res = apply_update(repo_root, python_exe=sys.executable)

    if args.json:
        print(json.dumps(res, indent=2))
    else:
        status = res.get("status", "unknown")
        local_short = (res.get("local_commit") or "")[:7]
        remote_short = (res.get("remote_commit") or "")[:7]
        if status == "updated":
            old_short = (res.get("old_commit") or "")[:7]
            print(f"[ ok  ] updated {old_short} -> {local_short} ({len(res.get('changed_files', []))} file(s) changed)")
        elif status == "update_available":
            print(f"[info ] update available: {local_short} -> {remote_short} ({res.get('commits_behind', 1)} commit(s) behind)")
        elif status == "up_to_date":
            print(f"[ ok  ] already up to date ({local_short})")
        else:
            err = f": {res['error']}" if res.get("error") else ""
            print(f"[info ] updater status={status}{err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
