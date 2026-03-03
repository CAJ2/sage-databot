"""Sync the databot repo to a local Windmill instance and optionally run a script.

Usage:
    python src/dev_sync.py                          # Sync repo to local Windmill
    python src/dev_sync.py --run f/scripts/gen_ids  # Sync then run a script
    python src/dev_sync.py --run f/scripts/gen_ids --run-args '{}'
    python src/dev_sync.py --no-sync --run f/scripts/gen_ids
    python src/dev_sync.py --workspace myworkspace  # Override workspace ID
"""

import argparse
import json
import subprocess
import sys
import urllib.request
import urllib.error
from pathlib import Path

LOCAL_URL = "http://localhost:8400"
DEFAULT_WORKSPACE = "localdev"
TOKEN_FILE = Path(__file__).parent.parent / ".local" / "windmill" / "token"


def check_windmill_up() -> bool:
    """Return True if local Windmill is responding."""
    try:
        with urllib.request.urlopen(LOCAL_URL, timeout=5) as resp:
            return resp.status < 500
    except Exception:
        return False


def get_token() -> list[str]:
    """Return token CLI args if a saved token exists."""
    if TOKEN_FILE.exists():
        return ["--token", TOKEN_FILE.read_text().strip()]
    return []


def ensure_workspace(workspace: str) -> None:
    """Verify the wmill workspace is configured, exit with hint if not."""
    result = subprocess.run(
        ["wmill", "workspace", "whoami", "--workspace", workspace],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(
            f"Workspace '{workspace}' not configured in wmill.\n"
            f"Run: pixi run local-up  (to initialize and start local Windmill)",
            file=sys.stderr,
        )
        sys.exit(1)


def cmd_sync(workspace: str) -> None:
    """Push local repo scripts to the local Windmill workspace."""
    token_args = get_token()
    print(f"Syncing to workspace '{workspace}'...")
    result = subprocess.run(
        [
            "wmill",
            "sync",
            "push",
            "--workspace",
            workspace,
            "--yes",
            *token_args,
        ],
        text=True,
    )
    if result.returncode != 0:
        print("Sync failed.", file=sys.stderr)
        sys.exit(result.returncode)
    print("Sync complete.")


def cmd_run(script_path: str, run_args: str | None, workspace: str) -> None:
    """Run a script in the local Windmill workspace."""
    token_args = get_token()
    cmd = [
        "wmill",
        "script",
        "run",
        script_path,
        "--workspace",
        workspace,
        *token_args,
    ]
    if run_args:
        # Validate JSON
        try:
            json.loads(run_args)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in --run-args: {e}", file=sys.stderr)
            sys.exit(1)
        cmd += ["-d", run_args]

    print(f"Running script: {script_path}")
    result = subprocess.run(cmd, text=True)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync databot repo to local Windmill and optionally run a script"
    )
    parser.add_argument(
        "--run",
        metavar="SCRIPT_PATH",
        help="Script path to run after syncing (e.g. f/scripts/gen_ids)",
    )
    parser.add_argument(
        "--run-args",
        metavar="JSON",
        help='JSON args to pass to the script (e.g. \'{"key": "value"}\')',
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip syncing; only run the script specified by --run",
    )
    parser.add_argument(
        "--workspace",
        default=DEFAULT_WORKSPACE,
        metavar="ID",
        help=f"Local workspace ID (default: {DEFAULT_WORKSPACE})",
    )

    args = parser.parse_args()

    if args.no_sync and not args.run:
        parser.error("--no-sync requires --run")

    # Check Windmill is up
    if not check_windmill_up():
        print(
            f"Local Windmill is not running at {LOCAL_URL}.\n"
            "Start it with: pixi run local-up",
            file=sys.stderr,
        )
        sys.exit(1)

    ensure_workspace(args.workspace)

    if not args.no_sync:
        cmd_sync(args.workspace)

    if args.run:
        cmd_run(args.run, args.run_args, args.workspace)


if __name__ == "__main__":
    main()
