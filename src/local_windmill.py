"""Local Windmill instance manager using podman compose.

Usage:
    python src/local_windmill.py            # Start (init if needed)
    python src/local_windmill.py --stop     # Stop containers
    python src/local_windmill.py --status   # Show container status
    python src/local_windmill.py --sync-secrets  # Sync secrets from dev workspace
    python src/local_windmill.py --reset    # Stop and remove volumes (destructive)
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

WINDMILL_DIR = Path(__file__).parent.parent / ".local" / "windmill"
COMPOSE_FILE = WINDMILL_DIR / "docker-compose.yml"
TOKEN_FILE = WINDMILL_DIR / "token"

LOCAL_URL = "http://localhost:8400"
LOCAL_WORKSPACE = "localdev"
LOCAL_WORKSPACE_ID = "localdev"
ADMIN_EMAIL = "admin@windmill.dev"
ADMIN_PASSWORD = "changeme"

RAW_BASE = "https://raw.githubusercontent.com/windmill-labs/windmill/main"
COMPOSE_URL = f"{RAW_BASE}/docker-compose.yml"
CADDYFILE_URL = f"{RAW_BASE}/Caddyfile"
ENV_URL = f"{RAW_BASE}/.env"


def download_file(url: str, dest: Path) -> None:
    """Download a file from URL to dest path."""
    print(f"  Downloading {dest.name}...")
    with urllib.request.urlopen(url, timeout=30) as resp:
        dest.write_bytes(resp.read())


def patch_compose(compose_path: Path) -> None:
    """Patch docker-compose.yml for podman and port 8400.

    Removes the docker socket bind-mount — containers run inside a Linux VM
    and cannot access the macOS host socket path. Windmill's native Python/
    TypeScript workers don't need it anyway.
    """
    lines = compose_path.read_text().splitlines(keepends=True)

    # Remove any line that bind-mounts a path into /var/run/docker.sock.
    # This handles both the original line and any previously-patched variant.
    filtered = [line for line in lines if "docker.sock" not in line]
    patched = "".join(filtered)

    # Change caddy port from 80:80 to 8400:80 (avoids macOS low-port privilege)
    patched = patched.replace('"80:80"', '"8400:80"')
    patched = patched.replace("'80:80'", "'8400:80'")
    patched = patched.replace("\n      - 80:80\n", "\n      - 8400:80\n")

    compose_path.write_text(patched)
    print("  Patched docker-compose.yml (removed docker.sock mount, port: 8400)")


def init_windmill_dir() -> None:
    """Download and configure Windmill compose files."""
    print("Initializing Windmill local environment...")
    WINDMILL_DIR.mkdir(parents=True, exist_ok=True)

    download_file(COMPOSE_URL, WINDMILL_DIR / "docker-compose.yml")
    download_file(CADDYFILE_URL, WINDMILL_DIR / "Caddyfile")
    download_file(ENV_URL, WINDMILL_DIR / ".env")

    patch_compose(COMPOSE_FILE)
    print("  Initialization complete.")


def ensure_podman_machine() -> None:
    """Start the podman machine if it exists but isn't running."""
    try:
        result = subprocess.run(
            ["podman", "machine", "list", "--format", "json"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return  # No machines configured; rootful podman, skip
        machines = json.loads(result.stdout)
        if not machines:
            return
        running = [m for m in machines if m.get("Running")]
        if running:
            return  # Already running
        # Start the default machine (or the first one)
        default = next((m for m in machines if m.get("Default")), machines[0])
        name = default.get("Name", "podman-machine-default")
        print(f"Starting podman machine '{name}'...")
        subprocess.run(["podman", "machine", "start", name], check=True)
        print(f"  Podman machine '{name}' started.")
    except (FileNotFoundError, json.JSONDecodeError, subprocess.TimeoutExpired):
        pass  # podman not installed or doesn't support machine; proceed anyway


def run_compose(*args: str) -> subprocess.CompletedProcess[str]:
    """Run podman compose with the project compose file."""
    cmd = ["podman", "compose", "-f", str(COMPOSE_FILE), *args]
    return subprocess.run(cmd, text=True)


def wait_for_windmill(timeout: int = 90) -> bool:
    """Poll until Windmill responds or timeout."""
    print(f"Waiting for Windmill at {LOCAL_URL} (timeout: {timeout}s)...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(LOCAL_URL, timeout=3) as resp:
                if resp.status < 500:
                    return True
        except Exception:
            pass
        time.sleep(2)
        print("  ...", end="", flush=True)
    print()
    return False


def api_post(path: str, payload: dict, bearer: str) -> str:
    """POST JSON to the local Windmill API and return the response body."""
    data = json.dumps(payload).encode()
    request = urllib.request.Request(
        f"{LOCAL_URL}/api{path}",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {bearer}",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as resp:
        return resp.read().decode().strip().strip('"')


def login() -> str:
    """Login with default admin credentials and return a session token."""
    data = json.dumps({"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}).encode()
    request = urllib.request.Request(
        f"{LOCAL_URL}/api/auth/login",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as resp:
        return resp.read().decode().strip().strip('"')


def setup_local_workspace(session_token: str) -> str | None:
    """Create the localdev workspace in Windmill and return a permanent API token.

    Steps:
    1. Create workspace via API (creating user becomes owner)
    2. Create a permanent API token
    3. Register the workspace with the wmill CLI
    """
    try:
        # Step 1: create the workspace
        print(f"  Creating '{LOCAL_WORKSPACE_ID}' workspace in Windmill...")
        try:
            api_post(
                "/workspaces/create",
                {"id": LOCAL_WORKSPACE_ID, "name": "Local Dev"},
                bearer=session_token,
            )
            print(f"  Workspace '{LOCAL_WORKSPACE_ID}' created.")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code == 409:
                print(f"  Workspace '{LOCAL_WORKSPACE_ID}' already exists.")
            else:
                print(
                    f"  Workspace creation failed (HTTP {e.code}): {body}",
                    file=sys.stderr,
                )
                raise

        # Step 2: create a permanent API token (scoped to localdev workspace)
        api_token = api_post(
            "/users/tokens/create",
            {"label": "local-dev", "expiration": None},
            bearer=session_token,
        )
        print("  API token created.")

        # Step 3: register with wmill CLI (always re-add to pick up new token)
        print(f"  Registering wmill workspace profile '{LOCAL_WORKSPACE}'...")
        proc = subprocess.run(
            [
                "wmill",
                "workspace",
                "add",
                LOCAL_WORKSPACE,
                LOCAL_WORKSPACE_ID,
                LOCAL_URL,
                "--token",
                api_token,
            ],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            # wmill prints an error if the name already exists; try removing first
            subprocess.run(
                ["wmill", "workspace", "remove", LOCAL_WORKSPACE],
                capture_output=True,
            )
            subprocess.run(
                [
                    "wmill",
                    "workspace",
                    "add",
                    LOCAL_WORKSPACE,
                    LOCAL_WORKSPACE_ID,
                    LOCAL_URL,
                    "--token",
                    api_token,
                ],
                check=True,
            )
        print("  Workspace profile registered.")
        return api_token

    except urllib.error.URLError as e:
        print(f"  API error during workspace setup: {e}", file=sys.stderr)
        return None


def cmd_start() -> None:
    """Initialize if needed, then start Windmill."""
    if not COMPOSE_FILE.exists():
        init_windmill_dir()
    else:
        # Re-apply patches on every start so stale/broken compose files are
        # fixed automatically (e.g. after a script update or manual edits).
        patch_compose(COMPOSE_FILE)

    ensure_podman_machine()
    print("Starting Windmill...")
    result = run_compose("up", "-d")
    if result.returncode != 0:
        print("Failed to start containers.", file=sys.stderr)
        sys.exit(1)

    if not wait_for_windmill():
        print(
            f"Windmill did not become ready in time. Check logs with: podman compose -f {COMPOSE_FILE} logs",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"\nWindmill is up at {LOCAL_URL}")

    # Check whether the workspace is fully set up (token saved AND in wmill CLI)
    wmill_list = subprocess.run(
        ["wmill", "workspace", "list"], capture_output=True, text=True
    )
    workspace_in_cli = LOCAL_WORKSPACE_ID in wmill_list.stdout

    if not TOKEN_FILE.exists() or not workspace_in_cli:
        print("Setting up local workspace...")
        # Give Windmill a few more seconds to fully initialize
        time.sleep(3)
        try:
            session_token = login()
            token = setup_local_workspace(session_token)
            if token:
                TOKEN_FILE.write_text(token)
                print(f"  Token saved to {TOKEN_FILE}")
            else:
                print(
                    f"  Could not complete setup. Visit {LOCAL_URL} and log in manually."
                )
                print(f"  Default credentials: {ADMIN_EMAIL} / {ADMIN_PASSWORD}")
        except urllib.error.URLError as e:
            print(f"  Login failed: {e}")
            print(
                f"  Visit {LOCAL_URL} to set up manually. Credentials: {ADMIN_EMAIL} / {ADMIN_PASSWORD}"
            )
    else:
        print("Local workspace ready.")

    print(f"\nOpen Windmill: {LOCAL_URL}")


def cmd_stop() -> None:
    """Stop running containers."""
    if not COMPOSE_FILE.exists():
        print("No compose file found. Run without flags to initialize.")
        sys.exit(1)
    print("Stopping Windmill...")
    result = run_compose("down")
    if result.returncode != 0:
        sys.exit(result.returncode)
    print("Windmill stopped.")


def cmd_status() -> None:
    """Print a single status line and exit with 0 (up) or 1 (down/partial)."""
    if not COMPOSE_FILE.exists():
        print("down (not initialized)")
        sys.exit(1)

    result = subprocess.run(
        [
            "podman",
            "ps",
            "-a",
            "--filter",
            "label=com.docker.compose.project=windmill",
            "--format",
            "{{.State}}",
        ],
        capture_output=True,
        text=True,
    )
    states = [s.strip() for s in result.stdout.splitlines() if s.strip()]
    total = len(states)
    running = sum(1 for s in states if s == "running")

    if total == 0:
        print("down (no containers)")
        sys.exit(1)
    elif running == total:
        print(f"up ({running}/{total} services running)")
        sys.exit(0)
    else:
        print(f"down ({running}/{total} services running)")
        sys.exit(1)


def cmd_reset() -> None:
    """Stop containers and remove volumes (destructive)."""
    if not COMPOSE_FILE.exists():
        print("Not initialized. Nothing to reset.")
        return

    confirm = input("This will delete all local Windmill data. Type 'yes' to confirm: ")
    if confirm.strip().lower() != "yes":
        print("Reset cancelled.")
        return

    print("Resetting Windmill (removing volumes)...")
    result = run_compose("down", "-v")
    if result.returncode != 0:
        sys.exit(result.returncode)

    # Remove token so next start re-initializes workspace
    if TOKEN_FILE.exists():
        TOKEN_FILE.unlink()
        print("  Removed saved token.")

    print("Reset complete. Run without flags to re-initialize.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manage a local Windmill instance via podman compose"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--stop", action="store_true", help="Stop the running instance")
    group.add_argument("--status", action="store_true", help="Show container status")
    group.add_argument(
        "--reset",
        action="store_true",
        help="Stop and remove volumes (destructive)",
    )

    args = parser.parse_args()

    if args.stop:
        cmd_stop()
    elif args.status:
        cmd_status()
    elif args.reset:
        cmd_reset()
    else:
        cmd_start()


if __name__ == "__main__":
    main()
