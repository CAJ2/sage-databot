import os
import time

import wmill


def main():
    """Safety check: abort if not running in a test workspace."""
    workspace = os.environ.get("WM_WORKSPACE", "")
    if not workspace.startswith("wm-fork-test"):
        raise RuntimeError(
            f"SAFETY CHECK FAILED: workspace is '{workspace}', expected 'wm-fork-test*'. Tests must run in a forked test workspace. Aborting to prevent accidental data modification."
        )
    print(f"✅ Running in test workspace: {workspace}")

    # Wait for dependency jobs triggered by wmill sync push to complete
    wmill_client = wmill.client.Windmill()
    timeout = 300
    interval = 5
    start = time.time()
    while True:
        queued = wmill_client.get(
            f"/w/{workspace}/jobs/queue/list",
            params={"job_kinds": "dependencies"},
        ).json()
        running = wmill_client.get(
            f"/w/{workspace}/jobs/list",
            params={"running": True, "job_kinds": "dependencies"},
        ).json()
        pending = len(queued) + len(running)
        if pending == 0:
            print("✅ All dependency jobs complete")
            break
        elapsed = time.time() - start
        if elapsed >= timeout:
            raise RuntimeError(
                f"Timed out after {timeout}s waiting for {pending} dependency job(s) to complete"
            )
        print(f"⏳ Waiting for {pending} dependency job(s)... ({elapsed:.0f}s elapsed)")
        time.sleep(interval)

    return workspace
