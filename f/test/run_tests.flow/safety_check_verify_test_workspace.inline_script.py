import os


def main():
    """Safety check: abort if not running in a test workspace."""
    workspace = os.environ.get("WM_WORKSPACE", "")
    if not workspace.startswith("wm-fork-test"):
        raise RuntimeError(
            f"SAFETY CHECK FAILED: workspace is '{workspace}', "
            "expected 'wm-fork-test*'. Tests must run in a forked test workspace. "
            "Aborting to prevent accidental data modification."
        )
    print(f"✅ Running in test workspace: {workspace}")
    return workspace
