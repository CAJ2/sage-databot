import wmill


def main(head_sha: str):
    """Discover all test scripts in f/test/ matching test_*.py pattern."""
    wmill_client = wmill.client.Windmill()
    res = wmill_client.get(
        f"/w/{wmill.get_workspace()}/scripts/list",
        params={"path_start": "f/test/"},
    )
    scripts = res.json()

    to_run: list[str] = []
    for s in scripts:
        path = s["path"]
        # Only include test scripts (test_*.py), skip framework/cleanup
        basename = path.split("/")[-1]
        if basename.startswith("test_"):
            to_run.append(path)

    print(f"Discovered {len(to_run)} test scripts: {to_run}")

    # Set flow state SHA
    wmill.set_flow_user_state("head_sha", head_sha)

    return to_run
