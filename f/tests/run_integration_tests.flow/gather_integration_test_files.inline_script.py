import wmill

def main():
    wmill_client = wmill.client.Windmill()
    res = wmill_client.get(f"/w/{wmill.get_workspace()}/scripts/list", params={"path_start":"f/tests/"})
    scripts = res.json()
    
    to_run: list[str] = []
    for s in scripts:
        to_run.append(s["path"])
    return to_run