import wmill


def main(test_script: str):
    res = wmill.run_script_by_path(test_script, timeout=60)
    return res