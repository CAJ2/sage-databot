import wmill


def main(test_script: str):
    """Run a single test script and return its results."""
    print(f"Running test: {test_script}")
    result = wmill.run_script(test_script, timeout=300)
    return result
