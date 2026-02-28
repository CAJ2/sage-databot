import subprocess
from pathlib import Path

import wmill

from f.utils.general import is_production

DATABOT_REPO_URL = "https://github.com/CAJ2/sage-databot.git"


def checkout_repo(branch: str = "dev") -> Path:
    """
    This utility is used for accessing version-controlled files in the databot repository
    that are not treated as scripts in Windmill.
    Clones the databot git repository and checks out the specified branch.
    Returns the path to the cloned repository.
    """
    if is_production() and branch != "main":
        raise ValueError("In production, only the 'main' branch can be cloned.")

    head_sha = wmill.get_flow_user_state("head_sha")

    if head_sha:
        # Only works with Git 2.49.0+
        # subprocess.run(
        #     [
        #       "git", "clone", "--depth", "1", "--revision", head_sha, DATABOT_REPO_URL, "./databot",
        #     ],
        #     check=True,
        # )
        #
        # Instead use this workaround
        subprocess.run(
            [
                "git", "init", "-q", "databot",
                "&&", "cd", "databot",
                "&&", "git", "remote", "add", "origin", DATABOT_REPO_URL,
                "&&", "git", "fetch", "--depth", "1", "origin", head_sha,
                "&&", "git", "checkout", "-q", "FETCH_HEAD",
            ],
            check=True,
        )
    else:
        subprocess.run(
            [
                "git", "clone", "--branch", branch, "--depth", "1",
                DATABOT_REPO_URL, "./databot",
            ],
            check=True,
        )

    return Path("./databot")
