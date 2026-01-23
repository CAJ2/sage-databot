from pathlib import Path
import subprocess

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

    subprocess.run(
        [
            "git",
            "clone",
            "--branch",
            branch,
            "--depth",
            "1",
            DATABOT_REPO_URL,
            "./databot",
        ],
        check=True,
    )
    return Path("./databot")
