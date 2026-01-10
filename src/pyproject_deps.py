import tomllib
import subprocess
from pathlib import Path
import re

def extract_python_version(requires_python: str) -> str:
    # Extract the first version number from a string like ">=3.12"
    m = re.search(r"(\d+\.\d+)", requires_python)
    return m.group(1) if m else "3.12"

def main():
    root = Path(__file__).parents[1]
    pyproject = root / "pyproject.toml"
    deps_dir = root / "dependencies"
    deps_dir.mkdir(exist_ok=True)
    with pyproject.open("rb") as f:
        data = tomllib.load(f)
    requires_python = data.get("project", {}).get("requires-python", ">=3.12")
    py_version = extract_python_version(requires_python)
    # Main project dependencies
    out_path = deps_dir / "project.requirements.in"
    with out_path.open("w") as f:
        f.write(f"# py: {py_version}\n")
    subprocess.run(
        f"uv export --no-hashes --format requirements-txt -q >> {out_path}",
        shell=True, check=True, cwd=root
    )
    # Optional dependencies
    opt_deps = data.get("project", {}).get("optional-dependencies", {})
    for name in opt_deps:
        out_path = deps_dir / f"{name}.requirements.in"
        with out_path.open("w") as f:
            f.write(f"# py: {py_version}\n")
        subprocess.run(
            f"uv export --no-hashes --format requirements-txt -q --extra {name} >> {out_path}",
            shell=True, check=True, cwd=root
        )

if __name__ == "__main__":
    main()
