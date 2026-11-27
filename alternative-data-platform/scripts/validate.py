"""Run the CI checks from any working directory using the current Python runtime."""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from demo import ROOT, replay


def main():
    subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
                   cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory() as tmp:
        actual = replay(Path(tmp))
    expected = json.loads((ROOT / "reports/demo.json").read_text())
    if actual != expected:
        raise SystemExit("Demo differs from reports/demo.json")
    print("Point-in-time exports, source health and raw hashes match the retained demo.")


if __name__ == "__main__":
    main()
