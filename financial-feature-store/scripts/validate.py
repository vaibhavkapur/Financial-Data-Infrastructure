"""Full validation gate: lint, unit/integration/scenario tests, demo, CLI, and evidence reports."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _run(cmd: list[str]) -> None:
    print(f"==> Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=ROOT, check=False)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def main() -> None:
    py = sys.executable
    bin_dir = Path(py).parent
    ruff = str(bin_dir / "ruff")
    pytest = str(bin_dir / "pytest")
    ffs = str(bin_dir / "ffs")

    _run([ruff, "check", "src", "tests", "scripts"])
    _run([pytest, "-v"])
    _run([py, "scripts/demo.py"])
    _run([ffs, "inspect-registry", "data/registry/feature_registry.json"])
    _run([py, "scripts/reproduce.py"])
    print("\nALL VALIDATION GATES PASSED.")


if __name__ == "__main__":
    main()
