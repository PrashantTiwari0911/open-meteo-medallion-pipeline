from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


def load_config() -> dict:
    cfg = yaml.safe_load((ROOT / "config/pipeline.yaml").read_text())
    cfg["locations"] = yaml.safe_load((ROOT / "config/locations.yaml").read_text())["locations"]
    cfg["bronze_dir"] = ROOT / cfg["bronze_dir"]
    cfg["warehouse"] = ROOT / cfg["warehouse"]
    return cfg
