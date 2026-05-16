"""
Pipeline configuration loader.

Reads config/pipeline.yml and config/categories.yml.
All paths and secrets are resolved here; no other module reads env vars directly.
"""

import os
from pathlib import Path

import yaml

_CONFIG_DIR = Path(__file__).parent.parent / "config"


def load_config() -> dict:
    pipeline_path = _CONFIG_DIR / "pipeline.yml"
    categories_path = _CONFIG_DIR / "categories.yml"

    with pipeline_path.open() as f:
        config = yaml.safe_load(f)

    with categories_path.open() as f:
        config["categories"] = yaml.safe_load(f)["categories"]

    # Inject secrets from environment — never from files.
    # .strip() guards against secrets pasted with trailing newlines in GitHub UI.
    config["r2_access_key_id"] = _require_env("R2_ACCESS_KEY_ID").strip()
    config["r2_secret_access_key"] = _require_env("R2_SECRET_ACCESS_KEY").strip()
    config["r2_endpoint_url"] = _require_env("R2_ENDPOINT_URL").strip()
    config["r2_bucket"] = _require_env("R2_BUCKET").strip()

    return config


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"Required environment variable not set: {name}")
    return value
