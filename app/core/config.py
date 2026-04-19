import json
import os
from pathlib import Path

SETTINGS_PATH = Path(os.getenv("SETTINGS_PATH", "data/settings.json"))
HISTORY_DB_PATH = Path(os.getenv("HISTORY_DB_PATH", "data/history.db"))

DEFAULT_SETTINGS = {
    "router_provider": os.getenv("ROUTER_PROVIDER", "groq"),
    "router_model": "llama3-8b-8192",
    "router_api_key": os.getenv("ROUTER_API_KEY", ""),
    "generator_provider": os.getenv("GENERATOR_PROVIDER", "openai"),
    "generator_model": "gpt-4o",
    "generator_api_key": os.getenv("GENERATOR_API_KEY", ""),
    "ollama_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
    "ollama_model": os.getenv("OLLAMA_MODEL", "snowflake-arctic-text2sql-r1:7b"),
    "db_engine": "duckdb",
    "duckdb_path": os.getenv("DUCKDB_PATH", "data/kur.db"),
    "db_host": os.getenv("DB_HOST", "localhost"),
    "db_port": int(os.getenv("DB_PORT", "5432")),
    "db_name": os.getenv("DB_NAME", "business_db"),
    "db_user": os.getenv("DB_USER", "analyst"),
    "db_password": os.getenv("DB_PASSWORD", ""),
    "polaris_url": os.getenv("POLARIS_URL", "http://polaris:8181"),
    "polaris_catalog": os.getenv("POLARIS_CATALOG", "kur_polaris_catalog"),
    "polaris_credentials": os.getenv("POLARIS_CREDENTIALS", "polaris:polaris_secret"),
    "max_retries": 3,
    "query_timeout": 30,
    "max_rows": 1000,
    "language": "auto",
}


def load_settings() -> dict:
    try:
        if SETTINGS_PATH.exists():
            with open(SETTINGS_PATH, "r") as f:
                saved = json.load(f)
            return {**DEFAULT_SETTINGS, **saved}
    except Exception:
        pass
    return dict(DEFAULT_SETTINGS)


def save_settings(settings: dict):
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SETTINGS_PATH, "w") as f:
        json.dump(settings, f, indent=2)


def apply_settings(settings: dict):
    env_map = {
        "router_provider": "ROUTER_PROVIDER",
        "router_model": "ROUTER_MODEL",
        "router_api_key": "ROUTER_API_KEY",
        "generator_provider": "GENERATOR_PROVIDER",
        "generator_model": "GENERATOR_MODEL",
        "generator_api_key": "GENERATOR_API_KEY",
        "ollama_url": "OLLAMA_BASE_URL",
        "ollama_model": "OLLAMA_MODEL",
        "db_engine": "DB_ENGINE",
        "duckdb_path": "DUCKDB_PATH",
        "db_host": "DB_HOST",
        "db_port": "DB_PORT",
        "db_name": "DB_NAME",
        "db_user": "DB_USER",
        "db_password": "DB_PASSWORD",
        "polaris_url": "POLARIS_URL",
        "polaris_catalog": "POLARIS_CATALOG",
        "polaris_credentials": "POLARIS_CREDENTIALS",
        "max_retries": "MAX_RETRIES",
        "query_timeout": "QUERY_TIMEOUT",
        "max_rows": "MAX_ROWS",
    }
    for key, env_var in env_map.items():
        if key in settings and settings[key]:
            os.environ[env_var] = str(settings[key])


def mask_key(key: str) -> str:
    if not key:
        return ""
    if len(key) < 8:
        return "•" * len(key)
    return key[:4] + "•" * (len(key) - 8) + key[-4:]
