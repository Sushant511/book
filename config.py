import os
import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_ids: set[int]
    db_path: str


# Paste your token directly here (no environment variables).
token = "8603785735:AAE4BWgYqrtn4HbLeVJP8qTQVbta8d-MNOo"
# Bot DB path (used for admin lookup).
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DB_PATH = os.path.join(_BASE_DIR, "bot.db")

# Optional admin IDs (explicit list as requested).
# Replace with your Telegram numeric user id(s), e.g. [123456789]
admin_ids = []
_admin_id_set: set[int] = {int(x) for x in admin_ids}


def is_admin(user_id: int) -> bool:
    """
    True if this user is the admin stored in the database.
    """
    if int(user_id) in _admin_id_set:
        return True
    try:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT admin_id FROM bot_admin LIMIT 1;").fetchone()
        conn.close()
        if not row:
            return False
        return int(row["admin_id"]) == int(user_id)
    except sqlite3.Error:
        return False


def load_settings() -> Settings:
    return Settings(bot_token=token, admin_ids=_admin_id_set, db_path=_DB_PATH)

