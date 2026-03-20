from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any


@dataclass(frozen=True)
class DailyProfit:
    trade_date: str  # YYYY-MM-DD
    profit: float


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Better concurrency for a bot process.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                join_date TEXT,
                language TEXT,
                is_banned INTEGER NOT NULL DEFAULT 0,
                is_premium INTEGER NOT NULL DEFAULT 0,
                expiry_date TEXT,
                is_paused INTEGER NOT NULL DEFAULT 0,
                pause_started_at TEXT,
                show_on_leaderboard INTEGER NOT NULL DEFAULT 0,
                leaderboard_paused INTEGER NOT NULL DEFAULT 0,
                leaderboard_consent_asked INTEGER NOT NULL DEFAULT 0,
                data_clear_count INTEGER NOT NULL DEFAULT 0,
                last_data_cleared_at TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                trade_date TEXT NOT NULL,
                created_at TEXT NOT NULL,
                qty REAL NOT NULL,
                buy_price REAL NOT NULL,
                sell_price REAL NOT NULL,
                profit REAL NOT NULL,
                roi REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_user_date ON trades(user_id, trade_date);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_user_created ON trades(user_id, created_at);")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_admin (
                admin_id INTEGER NOT NULL PRIMARY KEY
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT
            );
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO app_settings(key, value) VALUES('global_premium', '0');"
        )

        # ---- Schema migration for existing installs ----
        # SQLite doesn't support adding columns with IF NOT EXISTS cleanly, so we inspect.
        existing_cols = {r["name"] for r in conn.execute("PRAGMA table_info(users);").fetchall()}
        required_defaults: dict[str, str] = {
            "join_date": "TEXT",
            "language": "TEXT",
            "is_banned": "INTEGER NOT NULL DEFAULT 0",
            "is_premium": "INTEGER NOT NULL DEFAULT 0",
            "expiry_date": "TEXT",
            "is_paused": "INTEGER NOT NULL DEFAULT 0",
            "pause_started_at": "TEXT",
            "show_on_leaderboard": "INTEGER NOT NULL DEFAULT 0",
            "leaderboard_paused": "INTEGER NOT NULL DEFAULT 0",
            "leaderboard_consent_asked": "INTEGER NOT NULL DEFAULT 0",
            "data_clear_count": "INTEGER NOT NULL DEFAULT 0",
            "last_data_cleared_at": "TEXT",
        }
        for col, ddl in required_defaults.items():
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {ddl};")
        # Backfill join_date for older rows.
        conn.execute("UPDATE users SET join_date=first_seen WHERE join_date IS NULL AND first_seen IS NOT NULL;")


        # One-time migration: fix stored qty model for existing trade rows.
        # Safe-guarded by `app_settings.trade_qty_migrated`.
        row = conn.execute("SELECT value FROM app_settings WHERE key='trade_qty_migrated' LIMIT 1;").fetchone()
        if not row or str(row["value"] or "0") != "1":
            # Convert old qty (qty_old = amount / buy_price) to new volume/qty (amount).
            conn.execute("UPDATE trades SET qty = qty * buy_price;")
            conn.execute(
                """
                UPDATE trades
                SET
                    profit = (sell_price - buy_price) * qty,
                    roi = CASE WHEN buy_price != 0
                        THEN ((sell_price - buy_price) / buy_price) * 100
                        ELSE 0
                    END;
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO app_settings(key, value) VALUES('trade_qty_migrated', '1');"
            )
            conn.execute(
                "UPDATE app_settings SET value='1' WHERE key='trade_qty_migrated';"
            )


def get_admin_id(db_path: str) -> int | None:
    with _connect(db_path) as conn:
        row = conn.execute("SELECT admin_id FROM bot_admin LIMIT 1;").fetchone()
        if not row:
            return None
        return int(row["admin_id"])


def ensure_admin(db_path: str, user_id: int) -> None:
    """
    Set the first user as admin (only if admin is not set yet).
    """
    existing = get_admin_id(db_path)
    if existing is not None:
        return
    with _connect(db_path) as conn:
        conn.execute("INSERT OR IGNORE INTO bot_admin(admin_id) VALUES(?);", (int(user_id),))


def get_global_premium(db_path: str) -> int:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key='global_premium' LIMIT 1;"
        ).fetchone()
        if not row:
            conn.execute(
                "INSERT OR IGNORE INTO app_settings(key, value) VALUES('global_premium', '0');"
            )
            return 0
        try:
            return 1 if int(str(row["value"] or "0")) == 1 else 0
        except (TypeError, ValueError):
            return 0


def set_global_premium(db_path: str, enabled: bool) -> None:
    value = "1" if enabled else "0"
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO app_settings(key, value) VALUES('global_premium', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value;
            """,
            (value,),
        )


def migrate_trade_qty_model_v1_to_volume(db_path: str) -> None:
    """
    One-time migration for older installs.

    Previously, `qty` was stored as: qty = usdt_spent / buy_price
    After the fix, user input represents volume directly, so desired:
    qty_new = usdt_spent = qty_old * buy_price

    Then recompute profit/roi based on the updated qty.
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key='trade_qty_migrated' LIMIT 1;"
        ).fetchone()
        if row and str(row["value"] or "0") == "1":
            return

        # Convert qty back to the original user input amount.
        conn.execute("UPDATE trades SET qty = qty * buy_price;")

        # Recompute profit and ROI using the corrected qty.
        conn.execute(
            """
            UPDATE trades
            SET
                profit = (sell_price - buy_price) * qty,
                roi = CASE WHEN buy_price != 0
                    THEN ((sell_price - buy_price) / buy_price) * 100
                    ELSE 0
                END;
            """
        )

        conn.execute(
            "INSERT OR IGNORE INTO app_settings(key, value) VALUES('trade_qty_migrated', '1');"
        )
        conn.execute(
            "UPDATE app_settings SET value='1' WHERE key='trade_qty_migrated';"
        )


def upsert_user(db_path: str, user_id: int, username: str | None) -> None:
    now = datetime.utcnow().isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, username, first_seen, last_seen, join_date)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                username=excluded.username,
                last_seen=excluded.last_seen
            """,
            (user_id, username, now, now, now),
        )


def get_user_language(db_path: str, user_id: int) -> str | None:
    with _connect(db_path) as conn:
        row = conn.execute("SELECT language FROM users WHERE user_id=?;", (int(user_id),)).fetchone()
        if not row:
            return None
        lang = row["language"]
        if lang is None:
            return None
        return str(lang)


def get_user_flags(db_path: str, user_id: int) -> dict[str, Any]:
    """
    Returns language (can be None), is_banned, is_premium, expiry_date.
    Also refreshes expired premium flags automatically.
    """
    now = datetime.utcnow()
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT language, is_banned, is_premium, expiry_date, is_paused, pause_started_at, show_on_leaderboard, leaderboard_paused, leaderboard_consent_asked
            FROM users
            WHERE user_id=?;
            """,
            (int(user_id),),
        ).fetchone()
        if not row:
            return {
                "language": None,
                "is_banned": 0,
                "is_premium": 0,
                "expiry_date": None,
                "is_paused": 0,
                "pause_started_at": None,
                "show_on_leaderboard": 0,
                "leaderboard_paused": 0,
                "leaderboard_consent_asked": 0,
            }

        is_banned = int(row["is_banned"] or 0)
        is_premium = int(row["is_premium"] or 0)
        expiry_date = row["expiry_date"]
        is_paused = int(row["is_paused"] or 0)
        pause_started_at = row["pause_started_at"]
        show_on_leaderboard = int(row["show_on_leaderboard"] or 0)
        leaderboard_paused = int(row["leaderboard_paused"] or 0)
        leaderboard_consent_asked = int(row["leaderboard_consent_asked"] or 0)

        # Auto-expire premium on read (skip while paused).
        if is_premium and not is_paused and expiry_date:
            try:
                expiry = datetime.fromisoformat(str(expiry_date))
                if expiry <= now:
                    conn.execute(
                        "UPDATE users SET is_premium=0, expiry_date=NULL, is_paused=0, pause_started_at=NULL WHERE user_id=?;",
                        (int(user_id),),
                    )
                    is_premium = 0
                    expiry_date = None
                    is_paused = 0
                    pause_started_at = None
            except ValueError:
                # If stored date is invalid, just disable premium.
                conn.execute(
                    "UPDATE users SET is_premium=0, expiry_date=NULL, is_paused=0, pause_started_at=NULL WHERE user_id=?;",
                    (int(user_id),),
                )
                is_premium = 0
                expiry_date = None
                is_paused = 0
                pause_started_at = None

        return {
            "language": (str(row["language"]) if row["language"] is not None else None),
            "is_banned": is_banned,
            "is_premium": is_premium,
            "expiry_date": (str(expiry_date) if expiry_date is not None else None),
            "is_paused": is_paused,
            "pause_started_at": (str(pause_started_at) if pause_started_at is not None else None),
            "show_on_leaderboard": show_on_leaderboard,
            "leaderboard_paused": leaderboard_paused,
            "leaderboard_consent_asked": leaderboard_consent_asked,
        }


def set_user_language(db_path: str, user_id: int, language: str) -> None:
    with _connect(db_path) as conn:
        conn.execute("UPDATE users SET language=? WHERE user_id=?;", (language, int(user_id)))


def set_user_ban(db_path: str, user_id: int, banned: bool) -> None:
    with _connect(db_path) as conn:
        conn.execute("UPDATE users SET is_banned=? WHERE user_id=?;", (1 if banned else 0, int(user_id)))


def set_user_premium_days(db_path: str, user_id: int, days: int) -> None:
    expiry = datetime.utcnow() + timedelta(days=int(days))
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE users SET is_premium=1, expiry_date=?, is_paused=0, pause_started_at=NULL WHERE user_id=?;",
            (expiry.isoformat(), int(user_id)),
        )


def get_user_record(db_path: str, user_id: int) -> dict[str, Any] | None:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT user_id, username, is_premium, expiry_date, is_paused, pause_started_at, language
            FROM users
            WHERE user_id=?;
            """,
            (int(user_id),),
        ).fetchone()
    if not row:
        return None
    return {
        "user_id": int(row["user_id"]),
        "username": (str(row["username"]) if row["username"] is not None else None),
        "is_premium": int(row["is_premium"] or 0),
        "expiry_date": (str(row["expiry_date"]) if row["expiry_date"] is not None else None),
        "is_paused": int(row["is_paused"] or 0),
        "pause_started_at": (str(row["pause_started_at"]) if row["pause_started_at"] is not None else None),
        "language": (str(row["language"]) if row["language"] is not None else None),
    }


def extend_user_premium_days(db_path: str, user_id: int, days: int) -> None:
    now = datetime.utcnow()
    with _connect(db_path) as conn:
        row = conn.execute("SELECT expiry_date, is_premium FROM users WHERE user_id=?;", (int(user_id),)).fetchone()
        if not row:
            return
        expiry_raw = row["expiry_date"]
        if expiry_raw:
            try:
                expiry = datetime.fromisoformat(str(expiry_raw))
            except ValueError:
                expiry = now
        else:
            expiry = now
        base = expiry if expiry > now else now
        new_expiry = base + timedelta(days=int(days))
        conn.execute(
            "UPDATE users SET is_premium=1, expiry_date=?, is_paused=0, pause_started_at=NULL WHERE user_id=?;",
            (new_expiry.isoformat(), int(user_id)),
        )


def pause_user_premium(db_path: str, user_id: int) -> None:
    now = datetime.utcnow().isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            """
            UPDATE users
            SET is_paused=1,
                pause_started_at=CASE WHEN pause_started_at IS NULL THEN ? ELSE pause_started_at END
            WHERE user_id=? AND is_premium=1;
            """,
            (now, int(user_id)),
        )


def resume_user_premium(db_path: str, user_id: int) -> None:
    now = datetime.utcnow()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT expiry_date, pause_started_at, is_premium, is_paused FROM users WHERE user_id=?;",
            (int(user_id),),
        ).fetchone()
        if not row:
            return
        if int(row["is_premium"] or 0) != 1:
            return
        if int(row["is_paused"] or 0) != 1:
            return

        expiry_raw = row["expiry_date"]
        pause_raw = row["pause_started_at"]
        new_expiry = expiry_raw
        if expiry_raw and pause_raw:
            try:
                expiry = datetime.fromisoformat(str(expiry_raw))
                pause_start = datetime.fromisoformat(str(pause_raw))
                paused_delta = now - pause_start
                expiry = expiry + paused_delta
                new_expiry = expiry.isoformat()
            except ValueError:
                pass

        conn.execute(
            "UPDATE users SET is_paused=0, pause_started_at=NULL, expiry_date=? WHERE user_id=?;",
            (new_expiry, int(user_id)),
        )


def cancel_user_premium(db_path: str, user_id: int) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE users SET is_premium=0, expiry_date=NULL, is_paused=0, pause_started_at=NULL WHERE user_id=?;",
            (int(user_id),),
        )


def list_subscriptions(db_path: str) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT user_id, username, is_premium, expiry_date, is_paused
            FROM users
            WHERE is_premium=1 OR expiry_date IS NOT NULL
            ORDER BY expiry_date ASC;
            """
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "user_id": int(r["user_id"]),
                "username": (str(r["username"]) if r["username"] is not None else None),
                "is_premium": int(r["is_premium"] or 0),
                "expiry_date": (str(r["expiry_date"]) if r["expiry_date"] is not None else None),
                "is_paused": int(r["is_paused"] or 0),
            }
        )
    return out


def list_active_premium_users(db_path: str) -> list[dict[str, Any]]:
    now = datetime.utcnow()
    subs = list_subscriptions(db_path)
    out: list[dict[str, Any]] = []
    for s in subs:
        expiry_raw = s.get("expiry_date")
        if s.get("is_premium") != 1 or not expiry_raw:
            continue
        try:
            expiry = datetime.fromisoformat(str(expiry_raw))
        except ValueError:
            continue
        if int(s.get("is_paused") or 0) == 1 or expiry > now:
            out.append(s)
    return out


def set_user_show_on_leaderboard(db_path: str, user_id: int, show: bool) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE users SET show_on_leaderboard=?, leaderboard_consent_asked=1 WHERE user_id=?;",
            (1 if show else 0, int(user_id)),
        )


def set_user_leaderboard_paused(db_path: str, user_id: int, paused: bool) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE users SET leaderboard_paused=? WHERE user_id=?;",
            (1 if paused else 0, int(user_id)),
        )


def reset_leaderboard_all(db_path: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            UPDATE users
            SET show_on_leaderboard=0,
                leaderboard_paused=0,
                leaderboard_consent_asked=0;
            """
        )


def get_user_trade_total_profit(db_path: str, user_id: int, start_date: str | None = None) -> dict[str, Any]:
    query = """
        SELECT
            COALESCE(SUM(profit), 0) AS total_profit,
            COUNT(id) AS trade_count
        FROM trades
        WHERE user_id=?
    """
    params: list[Any] = [int(user_id)]
    if start_date:
        query += " AND trade_date >= ?"
        params.append(start_date)
    with _connect(db_path) as conn:
        row = conn.execute(query, tuple(params)).fetchone()
    return {"total_profit": float(row["total_profit"]), "trade_count": int(row["trade_count"])}


def leaderboard_totals(db_path: str, start_date: str | None = None, limit: int = 10, consent_only: bool = False) -> list[dict[str, Any]]:
    """
    Returns top-N leaderboard entries (user_id, username, total_profit) sorted by profit desc.
    """
    join_cond = ""
    if start_date:
        join_cond = " AND t.trade_date >= :start_date"

    consent_filter = ""
    if consent_only:
        consent_filter = " AND u.show_on_leaderboard=1 AND COALESCE(u.leaderboard_paused,0)=0"

    sql = f"""
        SELECT
            u.user_id AS user_id,
            u.username AS username,
            COALESCE(SUM(t.profit), 0) AS total_profit,
            COUNT(t.id) AS trade_count
        FROM users u
        LEFT JOIN trades t
            ON u.user_id=t.user_id {join_cond}
        WHERE 1=1 {consent_filter}
        GROUP BY u.user_id, u.username
        HAVING trade_count > 0
        ORDER BY total_profit DESC, u.user_id ASC
        LIMIT :limit;
    """
    with _connect(db_path) as conn:
        rows = conn.execute(sql, {"start_date": start_date, "limit": int(limit)}).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "user_id": int(r["user_id"]),
                "username": (str(r["username"]) if r["username"] is not None else None),
                "total_profit": float(r["total_profit"]),
                "trade_count": int(r["trade_count"]),
            }
        )
    return out


def leaderboard_rank(db_path: str, user_id: int, start_date: str | None = None, consent_only: bool = False) -> dict[str, Any]:
    """
    Rank is computed among all users with at least one trade in the given range.
    If consent_only=True, rank is computed among users who opted in.
    """
    query = """
        SELECT
            u.user_id AS user_id,
            u.username AS username,
            COALESCE(SUM(t.profit), 0) AS total_profit,
            COUNT(t.id) AS trade_count
        FROM users u
        JOIN trades t ON u.user_id=t.user_id
    """
    params: list[Any] = []
    where = []
    if consent_only:
        where.append("u.show_on_leaderboard=1")
        where.append("COALESCE(u.leaderboard_paused,0)=0")
    if start_date:
        where.append("t.trade_date >= ?")
        params.append(start_date)
    if where:
        query += " WHERE " + " AND ".join(where)
    query += """
        GROUP BY u.user_id, u.username
        ORDER BY total_profit DESC, u.user_id ASC
    """
    with _connect(db_path) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    totals: list[tuple[int, float]] = [(int(r["user_id"]), float(r["total_profit"])) for r in rows]
    sorted_ids = [uid for uid, _p in totals]
    trade_count = 0
    profit = 0.0
    for r in rows:
        if int(r["user_id"]) == int(user_id):
            trade_count = int(r["trade_count"])
            profit = float(r["total_profit"])
            break
    if trade_count <= 0:
        return {"rank": None, "total_profit": profit, "trade_count": 0}

    rank = None
    try:
        rank = sorted_ids.index(int(user_id)) + 1
    except ValueError:
        rank = None
    return {"rank": rank, "total_profit": profit, "trade_count": trade_count}


def weekly_leaderboard_range(db_path: str) -> str:
    # Last 7 days including today.
    start_date = (date.today() - timedelta(days=6)).isoformat()
    return start_date


def inactive_users_for_reminder(db_path: str, days_ago: int = 2) -> list[tuple[int, str | None]]:
    """
    Returns users whose last trade date is exactly `days_ago` days ago (to avoid spam).
    """
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT u.user_id, u.language
            FROM users u
            WHERE u.is_banned=0 AND EXISTS (
                SELECT 1
                FROM trades t
                WHERE t.user_id=u.user_id
                  AND t.trade_date = date('now', ?)
            )
              AND NOT EXISTS (
                SELECT 1
                FROM trades t2
                WHERE t2.user_id=u.user_id
                  AND t2.trade_date > date('now', ?)
            );
            """,
            (-f"{int(days_ago)} day", -f"{int(days_ago)} day"),
        ).fetchall()
    return [(int(r["user_id"]), (str(r["language"]) if r["language"] is not None else None)) for r in rows]


def list_users(db_path: str) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT user_id, username, join_date, language, is_banned, is_premium, expiry_date, data_clear_count, last_data_cleared_at
            FROM users
            ORDER BY join_date ASC;
            """
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "user_id": int(r["user_id"]),
                "username": (str(r["username"]) if r["username"] is not None else None),
                "join_date": (str(r["join_date"]) if r["join_date"] is not None else None),
                "language": (str(r["language"]) if r["language"] is not None else None),
                "is_banned": int(r["is_banned"] or 0),
                "is_premium": int(r["is_premium"] or 0),
                "expiry_date": (str(r["expiry_date"]) if r["expiry_date"] is not None else None),
                "data_clear_count": int(r["data_clear_count"] or 0),
                "last_data_cleared_at": (str(r["last_data_cleared_at"]) if r["last_data_cleared_at"] is not None else None),
            }
        )
    return out


def clear_user_trades(db_path: str, user_id: int) -> int:
    """
    Deletes all trades for a user and logs it in `users`.
    Returns number of deleted rows.
    """
    with _connect(db_path) as conn:
        # Count first for reporting.
        row = conn.execute("SELECT COUNT(*) AS c FROM trades WHERE user_id=?;", (int(user_id),)).fetchone()
        deleted = int(row["c"] or 0)
        conn.execute("DELETE FROM trades WHERE user_id=?;", (int(user_id),))
        now = datetime.utcnow().isoformat()
        conn.execute(
            """
            UPDATE users
            SET data_clear_count = data_clear_count + 1,
                last_data_cleared_at = ?
            WHERE user_id=?;
            """,
            (now, int(user_id)),
        )
        return deleted


def active_users_for_motivation(db_path: str) -> list[tuple[int, str | None]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT user_id, language
            FROM users
            WHERE is_banned=0
            ORDER BY last_seen DESC;
            """
        ).fetchall()
    return [(int(r["user_id"]), (str(r["language"]) if r["language"] is not None else None)) for r in rows]


def count_trades_by_date(db_path: str, user_id: int, trade_date: str) -> int:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM trades
            WHERE user_id=? AND trade_date=?;
            """,
            (int(user_id), trade_date),
        ).fetchone()
        return int(row["c"])


def count_users(db_path: str) -> int:
    with _connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM users;").fetchone()
        return int(row["c"])


def count_trades(db_path: str) -> int:
    with _connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM trades;").fetchone()
        return int(row["c"])


def sum_profit(db_path: str) -> float:
    with _connect(db_path) as conn:
        row = conn.execute("SELECT COALESCE(SUM(profit), 0) AS s FROM trades;").fetchone()
        return float(row["s"])


def last_trade_time(db_path: str) -> str | None:
    with _connect(db_path) as conn:
        row = conn.execute("SELECT created_at FROM trades ORDER BY id DESC LIMIT 1;").fetchone()
        if not row:
            return None
        return str(row["created_at"])


def user_profit_summary(db_path: str, user_id: int) -> dict[str, Any]:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS trades,
                COALESCE(SUM(profit), 0) AS total_profit,
                COALESCE(SUM(qty), 0) AS total_qty,
                COALESCE(SUM(buy_price * qty), 0) AS sum_buy_w,
                COALESCE(SUM(sell_price * qty), 0) AS sum_sell_w
            FROM trades
            WHERE user_id=?;
            """,
            (user_id,),
        ).fetchone()

        trades = int(row["trades"])
        total_profit = float(row["total_profit"])
        total_qty = float(row["total_qty"])
        sum_buy_w = float(row["sum_buy_w"])
        sum_sell_w = float(row["sum_sell_w"])

    if total_qty > 0:
        avg_buy = sum_buy_w / total_qty
        avg_sell = sum_sell_w / total_qty
        roi = ((avg_sell - avg_buy) / avg_buy) * 100 if avg_buy != 0 else 0.0
    else:
        avg_buy = 0.0
        avg_sell = 0.0
        roi = 0.0

    return {
        "trades": trades,
        "total_profit": total_profit,
        "total_qty": total_qty,
        "avg_buy": avg_buy,
        "avg_sell": avg_sell,
        "roi": roi,
    }


def add_trade(
    db_path: str,
    user_id: int,
    trade_date: str,
    qty: float,
    buy_price: float,
    sell_price: float,
) -> None:
    # Profit is defined as (sell - buy) * qty
    profit = (sell_price - buy_price) * qty
    roi = ((sell_price - buy_price) / buy_price) * 100 if buy_price != 0 else 0.0
    created_at = datetime.utcnow().isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO trades(user_id, trade_date, created_at, qty, buy_price, sell_price, profit, roi)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, trade_date, created_at, qty, buy_price, sell_price, profit, roi),
        )


def last_trades(db_path: str, user_id: int, limit: int = 10, start_date: str | None = None) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        query = """
            SELECT trade_date, created_at, qty, buy_price, sell_price, profit, roi
            FROM trades
            WHERE user_id=?
        """
        params: list[Any] = [int(user_id)]
        if start_date:
            query += " AND trade_date >= ?"
            params.append(start_date)
        query += " ORDER BY id DESC LIMIT ?;"
        params.append(int(limit))

        rows = conn.execute(query, tuple(params)).fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "trade_date": str(r["trade_date"]),
                "created_at": str(r["created_at"]),
                "qty": float(r["qty"]),
                "buy_price": float(r["buy_price"]),
                "sell_price": float(r["sell_price"]),
                "profit": float(r["profit"]),
                "roi": float(r["roi"]),
            }
        )
    return out


def user_profit_summary_since(db_path: str, user_id: int, start_date: str) -> dict[str, Any]:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS trades,
                COALESCE(SUM(profit), 0) AS total_profit,
                COALESCE(SUM(qty), 0) AS total_qty,
                COALESCE(SUM(buy_price * qty), 0) AS sum_buy_w,
                COALESCE(SUM(sell_price * qty), 0) AS sum_sell_w
            FROM trades
            WHERE user_id=? AND trade_date >= ?;
            """,
            (int(user_id), start_date),
        ).fetchone()

        trades = int(row["trades"])
        total_profit = float(row["total_profit"])
        total_qty = float(row["total_qty"])
        sum_buy_w = float(row["sum_buy_w"])
        sum_sell_w = float(row["sum_sell_w"])

    if total_qty > 0:
        avg_buy = sum_buy_w / total_qty
        avg_sell = sum_sell_w / total_qty
        roi = ((avg_sell - avg_buy) / avg_buy) * 100 if avg_buy != 0 else 0.0
    else:
        avg_buy = 0.0
        avg_sell = 0.0
        roi = 0.0

    return {
        "trades": trades,
        "total_profit": total_profit,
        "total_qty": total_qty,
        "avg_buy": avg_buy,
        "avg_sell": avg_sell,
        "roi": roi,
    }


def trades_by_date(db_path: str, user_id: int, trade_date: str) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT qty, buy_price, sell_price, profit, roi, created_at
            FROM trades
            WHERE user_id=? AND trade_date=?
            ORDER BY id DESC;
            """,
            (user_id, trade_date),
        ).fetchall()
    return [
        {
            "qty": float(r["qty"]),
            "buy_price": float(r["buy_price"]),
            "sell_price": float(r["sell_price"]),
            "profit": float(r["profit"]),
            "roi": float(r["roi"]),
            "created_at": str(r["created_at"]),
        }
        for r in rows
    ]


def daily_profit_series(db_path: str, user_id: int, days: int = 30) -> list[DailyProfit]:
    today = date.today()
    start = today - timedelta(days=days - 1)
    start_iso = start.isoformat()
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT trade_date, COALESCE(SUM(profit), 0) AS profit
            FROM trades
            WHERE user_id=? AND trade_date >= ?
            GROUP BY trade_date
            ORDER BY trade_date ASC;
            """,
            (user_id, start_iso),
        ).fetchall()
    return [DailyProfit(trade_date=str(r["trade_date"]), profit=float(r["profit"])) for r in rows]


def best_worst_day(db_path: str, user_id: int, days: int = 30) -> tuple[DailyProfit | None, DailyProfit | None]:
    series = daily_profit_series(db_path, user_id, days=days)
    if not series:
        return None, None
    best = max(series, key=lambda x: x.profit)
    worst = min(series, key=lambda x: x.profit)
    return best, worst


def user_trades_range(db_path: str, user_id: int, start_date: str | None = None) -> list[dict[str, Any]]:
    query = """
        SELECT trade_date, created_at, qty, buy_price, sell_price, profit, roi
        FROM trades
        WHERE user_id=?
    """
    params: list[Any] = [user_id]
    if start_date:
        query += " AND trade_date >= ?"
        params.append(start_date)
    query += " ORDER BY id DESC"
    with _connect(db_path) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [
        {
            "trade_date": str(r["trade_date"]),
            "created_at": str(r["created_at"]),
            "qty": float(r["qty"]),
            "buy_price": float(r["buy_price"]),
            "sell_price": float(r["sell_price"]),
            "profit": float(r["profit"]),
            "roi": float(r["roi"]),
        }
        for r in rows
    ]


def all_users(db_path: str) -> list[tuple[int, str | None]]:
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT user_id, username FROM users;").fetchall()
    return [(int(r["user_id"]), (str(r["username"]) if r["username"] is not None else None)) for r in rows]

