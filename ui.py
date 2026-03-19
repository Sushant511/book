from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from i18n import get_first


def back_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⬅️ Back", callback_data="nav_back"), InlineKeyboardButton("❌ Cancel", callback_data="nav_cancel")],
        ]
    )


def _back_to_menu(lang: str = "en") -> list[InlineKeyboardButton]:
    return [InlineKeyboardButton(get_first(lang, "back_to_menu") or "⬅️ Back", callback_data="menu_home")]


def _noop_button() -> InlineKeyboardButton:
    # Decorative button to keep 2-per-row layout without cluttering UI.
    return InlineKeyboardButton("✨", callback_data="noop")


def main_menu_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "menu_trade") or "📊 Trade", callback_data="menu_trade"),
                InlineKeyboardButton(get_first(lang, "menu_stats") or "📈 Stats", callback_data="menu_stats"),
            ],
            [
                InlineKeyboardButton(get_first(lang, "menu_tools") or "⚡️ Tools", callback_data="menu_tools"),
                InlineKeyboardButton(get_first(lang, "menu_account") or "👤 Account", callback_data="menu_account"),
            ],
            [
                InlineKeyboardButton(get_first(lang, "menu_leaderboard") or "🏆 Leaderboard", callback_data="menu_leaderboard"),
                InlineKeyboardButton(get_first(lang, "menu_premium_short") or "💎 Premium", callback_data="menu_premium"),
            ],
        ]
    )


def trade_menu_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "menu_new_trade") or "📊 New Trade", callback_data="menu_new_trade"),
                InlineKeyboardButton(get_first(lang, "menu_history") or "📜 History", callback_data="menu_history"),
            ],
            [_back_to_menu(lang)[0], _noop_button()],
        ]
    )


def stats_menu_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "menu_analytics") or "📈 Analytics", callback_data="menu_analytics"),
                InlineKeyboardButton(get_first(lang, "menu_graph") or "📊 Graph", callback_data="menu_graph"),
            ],
            [
                InlineKeyboardButton(get_first(lang, "menu_reports") or "📊 Reports", callback_data="menu_reports"),
                InlineKeyboardButton(get_first(lang, "menu_leaderboard") or "🏆 Leaderboard", callback_data="menu_leaderboard"),
            ],
            [
                InlineKeyboardButton(get_first(lang, "menu_weekly") or "📅 Weekly", callback_data="menu_weekly"),
                InlineKeyboardButton(get_first(lang, "menu_leaderboard_settings") or "🏆 Leaderboard Settings", callback_data="menu_leaderboard_settings"),
            ],
            [_back_to_menu(lang)[0], _noop_button()],
        ]
    )


def tools_menu_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "menu_calculator") or "⚡️ Calculator", callback_data="menu_calculator"),
                InlineKeyboardButton(get_first(lang, "menu_escrow_analyzer") or "💎 Escrow Analyzer", callback_data="menu_escrow"),
            ],
            [_back_to_menu(lang)[0], _noop_button()],
        ]
    )


def account_menu_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "menu_premium") or "🔥 Premium", callback_data="menu_premium"),
                InlineKeyboardButton(get_first(lang, "menu_language") or "🌐 Language", callback_data="menu_language"),
            ],
            [
                InlineKeyboardButton(get_first(lang, "menu_help") or "❓ Help", callback_data="menu_help"),
                InlineKeyboardButton(get_first(lang, "menu_clear_data") or "🧹 Clear My Data", callback_data="menu_clear_data"),
            ],
            [_back_to_menu(lang)[0], _noop_button()],
        ]
    )


def premium_dashboard_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "premium_menu_insights") or "🔓 Insights", callback_data="premium_insights_menu"),
                InlineKeyboardButton(get_first(lang, "premium_menu_reports") or "📊 Reports", callback_data="premium_reports_menu"),
            ],
            [_back_to_menu(lang)[0], _noop_button()],
        ]
    )


def premium_insights_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "best_day_title") or "🏆 Best Day", callback_data="premium_insight_best"),
                InlineKeyboardButton(get_first(lang, "worst_day_title") or "📉 Worst Day", callback_data="premium_insight_worst"),
            ],
            [
                InlineKeyboardButton(get_first(lang, "win_rate_title") or "🎯 Win Rate", callback_data="premium_insight_winrate"),
                InlineKeyboardButton(get_first(lang, "streak_title") or "🔥 Streak", callback_data="premium_insight_streak"),
            ],
            [
                InlineKeyboardButton(get_first(lang, "total_volume_title") or "📦 Volume", callback_data="premium_insight_volume"),
                InlineKeyboardButton(get_first(lang, "back_to_menu") or "⬅️ Back", callback_data="menu_premium"),
            ],
        ]
    )


def premium_reports_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "premium_full_summary") or "📊 Full Summary", callback_data="premium_report_full"),
                InlineKeyboardButton(get_first(lang, "avg_daily_profit_title") or "📈 Avg Daily Profit", callback_data="premium_report_avg_daily"),
            ],
            [
                InlineKeyboardButton(get_first(lang, "total_profit_title") or "💰 Total Profit", callback_data="premium_report_total_profit"),
                InlineKeyboardButton(get_first(lang, "total_volume_title") or "📦 Total Volume", callback_data="premium_report_total_volume"),
            ],
            [
                InlineKeyboardButton(get_first(lang, "back_to_menu") or "⬅️ Back", callback_data="menu_premium"),
                _noop_button(),
            ],
        ]
    )


def trade_count_keyboard() -> InlineKeyboardMarkup:
    # Quick buttons for 1..5; user can also type 1..15.
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1", callback_data="tc_1"),
                InlineKeyboardButton("2", callback_data="tc_2"),
                InlineKeyboardButton("3", callback_data="tc_3"),
                InlineKeyboardButton("4", callback_data="tc_4"),
                InlineKeyboardButton("5", callback_data="tc_5"),
            ],
            [
                InlineKeyboardButton("⬅️ Back", callback_data="nav_back"),
                InlineKeyboardButton("❌ Cancel", callback_data="nav_cancel"),
            ],
        ]
    )


def result_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 New Trade", callback_data="menu_new_trade"),
                InlineKeyboardButton("📈 Analytics", callback_data="menu_analytics"),
            ],
            [
                InlineKeyboardButton("📜 History", callback_data="menu_history"),
                InlineKeyboardButton("📊 Graph", callback_data="menu_graph"),
            ],
            [
                InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_home"),
                _noop_button(),
            ],
        ]
    )


def export_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⬇️ Export Excel", callback_data="menu_export")],
            [InlineKeyboardButton("⬅️ Back", callback_data="nav_back"), InlineKeyboardButton("❌ Cancel", callback_data="nav_cancel")],
        ]
    )


def section_back_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back to Menu", callback_data="menu_home")]])


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇬🇧 English", callback_data="lang_en"),
                InlineKeyboardButton("🇮🇳 Hindi", callback_data="lang_hi"),
            ],
            [
                InlineKeyboardButton("🗣️ Hinglish", callback_data="lang_hinglish"),
                InlineKeyboardButton("🇨🇳 中文", callback_data="lang_zh"),
            ],
            [
                InlineKeyboardButton("❌ Cancel", callback_data="nav_cancel"),
            ],
        ]
    )


def consent_leaderboard_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Yes, show", callback_data="consent_show"),
                InlineKeyboardButton("🙅 No, keep private", callback_data="consent_private"),
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="nav_cancel")],
        ]
    )


def days_range_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("7", callback_data="range_7"),
                InlineKeyboardButton("15", callback_data="range_15"),
                InlineKeyboardButton("30", callback_data="range_30"),
            ],
            [
                InlineKeyboardButton("90", callback_data="range_90"),
                InlineKeyboardButton("365", callback_data="range_365"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="nav_back"), InlineKeyboardButton("❌ Cancel", callback_data="nav_cancel")],
        ]
    )


def escrow_result_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "escrow_recalculate") or "🔁 Recalculate", callback_data="menu_escrow"),
                InlineKeyboardButton(get_first(lang, "back_to_menu") or "⬅️ Back", callback_data="menu_tools"),
            ]
        ]
    )


def escrow_mode_keyboard(lang: str = "en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(get_first(lang, "escrow_buyer_pays") or "🟢 Buyer Pays Fee", callback_data="escrow_mode_buyer"),
                InlineKeyboardButton(get_first(lang, "escrow_seller_pays") or "🔵 Seller Pays Fee", callback_data="escrow_mode_seller"),
            ],
            [
                InlineKeyboardButton("⬅️ Back", callback_data="menu_tools"),
                InlineKeyboardButton("❌ Cancel", callback_data="nav_cancel"),
            ],
        ]
    )

