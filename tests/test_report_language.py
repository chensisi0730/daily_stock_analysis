# -*- coding: utf-8 -*-
"""Unit tests for report language helpers."""

import unittest

from src.report_language import (
    format_currency_amount,
    get_bias_status_emoji,
    get_currency_unit,
    get_localized_stock_name,
    get_sentiment_label,
    get_signal_level,
    localize_bias_status,
)


class ReportLanguageTestCase(unittest.TestCase):
    def test_get_signal_level_handles_compound_sell_advice(self) -> None:
        signal_text, emoji, signal_tag = get_signal_level("卖出/观望", 60, "zh")

        self.assertEqual(signal_text, "卖出")
        self.assertEqual(emoji, "🔴")
        self.assertEqual(signal_tag, "sell")

    def test_get_signal_level_handles_compound_buy_advice_in_english(self) -> None:
        signal_text, emoji, signal_tag = get_signal_level("Buy / Watch", 40, "en")

        self.assertEqual(signal_text, "Buy")
        self.assertEqual(emoji, "🟢")
        self.assertEqual(signal_tag, "buy")

    def test_get_localized_stock_name_replaces_placeholder_for_english(self) -> None:
        self.assertEqual(
            get_localized_stock_name("股票AAPL", "AAPL", "en"),
            "Unnamed Stock",
        )

    def test_get_sentiment_label_preserves_higher_band_thresholds(self) -> None:
        self.assertEqual(get_sentiment_label(80, "en"), "Very Bullish")
        self.assertEqual(get_sentiment_label(60, "en"), "Bullish")
        self.assertEqual(get_sentiment_label(40, "zh"), "中性")
        self.assertEqual(get_sentiment_label(20, "zh"), "悲观")

    def test_bias_status_helpers_support_english_values(self) -> None:
        self.assertEqual(localize_bias_status("Safe", "en"), "Safe")
        self.assertEqual(localize_bias_status("警戒", "en"), "Caution")
        self.assertEqual(get_bias_status_emoji("Safe"), "✅")
        self.assertEqual(get_bias_status_emoji("Caution"), "⚠️")

    def test_currency_unit_follows_market(self) -> None:
        self.assertEqual(get_currency_unit("cn", "zh"), "元")
        self.assertEqual(get_currency_unit("hk", "zh"), "港元")
        self.assertEqual(get_currency_unit("us", "zh"), "美元")
        self.assertEqual(get_currency_unit("us", "en"), "USD")
        # 未知市场/语言按 A 股兜底，保持历史输出不变
        self.assertEqual(get_currency_unit(None, "zh"), "元")
        self.assertEqual(get_currency_unit("jp", "zh"), "元")

    def test_format_currency_amount_uses_market_currency(self) -> None:
        self.assertEqual(format_currency_amount(3.1563e10, "us", "zh"), "315.63 亿美元")
        self.assertEqual(format_currency_amount(3.1563e10, "cn", "zh"), "315.63 亿元")
        self.assertEqual(format_currency_amount(3.1563e10, "hk", "zh"), "315.63 亿港元")
        self.assertEqual(format_currency_amount(5.6e4, "us", "zh"), "5.60 万美元")
        self.assertEqual(format_currency_amount(230, "us", "zh"), "230 美元")
        self.assertEqual(format_currency_amount(3.1563e10, "us", "en"), "31.56B USD")

    def test_format_currency_amount_handles_missing_values(self) -> None:
        self.assertEqual(format_currency_amount(None, "us", "zh"), "N/A")
        self.assertEqual(format_currency_amount("bad", "cn", "zh"), "N/A")


if __name__ == "__main__":
    unittest.main()
