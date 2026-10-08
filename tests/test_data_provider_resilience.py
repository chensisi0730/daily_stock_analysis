# -*- coding: utf-8 -*-
"""Regression tests for data-source resilience guards.

Covers the wall-clock timeout wrapper, the process-local circuit breaker and
the market-routing helpers added to stop a single unresponsive data source
from stalling the whole pipeline.
"""

import logging
import threading
import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_provider import base as base_mod
from data_provider.base import (
    DataFetcherManager,
    RateLimitError,
    _is_international_market,
    _market_tag,
    call_with_timeout,
    normalize_stock_code,
)


# ---------------------------------------------------------------------------
# call_with_timeout
# ---------------------------------------------------------------------------

def test_call_with_timeout_returns_value():
    assert call_with_timeout(lambda x: x * 3, timeout=5, x=7) == 21


def test_call_with_timeout_propagates_exception():
    def _boom():
        raise ValueError("upstream failed")

    with pytest.raises(ValueError, match="upstream failed"):
        call_with_timeout(_boom, timeout=5)


def test_call_with_timeout_raises_and_does_not_block():
    """A hung call must surface a TimeoutError instead of blocking the caller."""
    release = threading.Event()

    def _hang():
        release.wait(10)
        return "late"

    started = time.time()
    try:
        with pytest.raises(TimeoutError, match="未返回"):
            call_with_timeout(_hang, timeout=0.3)
        assert time.time() - started < 5
    finally:
        release.set()


def test_call_with_timeout_uses_hung_thread_as_daemon():
    """The timed-out worker must not keep the interpreter alive."""
    started = threading.Event()
    release = threading.Event()

    def _hang():
        started.set()
        release.wait(30)

    try:
        with pytest.raises(TimeoutError):
            call_with_timeout(_hang, timeout=0.2)
        assert started.wait(2)
        worker = next(t for t in threading.enumerate() if t.name == "fetcher-call")
        assert worker.daemon is True
    finally:
        release.set()
        time.sleep(0.3)
        assert not any(t.name == "fetcher-call" and t.is_alive() for t in threading.enumerate())


# ---------------------------------------------------------------------------
# circuit breaker
# ---------------------------------------------------------------------------

class _RecordingFetcher:
    def __init__(self, name: str, priority: int, error: Exception | None = None, result=None):
        self.name = name
        self.priority = priority
        self._error = error
        self._result = result
        self.calls = 0

    def get_daily_data(self, *args, **kwargs):
        self.calls += 1
        if self._error is not None:
            raise self._error
        return self._result

    def _is_available(self):
        return True


def _frame(index: int) -> pd.DataFrame:
    return pd.DataFrame(
        [{"date": "2026-10-07", "open": 1.0, "high": 1.0, "low": 1.0,
          "close": 1.0 + index, "volume": 1, "amount": 1.0, "pct_chg": 0.0}]
    )


def test_generic_loop_skips_source_without_credentials(caplog):
    """通用 fetcher 循环不应调用未配置凭据的数据源。

    回归背景：Longbridge 未填 key 时仍在通用循环里被调用，
    每只港股都白跑一次并刷一条 ``QuoteContext not available`` 失败日志。
    """
    unconfigured = _RecordingFetcher("LongbridgeFetcher", 0, error=RuntimeError("QuoteContext not available"))
    unconfigured._is_available = lambda: False
    healthy = _RecordingFetcher("TushareFetcher", 1, result=_frame(1))
    manager = DataFetcherManager(fetchers=[unconfigured, healthy])

    with caplog.at_level(logging.INFO):
        _, source = manager.get_daily_data("00700", start_date="2026-10-01", end_date="2026-10-07")

    assert source == "TushareFetcher"
    assert unconfigured.calls == 0
    assert "未配置凭据" in caplog.text


def test_generic_loop_honors_circuit_breaker():
    """通用循环同样要遵守熔断状态，否则坏源每只票都白等一次。"""
    bad = _RecordingFetcher("IbkrFetcher", 0, error=RuntimeError("connection refused"))
    healthy = _RecordingFetcher("TushareFetcher", 1, result=_frame(1))
    manager = DataFetcherManager(fetchers=[bad, healthy])

    for _ in range(manager.FETCHER_CIRCUIT_THRESHOLD):
        _, source = manager.get_daily_data("00700", start_date="2026-10-01", end_date="2026-10-07")
        assert source == "TushareFetcher"

    assert manager._is_fetcher_circuit_open("IbkrFetcher") is True
    calls_at_breaker = bad.calls

    manager.get_daily_data("00701", start_date="2026-10-01", end_date="2026-10-07")
    assert bad.calls == calls_at_breaker  # 冷却期内不再重试


def test_generic_loop_records_circuit_state():
    """通用循环的成功/失败也要喂给熔断器，否则熔断器在这里形同虚设。"""
    manager = DataFetcherManager(
        fetchers=[_RecordingFetcher("TushareFetcher", 1, result=_frame(1))]
    )

    manager.get_daily_data("600519", start_date="2026-10-01", end_date="2026-10-07")
    assert manager._fetcher_failure_streak == {}


def test_circuit_opens_after_repeated_failures_and_skips_source():
    """连续失败到阈值后才熔断，冷却期内该源被跳过。"""
    good = _RecordingFetcher("YfinanceFetcher", 4, result=_frame(1))
    bad = _RecordingFetcher("IbkrFetcher", 0, error=RuntimeError("connection refused"))
    manager = DataFetcherManager(fetchers=[bad, good])

    threshold = manager.FETCHER_CIRCUIT_THRESHOLD
    for _ in range(threshold):
        _, source = manager.get_daily_data("AAPL", start_date="2026-10-01", end_date="2026-10-07")
        assert source == "YfinanceFetcher"

    assert manager._is_fetcher_circuit_open("IbkrFetcher") is True
    assert bad.calls == threshold

    _, source_after = manager.get_daily_data("AAPL", start_date="2026-10-01", end_date="2026-10-07")
    assert source_after == "YfinanceFetcher"
    assert bad.calls == threshold  # 冷却期内不再重试


def test_circuit_threshold_not_one():
    """阈值必须是 >= 2：单次网络抖动不应把可用数据源整体熔断。

    回归背景：初版阈值为 1，yfinance 因一次 DNS 抖动失败后被熔断 600 秒，
    导致 UMC/PLTR/INTC/CRWV/AAPL/TSLA 全部被跳过，而 yfinance 随后本可正常取数。
    """
    assert DataFetcherManager.FETCHER_CIRCUIT_THRESHOLD >= 2


def test_single_transient_failure_does_not_cascade():
    """一次瞬时失败后数据源仍应被继续使用（不发生雪崩式级联跳过）。"""
    flaky = _RecordingFetcher("YfinanceFetcher", 4, error=ConnectionError("DNS 解析失败"))
    manager = DataFetcherManager(fetchers=[flaky])

    with pytest.raises(Exception):
        manager.get_daily_data("AAPL", start_date="2026-10-01", end_date="2026-10-07")

    assert manager._is_fetcher_circuit_open("YfinanceFetcher") is False
    assert flaky.calls == 1  # 下一只标的仍会尝试它


def test_circuit_success_resets_streak():
    """成功一次应清零连续失败计数，避免零星失败累积触发误熔断。"""
    manager = DataFetcherManager(fetchers=[])
    threshold = manager.FETCHER_CIRCUIT_THRESHOLD

    for _ in range(threshold - 1):
        manager._record_fetcher_failure("YfinanceFetcher")
    manager._record_fetcher_success("YfinanceFetcher")

    for _ in range(threshold - 1):
        manager._record_fetcher_failure("YfinanceFetcher")

    assert manager._is_fetcher_circuit_open("YfinanceFetcher") is False


def test_circuit_expires_after_cooldown():
    manager = DataFetcherManager(fetchers=[])
    for _ in range(manager.FETCHER_CIRCUIT_THRESHOLD):
        manager._record_fetcher_failure("YfinanceFetcher")
    assert manager._is_fetcher_circuit_open("YfinanceFetcher") is True

    manager._fetcher_circuit_until["YfinanceFetcher"] = time.time() - 1
    assert manager._is_fetcher_circuit_open("YfinanceFetcher") is False
    assert "YfinanceFetcher" not in manager._fetcher_failure_streak


def test_is_fetcher_available_respects_credential_check():
    class _NoCreds(_RecordingFetcher):
        def _is_available(self):
            return False

    manager = DataFetcherManager(fetchers=[_NoCreds("LongbridgeFetcher", 5)])
    assert manager._is_fetcher_available("LongbridgeFetcher") is False
    assert manager._is_fetcher_available("YfinanceFetcher") is False  # not registered


# ---------------------------------------------------------------------------
# international market routing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "code,expected",
    [
        ("005930.KS", True),
        ("2330.TW", True),
        ("D05.SI", True),
        ("SHOP.TO", True),
        ("BHP.AX", True),
        ("HSBA.L", True),
        ("AAPL", False),
        ("600519", False),
        ("00700.HK", False),
    ],
)
def test_is_international_market(code, expected):
    assert _is_international_market(code) is expected


def test_market_tag_intl():
    assert _market_tag("005930.KS") == "intl"
    assert _market_tag("AAPL") == "us"
    assert _market_tag("00700.HK") == "hk"
    assert _market_tag("600519") == "cn"


def test_normalize_stock_code_preserves_international_suffix():
    assert normalize_stock_code("005930.KS") == "005930.KS"
    assert normalize_stock_code("600519.SH") == "600519"


def test_intl_daily_data_routes_to_yfinance():
    fetcher = _RecordingFetcher("YfinanceFetcher", 4, result=_frame(2))
    manager = DataFetcherManager(fetchers=[fetcher])

    df, source = manager.get_daily_data(
        "005930.KS", start_date="2026-10-01", end_date="2026-10-07"
    )

    assert df is not None
    assert source == "YfinanceFetcher"
    assert fetcher.calls == 1


def test_intl_daily_data_failure_raises_data_fetch_error():
    from data_provider.base import DataFetchError

    fetcher = _RecordingFetcher("YfinanceFetcher", 4, error=RuntimeError("no such ticker"))
    manager = DataFetcherManager(fetchers=[fetcher])

    with pytest.raises(DataFetchError, match="国际市场"):
        manager.get_daily_data("2330.TW", start_date="2026-10-01", end_date="2026-10-07")


# ---------------------------------------------------------------------------
# quota downgrade + chip distribution gating
# ---------------------------------------------------------------------------

def test_quota_error_downgrades_fetcher_priority():
    from data_provider.base import DataFetchError, RateLimitError

    broken = _RecordingFetcher("EfinanceFetcher", 0, error=RateLimitError("quota exhausted"))
    fallback = _RecordingFetcher("YfinanceFetcher", 4, result=_frame(1))
    manager = DataFetcherManager(fetchers=[broken, fallback])

    df, source = manager.get_daily_data("600519", start_date="2026-10-01", end_date="2026-10-07")

    assert source == "YfinanceFetcher"
    assert broken.priority == 99


def test_quota_demotion_resorts_routing_order():
    """降级必须真正改变路由顺序，而不是只改一个没人再读的 priority 字段。

    回归背景：初版只写 ``fetcher.priority = 99``，但 ``_get_fetchers_snapshot()``
    返回的是已排好序列表的浅拷贝，之后再没重排，导致每个标的仍先打配额耗尽的源。
    """
    broken = _RecordingFetcher("EfinanceFetcher", 0, error=RateLimitError("quota exhausted"))
    healthy = _RecordingFetcher("TushareFetcher", 1, result=_frame(1))
    spare = _RecordingFetcher("YfinanceFetcher", 2, result=_frame(2))
    manager = DataFetcherManager(fetchers=[broken, healthy, spare])

    manager.get_daily_data("600519", start_date="2026-10-01", end_date="2026-10-07")

    assert [f.name for f in manager._get_fetchers_snapshot()] == [
        "TushareFetcher",
        "YfinanceFetcher",
        "EfinanceFetcher",
    ]

    # 后续标的直接走健康源，耗尽源不再被重复尝试
    _, source = manager.get_daily_data("600520", start_date="2026-10-01", end_date="2026-10-07")
    assert source == "TushareFetcher"
    assert broken.calls == 1


def test_quota_demotion_is_idempotent(caplog):
    """重复命中耗尽源时不应重复改值或重复刷降级日志。"""
    broken = _RecordingFetcher("EfinanceFetcher", 0, error=RateLimitError("quota exhausted"))
    healthy = _RecordingFetcher("TushareFetcher", 1, result=_frame(1))
    manager = DataFetcherManager(fetchers=[broken, healthy])

    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            manager.get_daily_data("600519", start_date="2026-10-01", end_date="2026-10-07")

    demotions = [r for r in caplog.records if "数据源降级" in r.getMessage()]
    assert len(demotions) == 1
    assert broken.priority == manager.QUOTA_DEMOTED_PRIORITY
    assert broken.calls == 1


@pytest.mark.parametrize("code", ["AAPL", "00700.HK", "005930.KS", "0700.HK"])
def test_chip_distribution_skipped_for_non_a_share_codes(code):
    manager = DataFetcherManager(fetchers=[])
    with patch.object(base_mod.logger, "debug") as mock_debug:
        assert manager.get_chip_distribution(code) is None
    assert any("非 A 股个股" in str(call) for call in mock_debug.call_args_list)


def test_chip_distribution_still_queries_data_sources_for_a_share_codes():
    """6 位 A 股代码（含 ETF）仍走原有数据源链路，不被新门槛误伤。"""
    fetcher = MagicMock()
    fetcher.name = "TushareFetcher"
    fetcher.get_chip_distribution.return_value = None
    manager = DataFetcherManager(fetchers=[fetcher])

    assert manager.get_chip_distribution("600519") is None
    fetcher.get_chip_distribution.assert_called_once_with("600519")


def test_get_main_indices_times_out_and_falls_back():
    """A hung data source must not stall get_main_indices; the next one takes over."""

    class _HangingFetcher(_RecordingFetcher):
        def get_main_indices(self, region=None):
            self.calls += 1
            time.sleep(5)
            return []

    hung = _HangingFetcher("HangingFetcher", 0)
    good = _RecordingFetcher("YfinanceFetcher", 4)
    good.get_main_indices = lambda region=None: [{"code": "000001", "name": "上证指数"}]
    manager = DataFetcherManager(fetchers=[hung, good])

    with patch.object(base_mod, "FETCHER_CALL_TIMEOUT_SECONDS", 0.5):
        started = time.time()
        result = manager.get_main_indices(region="cn")
        elapsed = time.time() - started

    assert result and result[0]["code"] == "000001"
    assert elapsed < 4.5


def test_get_main_indices_returns_none_when_all_sources_timeout():
    class _HangingFetcher(_RecordingFetcher):
        def get_main_indices(self, region=None):
            self.calls += 1
            time.sleep(5)
            return []

    manager = DataFetcherManager(fetchers=[_HangingFetcher("HangingFetcher", 0)])

    with patch.object(base_mod, "FETCHER_CALL_TIMEOUT_SECONDS", 0.3):
        started = time.time()
        result = manager.get_main_indices(region="cn")
        elapsed = time.time() - started

    assert not result  # 全部超时时返回空结果（既有契约），关键是快速返回
    assert elapsed < 4.5