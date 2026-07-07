# -*- coding: utf-8 -*-
"""
===================================
IbkrFetcher - Interactive Brokers 数据源 (Priority 4)
===================================

数据来源：IBKR TWS / IB Gateway（通过 ib_insync 库）
特点：交易所直连，数据质量最高；覆盖美股、港股
定位：美股/港股首选数据源（配置后自动提升优先级）

凭证：
- IBKR_HOST: TWS/Gateway 主机地址（默认 127.0.0.1）
- IBKR_PORT: TWS/Gateway 端口（默认 7497 模拟盘 / 7496 实盘）
- IBKR_CLIENT_ID: 客户端 ID（默认 1）
- IBKR_ACCOUNT: 账户号（可选，自动获取）

使用方式：
1. 启动 IB Gateway 或 TWS
2. 在 IB 设置中勾选 "Enable ActiveX and Socket Clients"
3. 配置环境变量后，该数据源自动生效
"""

import logging
import os
import time
import threading
from datetime import datetime, timedelta, date as date_type
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import numpy as np

from .base import BaseFetcher, STANDARD_COLUMNS
from .realtime_types import UnifiedRealtimeQuote, RealtimeSource, safe_float
from .us_index_mapping import is_us_stock_code

logger = logging.getLogger(__name__)

_IB_INSYNC_AVAILABLE = False
_IB_INSYNC_VERSION = ""

try:
    import ib_insync

    _IB_INSYNC_AVAILABLE = True
    try:
        _IB_INSYNC_VERSION = ib_insync.__version__
    except Exception:
        _IB_INSYNC_VERSION = "unknown"
except ImportError:
    logger.debug("ib_insync 未安装，IBKR 数据源不可用")


def _parse_env_int(key: str, default: int) -> int:
    val = os.getenv(key, "").strip()
    if not val:
        return default
    try:
        return int(val)
    except ValueError:
        logger.warning(f"{key}={val} 不是有效整数，使用默认值 {default}")
        return default


def _parse_env_float(key: str, default: float) -> float:
    val = os.getenv(key, "").strip()
    if not val:
        return default
    try:
        return float(val)
    except ValueError:
        logger.warning(f"{key}={val} 不是有效数字，使用默认值 {default}")
        return default


def _is_us_code(stock_code: str) -> bool:
    normalized = stock_code.strip().upper()
    return is_us_stock_code(normalized)


def _ibkr_symbol(stock_code: str) -> Optional[str]:
    """Convert internal stock code to IBKR contract symbol.

    Examples:
        AAPL   -> AAPL
        TSLA   -> TSLA
        HK00700 -> 0700
    """
    code = stock_code.strip()
    upper = code.upper()

    if _is_us_code(code):
        return upper

    if upper.startswith("HK"):
        digits = upper[2:].lstrip("0") or "0"
        return digits.zfill(4)

    return None


class IbkrFetcher(BaseFetcher):
    """
    Interactive Brokers 数据源

    优先级：
    - 配置有效连接后：Priority 0（最高优先级，美股用）
    - 未配置：Priority 99（不可用）

    支持的市场：
    - 美股 (STK, SMART, USD)
    - 港股 (STK, SEHK, HKD)
    """

    name = "IbkrFetcher"

    def __init__(self):
        self._ib: Optional[Any] = None
        self._lock = threading.RLock()
        self._config = self._load_config()

    @staticmethod
    def _load_config() -> Dict[str, Any]:
        host = os.getenv("IBKR_HOST", "").strip()
        return {
            "host": host or None,
            "port": _parse_env_int("IBKR_PORT", 7497),
            "client_id": _parse_env_int("IBKR_CLIENT_ID", 1),
            "account": os.getenv("IBKR_ACCOUNT", "").strip() or None,
            "connect_timeout": _parse_env_float("IBKR_CONNECT_TIMEOUT", 10.0),
            "request_timeout": _parse_env_float("IBKR_REQUEST_TIMEOUT", 30.0),
        }

    @property
    def priority(self) -> int:
        if _IB_INSYNC_AVAILABLE and self._is_available():
            return 0
        return 99

    @priority.setter
    def priority(self, value: int) -> None:
        pass

    def _is_available(self) -> bool:
        if not _IB_INSYNC_AVAILABLE:
            return False
        cfg = self._config
        if not cfg.get("host"):
            return False
        return True

    def _connect(self) -> Optional[Any]:
        with self._lock:
            if self._ib is not None and self._ib.isConnected():
                return self._ib

            if not self._is_available():
                return None

            cfg = self._config
            try:
                ib = ib_insync.IB()
                ib.connect(
                    host=cfg["host"],
                    port=cfg["port"],
                    clientId=cfg["client_id"],
                    timeout=cfg["connect_timeout"],
                )
                self._ib = ib
                logger.info(
                    "[IBKR] 连接成功: %s:%s (clientId=%s, version=%s)",
                    cfg["host"],
                    cfg["port"],
                    cfg["client_id"],
                    _IB_INSYNC_VERSION,
                )
                return ib
            except Exception as exc:
                logger.warning("[IBKR] 连接失败 %s:%s: %s", cfg["host"], cfg["port"], exc)
                self._ib = None
                return None

    def _disconnect(self) -> None:
        with self._lock:
            if self._ib is not None:
                try:
                    self._ib.disconnect()
                except Exception as exc:
                    logger.debug("[IBKR] 断开连接时异常: %s", exc)
                self._ib = None

    def _create_contract(self, stock_code: str) -> Optional[Any]:
        symbol = _ibkr_symbol(stock_code)
        if symbol is None:
            return None

        if _is_us_code(stock_code):
            contract = ib_insync.Stock(symbol, "SMART", "USD")
        elif stock_code.strip().upper().startswith("HK"):
            contract = ib_insync.Stock(symbol, "SEHK", "HKD")
        else:
            return None

        contract.includeExpired = False
        return contract

    def _qualify_contract(self, contract: Any) -> Optional[Any]:
        ib = self._connect()
        if ib is None:
            return None
        try:
            details = ib.reqContractDetails(contract)
            if details:
                return details[0].contract
            logger.warning("[IBKR] 未找到合约: %s", contract.symbol)
            return None
        except Exception as exc:
            logger.warning("[IBKR] 获取合约详情失败 %s: %s", contract.symbol, exc)
            self._disconnect()
            return None

    # ------------------------------------------------------------------
    # get_realtime_quote
    # ------------------------------------------------------------------

    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        if not self._is_available():
            return None

        contract = self._create_contract(stock_code)
        if contract is None:
            return None

        ib = self._connect()
        if ib is None:
            return None

        qualified = self._qualify_contract(contract)
        if qualified is None:
            return None

        try:
            ticker = ib.reqMktData(
                qualified,
                "",
                False,
                False,
            )
            ib.sleep(2)

            price = None
            if hasattr(ticker, "last") and ticker.last:
                price = safe_float(ticker.last)
            if price is None and hasattr(ticker, "close") and ticker.close:
                price = safe_float(ticker.close)
            if price is None:
                logger.debug("[IBKR] %s 无有效价格", stock_code)
                ib.cancelMktData(qualified)
                return None

            prev_close = safe_float(getattr(ticker, "close", None))
            if prev_close is None and hasattr(ticker, "prevClose"):
                prev_close = safe_float(ticker.prevClose)

            change_pct = None
            change_amount = None
            if prev_close and prev_close > 0:
                change_amount = round(price - prev_close, 4)
                change_pct = round((price - prev_close) / prev_close * 100, 2)

            open_price = safe_float(getattr(ticker, "open", None))
            high = safe_float(getattr(ticker, "high", None))
            low = safe_float(getattr(ticker, "low", None))

            volume_raw = getattr(ticker, "volume", None)
            volume = int(volume_raw) if volume_raw else None

            ib.cancelMktData(qualified)

            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=qualified.symbol,
                source=RealtimeSource.IBKR,
                price=price,
                change_pct=change_pct,
                change_amount=change_amount,
                volume=volume,
                amount=None,
                volume_ratio=None,
                turnover_rate=None,
                amplitude=None,
                open_price=open_price,
                high=high,
                low=low,
                pre_close=prev_close,
                pe_ratio=None,
                pb_ratio=None,
                total_mv=None,
                circ_mv=None,
            )

            logger.info(
                "[IBKR] %s 实时行情: 价格=%.2f, 涨跌幅=%s%%",
                stock_code,
                price,
                change_pct,
            )
            return quote

        except Exception as exc:
            logger.warning("[IBKR] 获取 %s 实时行情失败: %s", stock_code, exc)
            self._disconnect()
            return None

    # ------------------------------------------------------------------
    # 历史日线数据
    # ------------------------------------------------------------------

    def _fetch_raw_data(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        contract = self._create_contract(stock_code)
        if contract is None:
            logger.debug("[IBKR] %s 无法创建合约", stock_code)
            return pd.DataFrame()

        ib = self._connect()
        if ib is None:
            logger.debug("[IBKR] 无法连接")
            return pd.DataFrame()

        qualified = self._qualify_contract(contract)
        if qualified is None:
            return pd.DataFrame()

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        duration_days = (end_dt - start_dt).days + 1

        duration_str = self._duration_str(duration_days)
        bar_size = "1 day"
        what_to_show = "TRADES"
        use_rth = True

        try:
            bars = ib.reqHistoricalData(
                qualified,
                endDateTime=end_dt.strftime("%Y%m%d 23:59:59 US/Eastern"),
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow=what_to_show,
                useRTH=use_rth,
                formatDate=1,
                keepUpToDate=False,
            )
        except Exception as exc:
            logger.warning("[IBKR] 获取 %s 日线数据失败(%s~%s): %s", stock_code, start_date, end_date, exc)
            self._disconnect()
            return pd.DataFrame()

        if not bars:
            logger.debug("[IBKR] %s 返回 0 条日线记录", stock_code)
            return pd.DataFrame()

        rows = []
        for bar in bars:
            rows.append({
                "date": str(bar.date),
                "open": safe_float(bar.open),
                "high": safe_float(bar.high),
                "low": safe_float(bar.low),
                "close": safe_float(bar.close),
                "volume": int(bar.volume) if bar.volume else 0,
                "amount": safe_float(getattr(bar, "average", None)),
            })

        df = pd.DataFrame(rows)
        logger.info(
            "[IBKR] %s 日线数据获取成功: %s~%s, %d 条",
            stock_code,
            start_date,
            end_date,
            len(df),
        )
        return df

    @staticmethod
    def _duration_str(days: int) -> str:
        """Convert days to IBKR duration string."""
        if days <= 1:
            return "1 D"
        if days <= 7:
            return f"{days} D"
        if days <= 365:
            months = max(1, (days + 15) // 30)
            return f"{months} M"
        years = max(1, (days + 182) // 365)
        return f"{years} Y"

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=STANDARD_COLUMNS)

        if "amount" in df.columns:
            df = df.rename(columns={"amount": "amount_tmp"})

        if "close" in df.columns and "pct_chg" not in df.columns:
            df["pct_chg"] = df["close"].pct_change() * 100

        for col in STANDARD_COLUMNS:
            if col == "amount":
                continue
            if col not in df.columns:
                df[col] = None

        if "amount_tmp" in df.columns:
            df["amount"] = df["amount_tmp"]
            df = df.drop(columns=["amount_tmp"])

        result_cols = [c for c in STANDARD_COLUMNS if c in df.columns]
        return df[result_cols]

    def close(self) -> None:
        self._disconnect()

    def __del__(self) -> None:
        try:
            self._disconnect()
        except Exception:
            pass
