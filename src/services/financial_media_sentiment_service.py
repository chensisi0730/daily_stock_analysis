# -*- coding: utf-8 -*-
"""
Financial Media Sentiment Intelligence Service

Aggregates financial news from major media outlets and Reddit financial
communities, computes sentiment score (-100 to +100) and discussion heat
(0-100 normalized).

Supports A-shares, HK stocks, and US stocks.
"""

import logging
import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.config import get_config
from src.search_service import SearchService

logger = logging.getLogger(__name__)


FINANCIAL_MEDIA_DOMAINS = {
    "zerohedge": ["zerohedge.com"],
    "bloomberg": ["bloomberg.com", "bloomberg.cn"],
    "reuters": ["reuters.com", "cn.reuters.com"],
    "financial_times": ["ft.com", "ftchinese.com"],
    "wall_street_journal": ["wsj.com", "cn.wsj.com"],
    "marketwatch": ["marketwatch.com"],
    "seeking_alpha": ["seekingalpha.com"],
    "yahoo_finance": ["finance.yahoo.com"],
    "cnbc": ["cnbc.com"],
    "business_insider": ["businessinsider.com"],
    "the_economist": ["economist.com"],
    "barrons": ["barrons.com"],
    "investing": ["investing.com", "cn.investing.com"],
}

REDDIT_FINANCIAL_SUBREDDITS = [
    "wallstreetbets",
    "stocks",
    "investing",
    "SecurityAnalysis",
    "ValueInvesting",
    "StockMarket",
    "pennystocks",
    "dividends",
    "options",
    "thetagang",
    "Shortsqueeze",
    "ChineseStocks",
    "HKStocks",
]

POSITIVE_KEYWORDS = [
    "bullish", "buy", "long", "upside", "growth", "strong", "beat", "exceed",
    "outperform", "upgrade", "positive", "optimistic", "rally", "surge",
    "soar", "jump", "gain", "profit", "earnings beat", "guidance raised",
    "record high", "breakthrough", "catalyst", "momentum", "accumulate",
    "看多", "买入", "增持", "上涨", "暴涨", "大涨", "突破", "利好", "强势",
    "业绩超预期", "上调", "看好", "买入评级", "目标价上调", "基本面向好",
    "估值修复", "反弹", "启动", "主升浪", "资金流入", "北向资金", "机构加仓",
]

NEGATIVE_KEYWORDS = [
    "bearish", "sell", "short", "downside", "decline", "weak", "miss",
    "underperform", "downgrade", "negative", "pessimistic", "crash", "plunge",
    "drop", "fall", "loss", "earnings miss", "guidance cut", "warning",
    "risk", "concern", "headwind", "pressure", "liquidate", "dump",
    "看空", "卖出", "减持", "下跌", "暴跌", "大跌", "破位", "利空", "弱势",
    "业绩不及预期", "下调", "看衰", "卖出评级", "目标价下调", "基本面恶化",
    "估值过高", "回调", "见顶", "主跌浪", "资金流出", "北向资金流出", "机构减仓",
]


@dataclass
class FinancialMediaSentiment:
    ticker: str
    sentiment_score: float
    discussion_heat: int
    mention_count: int
    sources: List[str]
    top_headlines: List[Dict[str, Any]]
    timestamp: str


class FinancialMediaSentimentService:
    """
    Financial Media Sentiment Intelligence Service.
    
    Usage:
        svc = FinancialMediaSentimentService()
        if svc.is_available:
            sentiment = svc.get_sentiment("AAPL")
            sentiment = svc.get_sentiment("600519")
            sentiment = svc.get_sentiment("hk00700")
    """
    
    _CACHE_TTL = 300
    
    def __init__(
        self,
        search_service: Optional[SearchService] = None,
        news_max_age_days: int = 3,
    ):
        self._search_service = search_service
        self._news_max_age_days = news_max_age_days
        self._cache: Dict[str, tuple] = {}
        self._cache_lock = threading.RLock()
        self._cache_inflight: Dict[str, threading.Event] = {}
    
    @property
    def is_available(self) -> bool:
        return self._search_service is not None and self._search_service.is_available
    
    def get_sentiment(self, ticker: str) -> Optional[FinancialMediaSentiment]:
        if not self.is_available:
            return None
        
        cache_key = self._normalize_ticker(ticker).upper()
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        sentiment = self._compute_sentiment(cache_key)
        if sentiment:
            self._set_cache(cache_key, sentiment)
        
        return sentiment
    
    def _normalize_ticker(self, ticker: str) -> str:
        t = ticker.strip().upper()
        if t.startswith(("SH", "SZ")):
            t = t[2:]
        elif t.startswith("HK"):
            t = "HK" + t[2:]
        return t
    
    def _get_search_queries(self, ticker: str) -> List[str]:
        queries = []
        normalized = self._normalize_ticker(ticker)
        stock_name = self._get_stock_name(normalized)
        
        if normalized.startswith("HK"):
            code = normalized[2:]
            queries.extend([
                f"{stock_name} {code}.HK",
                f"{stock_name} 港股",
                f"0{code}.HK" if len(code) == 4 else f"{code}.HK",
            ])
        elif normalized.isdigit() and len(normalized) == 6:
            queries.extend([
                f"{stock_name} {normalized}",
                f"{stock_name} A股",
                f"{normalized}.SS" if normalized.startswith("6") else f"{normalized}.SZ",
            ])
        else:
            queries.extend([
                f"{stock_name} {normalized}",
                f"{normalized} stock",
            ])
        
        media_queries = []
        for q in queries[:3]:
            for domain in ["zerohedge", "bloomberg", "reuters", "seeking_alpha"]:
                media_queries.append(f"{q} site:{domain}")
        
        reddit_queries = []
        for q in queries[:2]:
            reddit_queries.append(f"{q} reddit")
            reddit_queries.append(f"{q} wallstreetbets")
        
        all_queries = queries + media_queries[:8] + reddit_queries[:4]
        return list(dict.fromkeys(all_queries))[:12]
    
    def _get_stock_name(self, ticker: str) -> str:
        from src.data.stock_mapping import STOCK_NAME_MAP
        return STOCK_NAME_MAP.get(ticker, ticker)
    
    def _compute_sentiment(self, ticker: str) -> Optional[FinancialMediaSentiment]:
        queries = self._get_search_queries(ticker)
        
        all_results = []
        sources_found = set()
        
        for query in queries:
            try:
                response = self._search_service.search(
                    query=query,
                    max_results=10,
                    days=self._news_max_age_days,
                )
                if response.success and response.results:
                    all_results.extend(response.results)
                    for result in response.results:
                        source_domain = self._identify_source(result.url)
                        if source_domain:
                            sources_found.add(source_domain)
            except Exception as e:
                logger.debug(f"Search failed for query '{query}': {e}")
        
        if not all_results:
            return None
        
        seen_urls = set()
        unique_results = []
        for r in all_results:
            if r.url not in seen_urls:
                seen_urls.add(r.url)
                unique_results.append(r)
        
        sentiment_score = self._calculate_sentiment_score(unique_results)
        discussion_heat = self._calculate_discussion_heat(len(unique_results))
        top_headlines = self._extract_top_headlines(unique_results)
        
        return FinancialMediaSentiment(
            ticker=ticker,
            sentiment_score=sentiment_score,
            discussion_heat=discussion_heat,
            mention_count=len(unique_results),
            sources=sorted(list(sources_found)),
            top_headlines=top_headlines,
            timestamp=datetime.now().isoformat(),
        )
    
    def _identify_source(self, url: str) -> Optional[str]:
        url_lower = url.lower()
        for source_name, domains in FINANCIAL_MEDIA_DOMAINS.items():
            for domain in domains:
                if domain in url_lower:
                    return source_name
        if "reddit.com" in url_lower:
            for sub in REDDIT_FINANCIAL_SUBREDDITS:
                if f"r/{sub}" in url_lower or f"/r/{sub}" in url_lower:
                    return f"reddit_{sub}"
            return "reddit"
        return None
    
    def _calculate_sentiment_score(self, results: List) -> float:
        if not results:
            return 0.0
        
        total_score = 0.0
        total_weight = 0.0
        
        for result in results:
            text = f"{result.title} {result.snippet}".lower()
            
            pos_count = sum(1 for kw in POSITIVE_KEYWORDS if kw.lower() in text)
            neg_count = sum(1 for kw in NEGATIVE_KEYWORDS if kw.lower() in text)
            
            weight = 1.0
            source = self._identify_source(result.url)
            if source:
                if source in ["bloomberg", "reuters", "financial_times", "wall_street_journal"]:
                    weight = 1.5
                elif source in ["zerohedge", "cnbc", "marketwatch"]:
                    weight = 1.2
                elif source.startswith("reddit_"):
                    weight = 0.8
            
            if pos_count + neg_count > 0:
                score = (pos_count - neg_count) / (pos_count + neg_count) * 100 * weight
            else:
                score = 0.0
            
            total_score += score
            total_weight += weight
        
        avg_score = total_score / total_weight if total_weight > 0 else 0.0
        return max(-100.0, min(100.0, avg_score))
    
    def _calculate_discussion_heat(self, mention_count: int) -> int:
        if mention_count <= 0:
            return 0
        heat = min(100, int(10 * math.log10(mention_count + 1) * 10))
        return heat
    
    def _extract_top_headlines(self, results: List, max_items: int = 5) -> List[Dict[str, Any]]:
        headlines = []
        for r in results[:max_items]:
            source = self._identify_source(r.url)
            headlines.append({
                "title": r.title[:200],
                "url": r.url,
                "source": source or self._extract_domain(r.url),
                "snippet": r.snippet[:300],
                "date": r.published_date,
            })
        return headlines
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.replace('www.', '') or 'unknown'
        except Exception:
            return 'unknown'
    
    def _get_cached(self, cache_key: str) -> Optional[FinancialMediaSentiment]:
        now = time.monotonic()
        with self._cache_lock:
            cached = self._cache.get(cache_key)
            if cached and (now - cached[0]) < self._CACHE_TTL:
                return cached[1]
        return None
    
    def _set_cache(self, cache_key: str, sentiment: FinancialMediaSentiment) -> None:
        with self._cache_lock:
            self._cache[cache_key] = (time.monotonic(), sentiment)
    
    def format_for_prompt(self, sentiment: FinancialMediaSentiment) -> str:
        lines = [
            f"📰 Financial Media Sentiment for {sentiment.ticker}",
            "=" * 60,
            f"Sentiment Score: {sentiment.sentiment_score:+.1f}/100",
            f"Discussion Heat: {sentiment.discussion_heat}/100 ({sentiment.mention_count} mentions)",
            f"Sources: {', '.join(sentiment.sources) if sentiment.sources else 'General search'}",
            "",
            "Top Headlines:",
        ]
        
        for i, h in enumerate(sentiment.top_headlines, 1):
            date_str = f" ({h['date']})" if h['date'] else ""
            lines.append(f"  {i}. [{h['source']}] {h['title']}{date_str}")
            if h['snippet']:
                lines.append(f"     {h['snippet'][:150]}...")
        
        lines.append("")
        lines.append("Source: Major financial media + Reddit financial communities")
        return "\n".join(lines)


def create_financial_media_sentiment_service(
    search_service: Optional[SearchService] = None,
    news_max_age_days: int = 3,
) -> FinancialMediaSentimentService:
    return FinancialMediaSentimentService(
        search_service=search_service,
        news_max_age_days=news_max_age_days,
    )