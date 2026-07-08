#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IBKR 数据源详细调试

测试内容：
1. 连接状态
2. 账户信息
3. 合约资格验证
4. 历史数据获取（不同时间段）
5. 实时行情
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

try:
    import ib_insync
    print(f"✅ ib_insync 版本: {ib_insync.__version__}")
except ImportError:
    print("❌ ib_insync 未安装")
    sys.exit(1)

host = os.getenv("IBKR_HOST", "127.0.0.1")
port = int(os.getenv("IBKR_PORT", "7497"))
cid = 100

print(f"\n连接参数: {host}:{port} (clientId={cid})")

# 创建 IB 实例
ib = ib_insync.IB()
ib.errorEvent += lambda reqId, errorCode, errorString, contract: \
    print(f"❌ IBKR Error [{reqId}] {errorCode}: {errorString}")

try:
    ib.connect(host, port, clientId=cid, timeout=15)
    print(f"\n✅ IBKR 连接成功!")
    print(f"   ib.isConnected(): {ib.isConnected()}")
    
    # 获取账户信息
    accounts = ib.managedAccounts()
    print(f"\n📊 账户信息:")
    print(f"   账户列表: {accounts}")
    
    # 获取当前时间
    current_time = ib.reqCurrentTime()
    print(f"   服务器时间: {current_time}")
    
    # 测试合约
    test_symbols = ["AAPL", "TSLA", "INTC"]
    
    for symbol in test_symbols:
        print(f"\n{'='*60}")
        print(f"测试合约: {symbol}")
        print(f"{'='*60}")
        
        contract = ib_insync.Stock(symbol, "SMART", "USD")
        
        # 合约资格验证
        try:
            details = ib.reqContractDetails(contract)
            if details:
                qualified = details[0].contract
                print(f"\n✅ 合约详情:")
                print(f"   symbol: {qualified.symbol}")
                print(f"   conId: {qualified.conId}")
                print(f"   exchange: {qualified.exchange}")
                print(f"   primaryExchange: {qualified.primaryExchange}")
                print(f"   currency: {qualified.currency}")
                
                # 测试历史数据（不同时间段）
                for days in [7, 30, 90]:
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=days)
                    
                    print(f"\n🔍 尝试获取 {days} 天历史数据 ({start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')})")
                    
                    try:
                        bars = ib.reqHistoricalData(
                            qualified,
                            endDateTime=end_date.strftime("%Y%m%d 23:59:59"),
                            durationStr=f"{days} D",
                            barSizeSetting="1 day",
                            whatToShow="TRADES",
                            useRTH=True,
                            formatDate=1,
                            keepUpToDate=False,
                        )
                        
                        if bars:
                            print(f"✅ 成功获取 {len(bars)} 条数据")
                            for bar in bars[-3:]:
                                print(f"   {bar.date}: O={bar.open:.2f} H={bar.high:.2f} L={bar.low:.2f} C={bar.close:.2f} V={bar.volume}")
                        else:
                            print(f"❌ 返回 0 条数据")
                            
                    except Exception as e:
                        print(f"❌ 获取历史数据失败: {e}")
                
                # 测试实时行情
                print(f"\n📈 测试实时行情:")
                try:
                    ticker = ib.reqMktData(qualified, "", False, False)
                    ib.sleep(3)
                    
                    if ticker.last:
                        print(f"✅ 最新价: ${ticker.last:.2f}")
                    if ticker.close:
                        print(f"   收盘价: ${ticker.close:.2f}")
                    if ticker.volume:
                        print(f"   成交量: {ticker.volume:,}")
                    
                    ib.cancelMktData(qualified)
                except Exception as e:
                    print(f"❌ 获取实时行情失败: {e}")
                
                # 测试基本面数据
                print(f"\n📊 测试基本面数据:")
                try:
                    report = ib.reqFundamentalData(qualified, "ReportSnapshot")
                    if report:
                        print(f"✅ 获取基本面数据成功")
                        print(f"   数据长度: {len(report)} 字符")
                        print(f"   前500字符:\n{report[:500]}...")
                        
                        try:
                            import xml.etree.ElementTree as ET
                            root = ET.fromstring(report)
                            print(f"\n   XML 解析成功，根标签: {root.tag}")
                            
                            all_tags = []
                            for field in root.iter():
                                tag = field.tag
                                text = field.text
                                if text and len(text) < 200:
                                    try:
                                        val = float(text)
                                        all_tags.append((tag, val))
                                    except ValueError:
                                        pass
                            
                            print(f"\n   找到 {len(all_tags)} 个数值字段，前30个:")
                            for tag, val in all_tags[:30]:
                                print(f"   {tag}: {val}")
                                
                            pe_fields = [(t, v) for t, v in all_tags if any(k in t for k in ["PE", "pe", "P/E", "PriceEarnings"])]
                            pb_fields = [(t, v) for t, v in all_tags if any(k in t for k in ["PB", "pb", "P/B", "PriceBook"])]
                            cap_fields = [(t, v) for t, v in all_tags if any(k in t for k in ["Cap", "cap", "Market", "market"])]
                            
                            print(f"\n   PE相关字段: {pe_fields}")
                            print(f"   PB相关字段: {pb_fields}")
                            print(f"   市值相关字段: {cap_fields[:10]}")
                            
                        except Exception as e:
                            print(f"   ⚠️ XML 解析失败: {e}")
                    else:
                        print(f"❌ 无基本面数据返回")
                except Exception as e:
                    print(f"❌ 获取基本面数据失败: {e}")
                    
            else:
                print(f"❌ 未找到合约详情")
        except Exception as e:
            print(f"❌ 合约资格验证失败: {e}")
    
    ib.disconnect()
    print(f"\n✅ 测试完成")
    
except Exception as e:
    print(f"\n❌ 连接失败: {e}")
    import traceback
    traceback.print_exc()
