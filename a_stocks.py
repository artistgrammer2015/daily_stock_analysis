"""
scan_m_w_pattern.py - 超稳定版（修复版）
核心优化：
- 增加akshare请求超时到120秒
- 降低并发数到1
- 每次请求间隔3-5秒
- 失败后等待更久（最长32秒）
- SQLite缓存股票信息（7天有效期）
"""

import akshare as ak
import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
import random
import socket
import sqlite3
from pathlib import Path

# ⭐ 设置全局socket超时
socket.setdefaulttimeout(120)

# ⭐ 猴子补丁：修改requests的默认超时
import requests.adapters
original_send = requests.adapters.HTTPAdapter.send

def send_with_timeout(self, request, *args, **kwargs):
    """为requests添加默认超时"""
    if kwargs.get('timeout') is None:
        kwargs['timeout'] = 120
    return original_send(self, request, *args, **kwargs)

requests.adapters.HTTPAdapter.send = send_with_timeout

# ⭐ 第3层：修改requests.get/post的默认行为
import requests.api
original_request = requests.api.request

def request_with_timeout(method, url, **kwargs):
    """强制所有请求使用120秒超时"""
    if 'timeout' not in kwargs or kwargs['timeout'] is None or kwargs['timeout'] < 120:
        kwargs['timeout'] = 120
    return original_request(method, url, **kwargs)

requests.api.request = request_with_timeout
requests.get = lambda url, **kwargs: request_with_timeout('get', url, **kwargs)
requests.post = lambda url, **kwargs: request_with_timeout('post', url, **kwargs)

# ================== 配置 ==================
TELEGRAM_BOT_TOKEN = "8472197175:AAEz6EXsvmEfDkdsZHpczY4v__ARy3AFGT0"
TELEGRAM_CHAT_ID = "6017808464"

START_DATE = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
END_DATE = datetime.today().strftime("%Y-%m-%d")

LOOKBACK = 120
MIN_GAP = 20
BREAKOUT_PCT = 0.01
RETEST_WINDOW = 30
RETEST_TOL_PCT = 0.08
BUY_POINT_PRICE_TOL = 0.08

MAX_WORKERS = 1

REQUEST_DELAY_MIN = 3.0
REQUEST_DELAY_MAX = 5.0
MAX_RETRIES = 5
RETRY_DELAY_BASE = 4

REQUEST_TIMEOUT = 120

TEST_SYMBOLS = []

# ================== 缓存配置 ==================
CACHE_DB_PATH = Path(__file__).parent / 'data' / 'stock_cache.db'
CACHE_DAYS = 7

# ⭐ 确保缓存目录存在
CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
# =========================================

# 全局计数器
_request_count = 0
_last_request_time = time.time()

def controlled_request():
    """控制请求速率：每分钟最多10次请求"""
    global _request_count, _last_request_time
    
    current_time = time.time()
    
    if current_time - _last_request_time > 60:
        _request_count = 0
        _last_request_time = current_time
    
    if _request_count >= 10:
        wait_time = 60 - (current_time - _last_request_time)
        if wait_time > 0:
            print(f"  ⏰ 请求限速中，等待 {wait_time:.1f} 秒...")
            time.sleep(wait_time)
            _request_count = 0
            _last_request_time = time.time()
    
    _request_count += 1
    
    delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
    print(f"  ⏱️  延迟 {delay:.1f} 秒...")
    time.sleep(delay)


def retry_on_error(max_retries=MAX_RETRIES, base_delay=RETRY_DELAY_BASE):
    """装饰器：自动重试失败的请求（指数退避）"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    controlled_request()
                    
                    result = func(*args, **kwargs)
                    return result
                    
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        requests.exceptions.ReadTimeout,
                        requests.exceptions.HTTPError,
                        ConnectionResetError,
                        ConnectionAbortedError,
                        socket.timeout,
                        Exception) as e:
                    
                    error_msg = str(e)[:200]
                    
                    if attempt < max_retries:
                        delay = base_delay * (2 ** (attempt - 1))
                        print(f"  ⚠️  请求失败 (第{attempt}/{max_retries}次)")
                        print(f"     错误: {error_msg}")
                        print(f"     等待 {delay} 秒后重试...")
                        time.sleep(delay)
                    else:
                        print(f"  ❌ 请求失败，已重试{max_retries}次")
                        print(f"     最后错误: {error_msg}")
                        return None
            
            return None
        
        return wrapper
    return decorator


def format_seal_time(time_str, date_str=None):
    """格式化封板时间显示"""
    if not time_str or time_str == '-' or len(str(time_str)) != 6:
        return time_str
    try:
        time_str = str(time_str).strip()
        if len(time_str) == 6:
            hour = time_str[0:2]
            minute = time_str[2:4]
            second = time_str[4:6]
            if date_str:
                return f"{date_str} {hour}:{minute}:{second}"
            else:
                return f"{hour}:{minute}:{second}"
    except:
        pass
    return time_str


def escape_markdown(text):
    """转义Markdown特殊字符"""
    if not text:
        return text
    result = str(text)
    result = result.replace('\\', '\\\\')
    result = result.replace('*', '\\*')
    result = result.replace('_', '\\_')
    result = result.replace('[', '\\[')
    result = result.replace(']', '\\]')
    result = result.replace('`', '\\`')
    return result


def send_telegram(text):
    """发送Telegram消息"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    
    try:
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(1)
        return True
    except Exception as e:
        print(f"  ⚠️  Telegram发送失败: {e}")
        return False


def send_long_message(title, items, prefix=""):
    """发送长消息（自动分段）"""
    if not items:
        return
    
    max_length = 4000
    current_msg = f"*{title}*\n\n"
    
    for item in items:
        item_text = f"{prefix}{item}\n"
        
        if len(current_msg) + len(item_text) > max_length:
            send_telegram(current_msg)
            time.sleep(1)
            current_msg = f"*{title} (续)*\n\n"
        
        current_msg += item_text
    
    if current_msg.strip() not in [f"*{title}*", f"*{title} (续)*"]:
        send_telegram(current_msg)


def code_with_prefix(code):
    """添加 sh/sz 前缀"""
    code = str(code)
    if code.startswith("6") or code.startswith("9") or code.startswith("5"):
        return "sh" + code
    else:
        return "sz" + code


def is_valid_stock_code(code):
    """过滤无效代码"""
    code = str(code)
    code_num = code[2:] if len(code) > 2 else code
    
    if code_num.startswith('920'):
        return False
    if code_num.startswith('000') and len(code_num) == 6:
        return False
    if code_num.startswith('399'):
        return False
    if code_num.startswith('8'):
        return False
    
    return True


def is_weekend(date_obj):
    """判断是否是周末"""
    return date_obj.weekday() >= 5


# ================== SQLite缓存部分 ==================

def init_cache_db():
    """初始化缓存数据库"""
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_info (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            sector TEXT,
            updated_at TEXT NOT NULL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cache_metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT NOT NULL
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"✓ 缓存数据库初始化完成: {CACHE_DB_PATH}")


def is_cache_valid():
    """检查缓存是否有效"""
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT value, updated_at FROM cache_metadata 
            WHERE key = 'last_update'
        ''')
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return False
        
        _, updated_at_str = result
        updated_at = datetime.fromisoformat(updated_at_str)
        now = datetime.now()
        
        is_valid = (now - updated_at).days < CACHE_DAYS
        
        if is_valid:
            print(f"✓ 缓存有效 (更新于 {updated_at.strftime('%Y-%m-%d %H:%M:%S')})")
        else:
            print(f"⚠️  缓存已过期 (更新于 {updated_at.strftime('%Y-%m-%d %H:%M:%S')}，有效期{CACHE_DAYS}天)")
        
        return is_valid
        
    except Exception as e:
        print(f"⚠️  检查缓存失败: {e}")
        return False


def save_stock_info_to_cache(stock_info_dict):
    """保存股票信息到缓存"""
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        cursor.execute('DELETE FROM stock_info')
        
        data = []
        for code, info in stock_info_dict.items():
            data.append((
                code,
                info.get('name', code),
                info.get('sector', '未知'),
                now
            ))
        
        cursor.executemany('''
            INSERT INTO stock_info (code, name, sector, updated_at)
            VALUES (?, ?, ?, ?)
        ''', data)
        
        cursor.execute('''
            INSERT OR REPLACE INTO cache_metadata (key, value, updated_at)
            VALUES ('last_update', ?, ?)
        ''', (now, now))
        
        cursor.execute('''
            INSERT OR REPLACE INTO cache_metadata (key, value, updated_at)
            VALUES ('stock_count', ?, ?)
        ''', (str(len(stock_info_dict)), now))
        
        conn.commit()
        conn.close()
        
        print(f"✅ 已缓存 {len(stock_info_dict)} 只股票信息")
        
    except Exception as e:
        print(f"❌ 缓存保存失败: {e}")


def load_stock_info_from_cache():
    """从缓存加载股票信息"""
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT code, name, sector FROM stock_info
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            print("⚠️  缓存为空")
            return None
        
        stock_info = {}
        for code, name, sector in rows:
            stock_info[code] = {
                'name': name,
                'sector': sector
            }
        
        print(f"✅ 从缓存加载 {len(stock_info)} 只股票信息")
        return stock_info
        
    except Exception as e:
        print(f"❌ 缓存加载失败: {e}")
        return None


def get_cache_info():
    """获取缓存信息"""
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT key, value, updated_at FROM cache_metadata
        ''')
        
        metadata = {}
        for key, value, updated_at in cursor.fetchall():
            metadata[key] = {
                'value': value,
                'updated_at': updated_at
            }
        
        cursor.execute('SELECT COUNT(*) FROM stock_info')
        stock_count = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'stock_count': stock_count,
            'metadata': metadata
        }
        
    except Exception as e:
        print(f"⚠️  获取缓存信息失败: {e}")
        return None


def clear_cache():
    """清空缓存"""
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM stock_info')
        cursor.execute('DELETE FROM cache_metadata')
        
        conn.commit()
        conn.close()
        
        print("✅ 缓存已清空")
        
    except Exception as e:
        print(f"❌ 清空缓存失败: {e}")


@retry_on_error()
def fetch_stock_info_from_api():
    """从API获取股票信息（带重试）"""
    print("正在从API获取股票基本信息...")
    print("  ⏳ 这可能需要1-2分钟，请耐心等待...")
    print("  ⏳ 超时设置: 120秒")
    
    df = ak.stock_zh_a_spot_em()
    
    if df is None or df.empty:
        print("  ❌ 无法获取股票列表")
        return {}
    
    stock_info = {}
    
    for _, row in df.iterrows():
        try:
            code = str(row.get('代码', row.get('code', '')))
            name = str(row.get('名称', row.get('name', code)))
            sector = str(row.get('所属行业', row.get('sector', '未知')))
            
            if code:
                stock_info[code] = {
                    'name': name,
                    'sector': sector
                }
        except:
            continue
    
    print(f"✅ 获取到 {len(stock_info)} 只股票信息")
    return stock_info


def get_stock_info_dict(force_refresh=False):
    """获取股票信息字典（优先使用缓存）"""
    print("\n" + "="*60)
    print("📊 获取股票基本信息")
    print("="*60)
    
    if not CACHE_DB_PATH.exists():
        print("⚠️  缓存数据库不存在，首次初始化...")
        init_cache_db()
    else:
        init_cache_db()
    
    cache_info = get_cache_info()
    if cache_info:
        print(f"📂 缓存状态:")
        if cache_info['metadata'].get('last_update'):
            last_update = cache_info['metadata']['last_update']['updated_at']
            last_update_dt = datetime.fromisoformat(last_update)
            days_ago = (datetime.now() - last_update_dt).days
            print(f"   上次更新: {last_update_dt.strftime('%Y-%m-%d %H:%M:%S')} ({days_ago}天前)")
        print(f"   缓存数量: {cache_info['stock_count']} 只")
        print(f"   有效期: {CACHE_DAYS} 天")
    
    if force_refresh:
        print("\n⚡ 强制刷新缓存...")
        stock_info = fetch_stock_info_from_api()
        if stock_info:
            save_stock_info_to_cache(stock_info)
        return stock_info
    
    if is_cache_valid():
        stock_info = load_stock_info_from_cache()
        if stock_info:
            return stock_info
    
    print("\n🌐 从API获取最新数据...")
    stock_info = fetch_stock_info_from_api()
    
    if stock_info:
        save_stock_info_to_cache(stock_info)
    
    return stock_info


# ================== 主要功能函数 ==================

@retry_on_error()
def get_market_stats():
    """获取全市场涨跌统计"""
    print("\n正在获取全市场涨跌统计...")
    print("  ⏳ 这可能需要1-2分钟，请耐心等待...")
    print("  ⏳ 超时设置: 120秒")
    
    df = ak.stock_zh_a_spot_em()
    
    if df is None or df.empty:
        return None
    
    change_col = None
    for col in df.columns:
        if '涨跌幅' in col or 'change' in col.lower() or '涨幅' in col:
            change_col = col
            break
    
    if change_col is None:
        print("⚠️  未找到涨跌幅列")
        return None
    
    valid_codes = []
    for _, row in df.iterrows():
        code = str(row.get('代码', row.get('code', '')))
        if is_valid_stock_code(code):
            valid_codes.append(code)
    
    df_filtered = df[df['代码'].isin(valid_codes) if '代码' in df.columns else df['code'].isin(valid_codes)]
    changes = df_filtered[change_col].astype(float)
    
    stats = {
        'up': int((changes > 0).sum()),
        'down': int((changes < 0).sum()),
        'flat': int((changes == 0).sum()),
        'total': int(len(changes))
    }
    
    print(f"✅ 市场统计: 涨{stats['up']} 跌{stats['down']} 平{stats['flat']} 总{stats['total']}")
    return stats


@retry_on_error()
def get_sector_fund_flow_rank(indicator='今日', sector_type='概念资金流', top_n=20):
    """获取板块资金流入排行榜"""
    print(f"\n正在获取板块资金流入排行榜 ({sector_type}, {indicator})...")
    print("  ⏳ 这可能需要30-60秒，请耐心等待...")
    print("  ⏳ 超时设置: 120秒")
    
    df = ak.stock_sector_fund_flow_rank(indicator=indicator, sector_type=sector_type)
    
    if df is None or df.empty:
        print("⚠️  未获取到板块资金流数据")
        return None
    
    fund_col = None
    for col in df.columns:
        if '主力净流入' in col and '净额' in col:
            fund_col = col
            break
    
    if fund_col is None:
        print("⚠️  未找到资金流入列")
        return None
    
    df = df.sort_values(by=fund_col, ascending=False).reset_index(drop=True)
    
    if top_n > 0:
        df = df.head(top_n)
    
    if fund_col in df.columns:
        df['资金流入(亿元)'] = df[fund_col] / 100000000
    
    print(f"✅ 获取到 {len(df)} 个板块的资金流数据")
    return df


def send_sector_fund_flow_message(df, stock_info_dict, indicator='今日', sector_type='概念资金流', collect_top_stocks=False):
    """发送板块资金流入排行榜消息到Telegram（支持名称反查代码）
    
    Args:
        df: 板块资金流数据
        stock_info_dict: 股票信息字典
        indicator: 时间周期（今日/5日/10日）
        sector_type: 板块类型
        collect_top_stocks: 是否收集前10名板块的领涨股
    """
    if df is None or df.empty:
        return

    top_stocks_collected = set()

    try:
        # 创建名称到代码的反向映射
        print("\n🔍 建立股票名称到代码的映射...")
        name_to_code = {}
        for code, info in stock_info_dict.items():
            name = info.get('name', '')
            if name:
                name_to_code[name] = code
        
        print(f"✓ 已建立 {len(name_to_code)} 个名称映射")
        
        prefix = indicator
        
        fund_col = None
        for col in df.columns:
            if '主力净流入' in col and '净额' in col:
                fund_col = col
                break
        
        if fund_col is None:
            print("⚠️  未找到资金流入列")
            return
        
        change_col = f'{prefix}涨跌幅'
        ratio_col = f'{prefix}主力净流入-净占比'
        top_stock_col = f'{prefix}主力净流入最大股'
        
        if change_col not in df.columns:
            for col in df.columns:
                if '涨跌幅' in col:
                    change_col = col
                    break
        
        if ratio_col not in df.columns:
            for col in df.columns:
                if '主力净流入' in col and '净占比' in col:
                    ratio_col = col
                    break
        
        if top_stock_col not in df.columns:
            for col in df.columns:
                if '主力净流入最大股' in col or '最大股' in col:
                    top_stock_col = col
                    break
        
        message_lines = [
            f"*💰 {sector_type}排行榜 ({indicator})*",
            f"",
        ]
        
        # ⭐ 添加调试信息
        if collect_top_stocks:
            print(f"\n🔍 开始解析前10名板块的领涨股...")
        
        for idx, row in df.iterrows():
            name = str(row.get('名称', row.get('name', '')))
            
            try:
                change_pct = float(row.get(change_col, row.get('涨跌幅', 0)))
            except (ValueError, TypeError):
                change_pct = 0
            
            try:
                fund_inflow = float(row.get(fund_col, 0)) / 100000000
            except (ValueError, TypeError):
                fund_inflow = 0
            
            try:
                fund_ratio = float(row.get(ratio_col, row.get('主力净流入-净占比', 0)))
            except (ValueError, TypeError):
                fund_ratio = 0
            
            top_stock = row.get(top_stock_col, '-')

            # ⭐ 只收集前10名板块的领涨股
            if collect_top_stocks and idx < 10 and top_stock and str(top_stock).strip() not in ['-', '', 'None', 'nan', 'NaN']:
                stock_code_raw = str(top_stock).strip()
                
                print(f"  板块 #{idx+1}: {name}")
                print(f"    原始领涨股: {stock_code_raw}")
                
                code = None
                
                # 策略1: 尝试从字符串中提取6位数字代码
                if '(' in stock_code_raw and ')' in stock_code_raw:
                    parts = stock_code_raw.split('(')
                    part1 = parts[0].strip()
                    part2 = parts[1].split(')')[0].strip()
                    
                    if len(part1) == 6 and part1.isdigit():
                        code = part1
                    elif len(part2) == 6 and part2.isdigit():
                        code = part2
                
                elif len(stock_code_raw) == 6 and stock_code_raw.isdigit():
                    code = stock_code_raw
                
                elif ' ' in stock_code_raw:
                    parts = stock_code_raw.split()
                    for part in parts:
                        if len(part) == 6 and part.isdigit():
                            code = part
                            break
                
                elif any(c.isdigit() for c in stock_code_raw):
                    digits = ''.join(filter(str.isdigit, stock_code_raw))
                    if len(digits) >= 6:
                        if digits[:6] not in top_stocks_collected:
                            code = digits[:6]
                        elif len(digits) >= 6 and digits[-6:] not in top_stocks_collected:
                            code = digits[-6:]
                
                # 策略2: 通过名称反查
                if not code:
                    if stock_code_raw in name_to_code:
                        code = name_to_code[stock_code_raw]
                    else:
                        clean_name = stock_code_raw.split('(')[0].strip() if '(' in stock_code_raw else stock_code_raw
                        
                        if clean_name in name_to_code:
                            code = name_to_code[clean_name]
                        else:
                            matches = []
                            for n, c in name_to_code.items():
                                if clean_name in n or n in clean_name:
                                    matches.append((n, c))
                            
                            if len(matches) >= 1:
                                code = matches[0][1]
                
                if code and len(code) == 6 and code.isdigit():
                    if code not in top_stocks_collected:
                        top_stocks_collected.add(code)
                        print(f"    ✓ 解析成功: {code}")
                    else:
                        print(f"    ⚠️  重复股票: {code} (已存在)")
                else:
                    print(f"    ✗ 解析失败: 未找到有效代码")
            
            elif collect_top_stocks and idx < 10:
                # 前10名但没有领涨股数据
                print(f"  板块 #{idx+1}: {name}")
                print(f"    ⚠️  无领涨股数据: {top_stock}")

            change_str = f"+{change_pct:.2f}%" if change_pct >= 0 else f"{change_pct:.2f}%"
            
            if abs(fund_inflow) >= 1:
                fund_str = f"{fund_inflow:.2f}亿"
            else:
                fund_str = f"{fund_inflow * 10000:.0f}万"
            
            if pd.notna(fund_ratio) and fund_ratio != 0:
                ratio_str = f"{fund_ratio:.2f}%"
            else:
                ratio_str = "-"
            
            message_lines.append(
                f"{idx + 1}. *{name}*\n"
                f"   资金流入: {fund_str} ({ratio_str}) | 涨跌幅: {change_str}\n"
                f"   领涨股: {top_stock}"
            )
            message_lines.append("")
        
        message = "\n".join(message_lines)
        
        # ⭐ 添加解析总结
        if collect_top_stocks:
            print(f"\n📊 领涨股收集总结:")
            print(f"   成功收集: {len(top_stocks_collected)} 只")
            if len(top_stocks_collected) < 10:
                print(f"   ⚠️  预期10只，实际{len(top_stocks_collected)}只（可能原因：数据缺失、解析失败或重复股票）")
        
        if len(message) > 4000:
            items = []
            for idx, row in df.iterrows():
                name = str(row.get('名称', row.get('name', '')))
                
                try:
                    change_pct = float(row.get(change_col, row.get('涨跌幅', 0)))
                except (ValueError, TypeError):
                    change_pct = 0
                
                try:
                    fund_inflow = float(row.get(fund_col, 0)) / 100000000
                except (ValueError, TypeError):
                    fund_inflow = 0
                
                try:
                    fund_ratio = float(row.get(ratio_col, row.get('主力净流入-净占比', 0)))
                except (ValueError, TypeError):
                    fund_ratio = 0
                
                top_stock = row.get(top_stock_col, '-')
                
                change_str = f"+{change_pct:.2f}%" if change_pct >= 0 else f"{change_pct:.2f}%"
                if abs(fund_inflow) >= 1:
                    fund_str = f"{fund_inflow:.2f}亿"
                else:
                    fund_str = f"{fund_inflow * 10000:.0f}万"
                
                if pd.notna(fund_ratio) and fund_ratio != 0:
                    ratio_str = f"{fund_ratio:.2f}%"
                else:
                    ratio_str = "-"
                
                items.append(f"{idx + 1}. {name} | {fund_str} ({ratio_str}) | {change_str} | {top_stock}")
            
            send_long_message(f"💰 {sector_type}排行榜 ({indicator})", items, "")
        else:
            send_telegram(message)
        
        print(f"\n✅ 已发送板块资金流排行榜消息到Telegram")

        # ⭐ 只在收集了领涨股时才写入.env文件
        if collect_top_stocks and top_stocks_collected:
            try:
                print(f"\n📝 开始更新 .env 文件...")
                print(f"   从前10名板块收集到 {len(top_stocks_collected)} 只领涨股")
                
                env_path = Path(__file__).parent / '.env'
                
                if not env_path.exists():
                    env_path.write_text("# A股智能分析系统配置\n", encoding='utf-8')

                content = env_path.read_text(encoding='utf-8')
                lines = content.split('\n')
                
                # 查找 STOCK_LIST_FIXED
                fixed_stocks = []
                stock_list_fixed_line_idx = -1
                for i, line in enumerate(lines):
                    if line.strip().startswith('STOCK_LIST_FIXED='):
                        stock_list_fixed_line_idx = i
                        fixed_line = line.strip()
                        if '=' in fixed_line:
                            _, fixed_stocks_str = fixed_line.split('=', 1)
                            fixed_stocks_str = fixed_stocks_str.strip()
                            if fixed_stocks_str:
                                fixed_stocks = [code.strip() for code in fixed_stocks_str.split(',') if code.strip()]
                        break
                
                print(f"   从 STOCK_LIST_FIXED 读取到 {len(fixed_stocks)} 只固定股票")
                
                # 合并：今日前10名板块领涨股 + STOCK_LIST_FIXED
                new_stocks = list(top_stocks_collected)
                new_stocks.extend([code for code in fixed_stocks if code not in top_stocks_collected])
                new_value = ','.join(sorted(new_stocks))
                
                # 更新或添加 STOCK_LIST
                stock_list_line_idx = -1
                for i, line in enumerate(lines):
                    if line.strip().startswith('STOCK_LIST='):
                        stock_list_line_idx = i
                        break
                
                if stock_list_line_idx >= 0:
                    lines[stock_list_line_idx] = f"STOCK_LIST={new_value}"
                else:
                    lines.append(f"STOCK_LIST={new_value}")

                new_content = '\n'.join(lines)
                if not new_content.endswith('\n'):
                    new_content += '\n'

                env_path.write_text(new_content, encoding='utf-8')
                print(f"✅ .env 文件更新成功！")
                print(f"   ├─ 前10名板块领涨股: {len(top_stocks_collected)} 只")
                print(f"   ├─ STOCK_LIST_FIXED: {len(fixed_stocks)} 只")
                print(f"   └─ STOCK_LIST 总数: {len(new_stocks)} 只")
                if top_stocks_collected:
                    print(f"   领涨股代码: {', '.join(sorted(top_stocks_collected))}")

            except Exception as e:
                print(f"❌ 写入.env文件失败: {e}")

    except Exception as e:
        print(f"❌ 发送板块资金流排行榜消息失败: {e}")
        import traceback
        traceback.print_exc()



def get_continuous_limit_up_stocks(stock_info_dict):
    """获取连续两个交易日涨停的股票（不获取成交量）"""
    try:
        today = datetime.today()
        if is_weekend(today):
            print(f"\n⚠️  今天是{['周一','周二','周三','周四','周五','周六','周日'][today.weekday()]}，周末没有交易数据")
            return {}, None
        
        print("="*60)
        print("📊 开始统计连续涨停个股...")
        print("="*60)
        
        zt_data_list = []
        
        @retry_on_error()
        def fetch_zt_data(date_str):
            return ak.stock_zt_pool_em(date=date_str)
        
        for days_ago in range(1, 15):
            if len(zt_data_list) >= 3:
                break
            
            try_date = datetime.today() - timedelta(days=days_ago)
            
            if is_weekend(try_date):
                continue
            
            date_str = try_date.strftime("%Y%m%d")
            date_display = try_date.strftime("%Y-%m-%d %A")
            
            print(f"\n尝试获取 {date_display} 的涨停板数据...")
            df = fetch_zt_data(date_str)
            
            if df is not None and not df.empty and len(df) > 0:
                if '代码' in df.columns:
                    codes = df['代码'].astype(str).tolist()
                elif 'code' in df.columns:
                    codes = df['code'].astype(str).tolist()
                else:
                    codes = df.iloc[:, 0].astype(str).tolist()
                
                codes = [c for c in codes if is_valid_stock_code(c)]
                
                if len(codes) > 0:
                    print(f"✅ 找到 {len(codes)} 只涨停股")
                    
                    zt_data_list.append({
                        'date': try_date,
                        'date_str': date_str,
                        'date_display': date_display,
                        'codes': set(codes),
                        'df': df
                    })
        
        if len(zt_data_list) < 2:
            print("\n❌ 无法获取足够的涨停板数据")
            return {}, None
        
        zt_data_list.sort(key=lambda x: x['date'], reverse=True)
        
        day1 = zt_data_list[0]
        day2 = zt_data_list[1]
        
        continuous_codes = day1['codes'].intersection(day2['codes'])
        
        print(f"\n{'='*60}")
        print(f"📈 连续涨停统计结果:")
        print(f"{'='*60}")
        print(f"{day2['date_display']}: {len(day2['codes'])} 只涨停")
        print(f"{day1['date_display']}: {len(day1['codes'])} 只涨停")
        print(f"连续涨停: {len(continuous_codes)} 只")
        
        if len(continuous_codes) == 0:
            return {}, zt_data_list
        
        print(f"\n⚠️  跳过成交量获取（已禁用）")
        
        sector_groups = {}
        
        for code in continuous_codes:
            name = code
            sector = "未知"

            if 'df' in day1 and day1['df'] is not None:
                stock_row = day1['df'][day1['df']['代码'] == code]
                if not stock_row.empty:
                    row = stock_row.iloc[0]
                    name = str(row.get('名称', code))
                    sector = str(row.get('所属行业', '未知'))

            if name == code and isinstance(stock_info_dict.get(code), dict):
                name = stock_info_dict[code].get('name', code)
                sector = stock_info_dict[code].get('sector', sector)

            stock_detail = {'首次封板时间': '-', '最后封板时间': '-', '炸板次数': 0,
                          '涨停统计': '-', '连板数': 0, '总市值': 0, '所属行业': sector}

            if 'df' in day1 and day1['df'] is not None:
                stock_row = day1['df'][day1['df']['代码'] == code]
                if not stock_row.empty:
                    row = stock_row.iloc[0]
                    stock_detail.update({
                        '首次封板时间': str(row.get('首次封板时间', '-')),
                        '最后封板时间': str(row.get('最后封板时间', '-')),
                        '炸板次数': int(row.get('炸板次数', 0)),
                        '涨停统计': str(row.get('涨停统计', '-')),
                        '连板数': int(row.get('连板数', 0)),
                        '总市值': float(row.get('总市值', 0)),
                        '所属行业': str(row.get('所属行业', sector))
                    })

            if stock_detail['所属行业'] not in sector_groups:
                sector_groups[stock_detail['所属行业']] = []

            sector_groups[stock_detail['所属行业']].append({
                'code': code,
                'name': name,
                **stock_detail
            })
        
        return sector_groups, zt_data_list
        
    except Exception as e:
        print(f"❌ 统计连续涨停失败: {e}")
        import traceback
        traceback.print_exc()
        return {}, None


def send_continuous_limit_up_message(sector_groups, zt_data_list, market_stats):
    """发送连续涨停个股的Telegram消息（不显示成交量）"""
    if not sector_groups:
        return
    
    try:
        day1 = zt_data_list[0]
        day2 = zt_data_list[1]
        
        total_count = sum(len(stocks) for stocks in sector_groups.values())
        
        message_lines = [
            f"*📊 连续两日涨停个股 ({total_count}只)*",
            f"",
            f"📅 {day2['date'].strftime('%m月%d日')} → {day1['date'].strftime('%m月%d日')}",
        ]
        
        if market_stats:
            message_lines.append(f"")
            message_lines.append(f"*📈 全市场统计:*")
            message_lines.append(f"涨: {market_stats['up']}只 | 跌: {market_stats['down']}只 | 平: {market_stats['flat']}只")
            message_lines.append(f"总计: {market_stats['total']}只")
        
        message_lines.append(f"")
        
        sorted_sectors = sorted(sector_groups.items(), key=lambda x: len(x[1]), reverse=True)
        
        for sector, stocks in sorted_sectors:
            message_lines.append(f"*{sector}* ({len(stocks)}只):")
            
            for stock in stocks:
                data_date = day1['date'].strftime('%Y-%m-%d') if day1 else None
                first_seal_time = format_seal_time(stock.get('首次封板时间', '-'), data_date)
                last_seal_time = format_seal_time(stock.get('最后封板时间', '-'), data_date)
                bomb_count = stock.get('炸板次数', 0)
                zt_stats = stock.get('涨停统计', '-')
                continuous_days = stock.get('连板数', 0)
                market_cap = stock.get('总市值', 0)

                if market_cap > 0:
                    if market_cap >= 10000000000:
                        cap_str = f"{market_cap/10000000000:.1f}百亿"
                    elif market_cap >= 100000000:
                        cap_str = f"{market_cap/100000000:.1f}亿"
                    else:
                        cap_str = f"{market_cap/10000:.0f}万"
                else:
                    cap_str = "-"

                detail_info = f"连{continuous_days}d | {zt_stats} | 炸{bomb_count} | {cap_str}"
                time_info = f"首封{first_seal_time} | 末封{last_seal_time}"

                message_lines.append(f"  • {stock['code']} {stock['name']}")
                message_lines.append(f"    {detail_info}")
                message_lines.append(f"    {time_info}")
            
            message_lines.append("")
        
        message = "\n".join(message_lines)
        
        if len(message) > 4000:
            send_long_message(
                f"📊 连续两日涨停个股 ({total_count}只)", 
                [f"{s['code']} {s['name']}" for _, stocks in sorted_sectors for s in stocks],
                ""
            )
        else:
            send_telegram(message)
        
        print(f"\n✅ 已发送连续涨停消息到Telegram")
        
    except Exception as e:
        print(f"❌ 发送连续涨停消息失败: {e}")


def get_sector_fund_flow_example(stock_info_dict):
    """获取板块资金流入排行榜（接受stock_info_dict参数）"""
    print("\n" + "="*60)
    print("💰 板块资金流入排行榜")
    print("="*60)
    
    # ⭐ 只在"今日"时收集领涨股
    df1 = get_sector_fund_flow_rank(indicator='今日', sector_type='概念资金流', top_n=20)
    if df1 is not None and not df1.empty:
        send_sector_fund_flow_message(df1, stock_info_dict, indicator='今日', sector_type='概念资金流', collect_top_stocks=True)
    
    # 5日和10日不收集
    df2 = get_sector_fund_flow_rank(indicator='5日', sector_type='概念资金流', top_n=20)
    if df2 is not None and not df2.empty:
        send_sector_fund_flow_message(df2, stock_info_dict, indicator='5日', sector_type='概念资金流', collect_top_stocks=False)
    
    df3 = get_sector_fund_flow_rank(indicator='10日', sector_type='概念资金流', top_n=20)
    if df3 is not None and not df3.empty:
        send_sector_fund_flow_message(df3, stock_info_dict, indicator='10日', sector_type='概念资金流', collect_top_stocks=False)
    
    print("\n✅ 板块资金流排行榜获取完成！")


def main_scan(force_refresh_cache=False):
    """主扫描流程"""
    print("\n" + "="*60)
    print("🚀 超稳定版扫描开始")
    print("="*60)
    print(f"⚙️  配置:")
    print(f"   - 并发数: {MAX_WORKERS} (串行执行)")
    print(f"   - 延迟: {REQUEST_DELAY_MIN}-{REQUEST_DELAY_MAX}秒")
    print(f"   - 重试: {MAX_RETRIES}次 (指数退避)")
    print(f"   - 限速: 每分钟最多10次请求")
    print(f"   - 缓存: {CACHE_DAYS}天有效期")
    print(f"   - 超时: 120秒")
    print("="*60)
    
    startup_delay = random.uniform(1, 5)
    print(f"\n⏰ 启动延迟 {startup_delay:.1f} 秒...")
    time.sleep(startup_delay)
    
    today = datetime.today()
    if is_weekend(today):
        print(f"\n⚠️  今天是{['周一','周二','周三','周四','周五','周六','周日'][today.weekday()]}，周末没有交易数据")
        return
    
    # 1. 获取股票基本信息（使用缓存）
    stock_info_dict = get_stock_info_dict(force_refresh=force_refresh_cache)
    if not stock_info_dict:
        print("❌ 无法获取股票信息，程序终止")
        return
    
    # 2. 获取全市场涨跌平统计
    market_stats = get_market_stats()
    
    # 3. 统计连续涨停个股
    sector_groups, zt_data_list = get_continuous_limit_up_stocks(stock_info_dict)
    
    if sector_groups and zt_data_list:
        send_continuous_limit_up_message(sector_groups, zt_data_list, market_stats)
    
    # 4. 获取板块资金流排行榜（传入stock_info_dict）
    get_sector_fund_flow_example(stock_info_dict)
    
    print("\n🎉 任务完成！")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='A股涨停个股扫描')
    parser.add_argument('--refresh-cache', action='store_true', 
                       help='强制刷新股票信息缓存')
    parser.add_argument('--clear-cache', action='store_true',
                       help='清空股票信息缓存')
    parser.add_argument('--cache-info', action='store_true',
                       help='显示缓存信息')
    
    args = parser.parse_args()
    
    try:
        if args.clear_cache:
            clear_cache()
            exit(0)
        
        if args.cache_info:
            init_cache_db()
            cache_info = get_cache_info()
            if cache_info:
                print("\n📂 缓存信息:")
                print(f"   股票数量: {cache_info['stock_count']}")
                if cache_info['metadata'].get('last_update'):
                    last_update = cache_info['metadata']['last_update']['updated_at']
                    last_update_dt = datetime.fromisoformat(last_update)
                    days_ago = (datetime.now() - last_update_dt).days
                    print(f"   上次更新: {last_update_dt.strftime('%Y-%m-%d %H:%M:%S')} ({days_ago}天前)")
                print(f"   缓存路径: {CACHE_DB_PATH.absolute()}")
            exit(0)
        
        # 运行主扫描流程
        main_scan(force_refresh_cache=args.refresh_cache)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断程序")
    except Exception as e:
        print(f"\n\n❌ 程序异常退出: {e}")
        import traceback
        traceback.print_exc()