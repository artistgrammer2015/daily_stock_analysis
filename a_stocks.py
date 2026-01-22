"""
scan_m_w_pattern.py - 买点版（简化：只筛选买点1和买点2）
https://akshare.akfamily.xyz/data/stock/stock.html#id14 接口文档
核心逻辑：
- 统计前两个交易日连续涨停的个股，发送TG
- 显示过去三个交易日的成交量（单位：万）
- 显示全市场涨跌平个数统计
- W形态扫描：B点新高后回调，当前价格在A点附近（回踩位）
- 买点1：第一次回踩，当前价格在A点附近
- 买点2：第二次回踩，当前价格在A点附近

优化：
- 周末跳过请求
- 先找交集再请求成交量
"""

import akshare as ak
import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================== 配置 ==================
TELEGRAM_BOT_TOKEN = "8472197175:AAEz6EXsvmEfDkdsZHpczY4v__ARy3AFGT0"
TELEGRAM_CHAT_ID = "6017808464"

START_DATE = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
END_DATE = datetime.today().strftime("%Y-%m-%d")

LOOKBACK = 120           # 寻找 A 的回溯长度
MIN_GAP = 20             # A 和 B 之间至少1个月
BREAKOUT_PCT = 0.01      # 突破阈值 1%
RETEST_WINDOW = 30       # 突破后回踩窗口
RETEST_TOL_PCT = 0.08    # 回踩到A点附近的容忍度 ±8%

# ⭐ 买点判断：当前价格在回踩位附近的容忍度
BUY_POINT_PRICE_TOL = 0.08  # 当前价格距离回踩位±8%

MAX_WORKERS = 6
TEST_SYMBOLS = []
# =========================================

def format_seal_time(time_str, date_str=None):
    """格式化封板时间显示"""
    if not time_str or time_str == '-' or len(str(time_str)) != 6:
        return time_str

    try:
        # 将 HHMMSS 格式转换为 HH:MM:SS
        time_str = str(time_str).strip()
        if len(time_str) == 6:
            hour = time_str[0:2]
            minute = time_str[2:4]
            second = time_str[4:6]

            if date_str:
                # 如果提供了日期，显示完整日期时间
                return f"{date_str} {hour}:{minute}:{second}"
            else:
                # 否则只显示时间
                return f"{hour}:{minute}:{second}"
    except:
        pass

    return time_str


def escape_markdown(text):
    """转义Markdown特殊字符"""
    if not text:
        return text
    result = str(text)
    # 转义Markdown中最重要的特殊字符
    # 按顺序转义，先转义反斜杠，避免重复转义
    result = result.replace('\\', '\\\\')  # 先转义反斜杠本身
    result = result.replace('*', '\\*')    # 转义星号（用于加粗）
    result = result.replace('_', '\\_')    # 转义下划线（用于斜体）
    result = result.replace('[', '\\[')    # 转义左方括号
    result = result.replace(']', '\\]')    # 转义右方括号
    result = result.replace('`', '\\`')    # 转义反引号（用于代码）
    return result


def send_telegram(text):
    """发送Telegram消息"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"Telegram send error: {e}")
        return False


def send_long_message(title, items, prefix=""):
    """发送长消息（自动分段，避免超过Telegram 4096字符限制）"""
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
    
    if current_msg.strip() != f"*{title}*" and current_msg.strip() != f"*{title} (续)*":
        send_telegram(current_msg)


def code_with_prefix(code):
    """添加 sh/sz 前缀"""
    code = str(code)
    if code.startswith("6") or code.startswith("9") or code.startswith("5"):
        return "sh" + code
    else:
        return "sz" + code


def is_valid_stock_code(code):
    """过滤无效代码：指数、ETF、北交所等"""
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
    return date_obj.weekday() >= 5  # 5=周六, 6=周日


def get_market_stats():
    """
    ⭐ 获取全市场涨跌平统计
    返回: {'up': 上涨个数, 'down': 下跌个数, 'flat': 平盘个数, 'total': 总数}
    """
    try:
        print("\n正在获取全市场涨跌统计...")
        
        df = ak.stock_zh_a_spot_em()
        
        if df is None or df.empty:
            return None
        
        # 查找涨跌幅列
        change_col = None
        for col in df.columns:
            if '涨跌幅' in col or 'change' in col.lower() or '涨幅' in col:
                change_col = col
                break
        
        if change_col is None:
            print("⚠️  未找到涨跌幅列")
            return None
        
        # 过滤有效股票
        valid_codes = []
        for _, row in df.iterrows():
            code = str(row.get('代码', row.get('code', '')))
            if is_valid_stock_code(code):
                valid_codes.append(code)
        
        df_filtered = df[df['代码'].isin(valid_codes) if '代码' in df.columns else df['code'].isin(valid_codes)]
        
        # 统计涨跌平
        changes = df_filtered[change_col].astype(float)
        
        up_count = (changes > 0).sum()
        down_count = (changes < 0).sum()
        flat_count = (changes == 0).sum()
        total_count = len(changes)
        
        stats = {
            'up': int(up_count),
            'down': int(down_count),
            'flat': int(flat_count),
            'total': int(total_count)
        }
        
        print(f"✅ 市场统计: 涨{stats['up']} 跌{stats['down']} 平{stats['flat']} 总{stats['total']}")
        
        return stats
        
    except Exception as e:
        print(f"❌ 获取市场统计失败: {e}")
        return None


def get_sector_fund_flow_rank(indicator='今日', sector_type='概念资金流', top_n=20):
    """
    ⭐ 获取板块资金流入排行榜（仅支持概念资金流）
    参数:
        indicator: 时间周期，可选 {"今日", "5日", "10日"}，默认 "今日"
        sector_type: 板块类型，仅支持 "概念资金流"（行业资金流和地域资金流已禁用）
        top_n: 返回前N名，默认20
    返回: DataFrame，包含板块名称、资金流入等数据，按资金流入排序
    """
    try:
        print(f"\n正在获取板块资金流入排行榜 ({sector_type}, {indicator})...")
        
        df = ak.stock_sector_fund_flow_rank(indicator=indicator, sector_type=sector_type)
        
        if df is None or df.empty:
            print("⚠️  未获取到板块资金流数据")
            return None
        
        # 确保有资金流入列
        fund_col = None
        for col in df.columns:
            if '主力净流入' in col and '净额' in col:
                fund_col = col
                break
        
        if fund_col is None:
            print("⚠️  未找到资金流入列")
            return None
        
        # 按资金流入排序（降序）
        df = df.sort_values(by=fund_col, ascending=False).reset_index(drop=True)
        
        # 取前N名
        if top_n > 0:
            df = df.head(top_n)
        
        # 格式化资金流入显示（单位：亿元）
        if fund_col in df.columns:
            df['资金流入(亿元)'] = df[fund_col] / 100000000
        
        print(f"✅ 获取到 {len(df)} 个板块的资金流数据")
        
        return df
        
    except Exception as e:
        print(f"❌ 获取板块资金流排行榜失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def send_sector_fund_flow_message(df, indicator='今日', sector_type='概念资金流'):
    """发送板块资金流入排行榜消息到Telegram（仅支持概念资金流）"""
    if df is None or df.empty:
        return
    
    try:
        # 根据indicator动态查找列名
        prefix = indicator  # "今日"、"5日"、"10日"
        
        # 查找资金流入列
        fund_col = None
        for col in df.columns:
            if '主力净流入' in col and '净额' in col:
                fund_col = col
                break
        
        if fund_col is None:
            print("⚠️  未找到资金流入列")
            return
        
        # 动态查找其他列名
        change_col = f'{prefix}涨跌幅'
        ratio_col = f'{prefix}主力净流入-净占比'
        top_stock_col = f'{prefix}主力净流入最大股'
        
        # 如果找不到，尝试其他可能的列名
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
                if '主力净流入最大股' in col:
                    top_stock_col = col
                    break
        
        message_lines = [
            f"*💰 {sector_type}排行榜 ({indicator})*",
            f"",
        ]
        
        for idx, row in df.iterrows():
            name = str(row.get('名称', row.get('name', '')))
            
            # 安全地转换为数值类型
            try:
                change_pct = float(row.get(change_col, row.get('涨跌幅', 0)))
            except (ValueError, TypeError):
                change_pct = 0
            
            try:
                fund_inflow = float(row.get(fund_col, 0)) / 100000000  # 转换为亿元
            except (ValueError, TypeError):
                fund_inflow = 0
            
            try:
                fund_ratio = float(row.get(ratio_col, row.get('主力净流入-净占比', 0)))
            except (ValueError, TypeError):
                fund_ratio = 0
            
            top_stock = row.get(top_stock_col, row.get('主力净流入最大股', '-'))
            
            # 格式化涨跌幅
            change_str = f"+{change_pct:.2f}%" if change_pct >= 0 else f"{change_pct:.2f}%"
            
            # 格式化资金流入
            if abs(fund_inflow) >= 1:
                fund_str = f"{fund_inflow:.2f}亿"
            else:
                fund_str = f"{fund_inflow * 10000:.0f}万"
            
            # 格式化占比
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
        
        # 如果消息太长，分段发送
        if len(message) > 4000:
            items = []
            for idx, row in df.iterrows():
                name = str(row.get('名称', row.get('name', '')))
                
                # 安全地转换为数值类型
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
                
                top_stock = row.get(top_stock_col, row.get('主力净流入最大股', '-'))
                
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
        
        print(f"✅ 已发送板块资金流排行榜消息到Telegram")
        
    except Exception as e:
        print(f"❌ 发送板块资金流排行榜消息失败: {e}")
        import traceback
        traceback.print_exc()


def get_volume_for_stocks_on_dates(codes, date_strings):
    """
    ⭐ 批量获取指定股票在指定日期的成交量
    codes: 股票代码列表
    date_strings: 日期字符串列表，格式YYYYMMDD
    返回: {code: {date_str: volume}}
    """
    result = {}
    
    print(f"  正在获取 {len(codes)} 只股票在 {len(date_strings)} 个日期的成交量...")
    
    def get_stock_volumes(code):
        try:
            code_prefix = code_with_prefix(code)
            
            # 计算日期范围（包含前后几天的缓冲）
            all_dates = [datetime.strptime(ds, "%Y%m%d") for ds in date_strings]
            start_date = (min(all_dates) - timedelta(days=5)).strftime("%Y-%m-%d")
            end_date = (max(all_dates) + timedelta(days=5)).strftime("%Y-%m-%d")
            
            df = ak.stock_zh_a_daily(symbol=code_prefix, start_date=start_date, end_date=end_date)
            
            if df is None or df.empty:
                return code, {}
            
            df = df.reset_index()
            
            # 统一列名
            col_map = {}
            for c in df.columns:
                cl = str(c).lower().strip()
                if any(x in cl for x in ['日期', 'date', '时间', 'time']):
                    col_map[c] = "date"
                elif any(x in cl for x in ['成交量', 'volume', 'vol']):
                    col_map[c] = "volume"
            
            df = df.rename(columns=col_map)
            
            if 'date' not in df.columns or 'volume' not in df.columns:
                return code, {}
            
            df['date'] = pd.to_datetime(df['date'])
            df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # 提取指定日期的成交量
            volumes = {}
            for date_str in date_strings:
                target_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
                target_row = df[df['date_str'] == target_date]
                if not target_row.empty:
                    volumes[date_str] = float(target_row['volume'].values[0])
                else:
                    volumes[date_str] = 0
            
            return code, volumes
            
        except Exception as e:
            return code, {}
    
    # 使用线程池并发获取
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(get_stock_volumes, code) for code in codes]
        
        for future in as_completed(futures):
            code, volumes = future.result()
            result[code] = volumes
    
    return result


def get_continuous_limit_up_stocks(stock_info_dict):
    """获取连续两个交易日涨停的股票（带板块信息和过去三日成交量）"""
    try:
        # ⭐ 优化1：检查当前是否是周末
        today = datetime.today()
        if is_weekend(today):
            print(f"\n⚠️  今天是{['周一','周二','周三','周四','周五','周六','周日'][today.weekday()]}，周末没有交易数据，跳过")
            return {}, None
        
        print("="*60)
        print("📊 开始统计连续涨停个股...")
        print("="*60)
        
        # 第一步：只获取涨停股票代码，不获取成交量
        zt_data_list = []
        
        for days_ago in range(1, 15):
            if len(zt_data_list) >= 3:
                break
            
            try_date = datetime.today() - timedelta(days=days_ago)
            
            # ⭐ 跳过周末
            if is_weekend(try_date):
                continue
            
            date_str = try_date.strftime("%Y%m%d")
            date_display = try_date.strftime("%Y-%m-%d %A")
            
            try:
                print(f"\n尝试获取 {date_display} 的涨停板数据...")
                df = ak.stock_zt_pool_em(date=date_str)
                
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
                            'df': df  # 保存完整的DataFrame数据
                        })
                    else:
                        print(f"⚠️  {date_display} 没有有效涨停股")
                else:
                    print(f"⚠️  {date_display} 无涨停数据")
                    
            except Exception as e:
                print(f"⚠️  {date_display} 获取失败: {e}")
                continue
            
            time.sleep(0.5)
        
        if len(zt_data_list) < 2:
            print("\n❌ 无法获取足够的涨停板数据")
            return {}, None
        
        zt_data_list.sort(key=lambda x: x['date'], reverse=True)
        
        day1 = zt_data_list[0]  # 最近一个交易日
        day2 = zt_data_list[1]  # 倒数第二个交易日
        day3 = zt_data_list[2] if len(zt_data_list) >= 3 else None  # 倒数第三个交易日
        
        # ⭐ 优化2：先找到连续涨停的交集
        continuous_codes = day1['codes'].intersection(day2['codes'])
        
        print(f"\n{'='*60}")
        print(f"📈 连续涨停统计结果:")
        print(f"{'='*60}")
        print(f"{day2['date_display']}: {len(day2['codes'])} 只涨停")
        print(f"{day1['date_display']}: {len(day1['codes'])} 只涨停")
        print(f"连续涨停: {len(continuous_codes)} 只")
        
        if len(continuous_codes) == 0:
            return {}, zt_data_list
        
        # ⭐ 优化2：只对连续涨停的股票请求成交量
        print(f"\n正在获取 {len(continuous_codes)} 只连续涨停股票的成交量...")
        
        # 准备日期列表
        date_strings = [day1['date_str'], day2['date_str']]
        if day3:
            date_strings.append(day3['date_str'])
        
        # 批量获取成交量
        volume_data = get_volume_for_stocks_on_dates(list(continuous_codes), date_strings)
        
        # 按板块分组
        sector_groups = {}
        
        for code in continuous_codes:
            # 优先从涨停板数据中获取股票名称和行业
            name = code  # 默认使用代码
            sector = "未知"

            # 从最新的涨停板数据（day1）中获取股票名称和行业
            if 'df' in day1 and day1['df'] is not None:
                stock_row = day1['df'][day1['df']['代码'] == code]
                if not stock_row.empty:
                    row = stock_row.iloc[0]
                    name = str(row.get('名称', code))
                    sector = str(row.get('所属行业', '未知'))

            # 如果涨停板数据中没有，则尝试从 stock_info_dict 获取
            if name == code and isinstance(stock_info_dict.get(code), dict):
                name = stock_info_dict[code].get('name', code)
                sector = stock_info_dict[code].get('sector', sector)

            # 获取成交量（单位：万）
            code_volumes = volume_data.get(code, {})
            vol_day1 = code_volumes.get(day1['date_str'], 0) / 10000
            vol_day2 = code_volumes.get(day2['date_str'], 0) / 10000
            vol_day3 = code_volumes.get(day3['date_str'], 0) / 10000 if day3 else 0

            # 从涨停板数据中获取详细信息
            stock_detail = {'首次封板时间': '-', '最后封板时间': '-', '炸板次数': 0,
                          '涨停统计': '-', '连板数': 0, '总市值': 0, '所属行业': sector}

            # 从最新的涨停板数据（day1）中获取详细信息
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
                'vol_day3': vol_day3,
                'vol_day2': vol_day2,
                'vol_day1': vol_day1,
                **stock_detail
            })
        
        return sector_groups, zt_data_list
        
    except Exception as e:
        print(f"❌ 统计连续涨停失败: {e}")
        import traceback
        traceback.print_exc()
        return {}, None


def send_continuous_limit_up_message(sector_groups, zt_data_list, market_stats):
    """发送连续涨停个股的Telegram消息"""
    if not sector_groups:
        return
    
    try:
        day1 = zt_data_list[0]
        day2 = zt_data_list[1]
        day3 = zt_data_list[2] if len(zt_data_list) >= 3 else None
        
        total_count = sum(len(stocks) for stocks in sector_groups.values())
        
        message_lines = [
            f"*📊 连续两日涨停个股 ({total_count}只)*",
            f"",
            f"📅 {day2['date'].strftime('%m月%d日')} → {day1['date'].strftime('%m月%d日')}",
        ]
        
        # ⭐ 添加全市场涨跌平统计
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
                # 显示过去三个交易日的成交量（单位：万）
                if day3 and stock['vol_day3'] > 0:
                    vol_str = f"{stock['vol_day3']:.0f}w {stock['vol_day2']:.0f}w {stock['vol_day1']:.0f}w"
                else:
                    vol_str = f"{stock['vol_day2']:.0f}w {stock['vol_day1']:.0f}w"

                # 获取股票详细信息
                # 获取涨停数据日期
                data_date = day1['date'].strftime('%Y-%m-%d') if day1 else None
                first_seal_time = format_seal_time(stock.get('首次封板时间', '-'), data_date)
                last_seal_time = format_seal_time(stock.get('最后封板时间', '-'), data_date)
                bomb_count = stock.get('炸板次数', 0)
                zt_stats = stock.get('涨停统计', '-')
                continuous_days = stock.get('连板数', 0)
                market_cap = stock.get('总市值', 0)

                # 格式化市值显示
                if market_cap > 0:
                    if market_cap >= 10000000000:  # 百亿
                        cap_str = f"{market_cap/10000000000:.1f}百亿"
                    elif market_cap >= 100000000:  # 亿
                        cap_str = f"{market_cap/100000000:.1f}亿"
                    else:  # 万
                        cap_str = f"{market_cap/10000:.0f}万"
                else:
                    cap_str = "-"

                # 构建详细信息字符串
                detail_info = f"连{continuous_days}d | {zt_stats} | 炸{bomb_count} | {cap_str}"
                time_info = f"首封{first_seal_time} | 末封{last_seal_time}"

                message_lines.append(f"  • {stock['code']} {stock['name']}")
                message_lines.append(f"    成交: {vol_str} | {detail_info}")
                message_lines.append(f"    封板: {time_info}")
            
            message_lines.append("")
        
        message = "\n".join(message_lines)
        
        if len(message) > 4000:
            items = []
            
            # 先发送市场统计
            if market_stats:
                stats_msg = (
                    f"*📈 全市场统计:*\n"
                    f"涨: {market_stats['up']}只 | 跌: {market_stats['down']}只 | 平: {market_stats['flat']}只\n"
                    f"总计: {market_stats['total']}只\n"
                )
                items.append(stats_msg)
            
            for sector, stocks in sorted_sectors:
                items.append(f"*{sector}* ({len(stocks)}只):")
                for stock in stocks:
                    if day3 and stock['vol_day3'] > 0:
                        vol_str = f"{stock['vol_day3']:.0f}w {stock['vol_day2']:.0f}w {stock['vol_day1']:.0f}w"
                    else:
                        vol_str = f"{stock['vol_day2']:.0f}w {stock['vol_day1']:.0f}w"

                    # 获取股票详细信息
                    # 获取涨停数据日期
                    data_date = day1['date'].strftime('%Y-%m-%d') if day1 else None
                    first_seal_time = format_seal_time(stock.get('首次封板时间', '-'), data_date)
                    last_seal_time = format_seal_time(stock.get('最后封板时间', '-'), data_date)
                    bomb_count = stock.get('炸板次数', 0)
                    zt_stats = stock.get('涨停统计', '-')
                    continuous_days = stock.get('连板数', 0)
                    market_cap = stock.get('总市值', 0)

                    # 格式化市值显示
                    if market_cap > 0:
                        if market_cap >= 10000000000:  # 百亿
                            cap_str = f"{market_cap/10000000000:.1f}百亿"
                        elif market_cap >= 100000000:  # 亿
                            cap_str = f"{market_cap/100000000:.1f}亿"
                        else:  # 万
                            cap_str = f"{market_cap/10000:.0f}万"
                    else:
                        cap_str = "-"

                    # 构建详细信息字符串
                    detail_info = f"连{continuous_days}d | {zt_stats} | 炸{bomb_count} | {cap_str}"
                    time_info = f"首封{first_seal_time} | 末封{last_seal_time}"

                    items.append(f"{stock['code']} {stock['name']}")
                    items.append(f"  成交: {vol_str} | {detail_info}")
                    items.append(f"  封板: {time_info}")
                    items.append("")  # 空行分隔
            
            title = f"📊 连续两日涨停个股 ({total_count}只)\n{day2['date'].strftime('%m月%d日')}→{day1['date'].strftime('%m月%d日')}"
            send_long_message(title, items, "  ")
        else:
            send_telegram(message)
        
        print(f"\n✅ 已发送连续涨停消息到Telegram")
        
    except Exception as e:
        print(f"❌ 发送连续涨停消息失败: {e}")


def fetch_daily(code_prefix):
    """增强版数据获取"""
    try:
        if not is_valid_stock_code(code_prefix):
            return None
        
        df = ak.stock_zh_a_daily(symbol=code_prefix, start_date=START_DATE, end_date=END_DATE)
        
        if df is None or df.empty:
            return None
        
        if len(df) < 60:
            return None
        
        df = df.reset_index()
        
        col_map = {}
        for c in df.columns:
            cl = str(c).lower().strip()
            
            if any(x in cl for x in ['日期', 'date', '时间', 'time', 'datetime']):
                col_map[c] = "date"
            elif any(x in cl for x in ['开盘', 'open']):
                col_map[c] = "open"
            elif any(x in cl for x in ['收盘', 'close']):
                col_map[c] = "close"
            elif any(x in cl for x in ['最高', 'high']):
                col_map[c] = "high"
            elif any(x in cl for x in ['最低', 'low']):
                col_map[c] = "low"
        
        df = df.rename(columns=col_map)
        
        required_cols = ['date', 'open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            return None
        
        df = df[required_cols].copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        return df
        
    except Exception as e:
        return None


def detect_magic_13_turn(df):
    """
    ⭐ 检测神奇13转形态（只检测昨天收盘时是否完成13转）
    规则：
    - 上升13转：连续13天收盘价高于前4天的收盘价，且昨天收盘时刚好完成（可能预示下跌反转，卖出信号）
    - 下跌13转：连续13天收盘价低于前4天的收盘价，且昨天收盘时刚好完成（可能预示上涨反转，买入信号）
    
    返回: (has_pattern, pattern_type, details)
    - has_pattern: 是否检测到形态
    - pattern_type: "上升13转" 或 "下跌13转"
    - details: 包含形态详情的字典
    """
    try:
        if df is None or len(df) < 17:  # 至少需要17天数据（13天计数 + 4天比较基准）
            return False, None, None
        
        closes = df['close'].values
        dates = df['date'].values
        n = len(closes)
        
        # 只检查最近13天（从倒数第13天到昨天）
        # 需要确保有足够的数据（至少17天，因为需要前4天作为基准）
        if n < 17:
            return False, None, None
        
        # 从倒数第13天开始检查，到昨天（最后一天）
        start_idx = n - 13  # 倒数第13天的索引
        end_idx = n - 1     # 昨天（最后一天）的索引
        
        # 确保start_idx >= 4，这样才能比较前4天的价格
        if start_idx < 4:
            return False, None, None
        
        # 检查这13天是否都满足上升条件（每天收盘价都高于前4天）
        up_all_match = True
        for i in range(start_idx, end_idx + 1):
            current_close = closes[i]
            prev_4_close = closes[i - 4]
            if current_close <= prev_4_close:  # 不满足上升条件
                up_all_match = False
                break
        
        # 检查这13天是否都满足下跌条件（每天收盘价都低于前4天）
        down_all_match = True
        for i in range(start_idx, end_idx + 1):
            current_close = closes[i]
            prev_4_close = closes[i - 4]
            if current_close >= prev_4_close:  # 不满足下跌条件
                down_all_match = False
                break
        
        # 如果上升13转完成（昨天收盘时刚好完成）
        if up_all_match:
            current_price = closes[end_idx]
            count_start_price = closes[start_idx]
            price_change = (current_price - count_start_price) / count_start_price * 100
            
            return True, "上升13转", {
                'start_date': dates[start_idx],
                'end_date': dates[end_idx],
                'start_price': count_start_price,
                'end_price': current_price,
                'price_change_pct': price_change,
                'current_price': current_price,
                'signal': '卖出信号（可能下跌反转）'
            }
        
        # 如果下跌13转完成（昨天收盘时刚好完成）
        if down_all_match:
            current_price = closes[end_idx]
            count_start_price = closes[start_idx]
            price_change = (current_price - count_start_price) / count_start_price * 100
            
            return True, "下跌13转", {
                'start_date': dates[start_idx],
                'end_date': dates[end_idx],
                'start_price': count_start_price,
                'end_price': current_price,
                'price_change_pct': price_change,
                'current_price': current_price,
                'signal': '买入信号（可能上涨反转）'
            }
        
        return False, None, None
        
    except Exception as e:
        return False, None, None


def is_st_stock(name):
    """判断是否是ST股票"""
    if not name:
        return False
    name_str = str(name).upper()
    return 'ST' in name_str or '*ST' in name_str or 'ST*' in name_str


def get_stock_profit(code):
    """获取股票最新业绩（净利润），返回净利润（单位：元），如果获取失败返回None"""
    try:
        code_prefix = code_with_prefix(code)
        # 获取最新财报数据（按报告期）
        df = ak.stock_profit_sheet_by_report_em(symbol=code_prefix)
        
        if df is None or df.empty:
            return None
        
        # 查找净利润列
        profit_col = None
        for col in df.columns:
            col_lower = str(col).lower()
            if '净利润' in str(col) or 'net_profit' in col_lower or '归属净利润' in str(col):
                profit_col = col
                break
        
        if profit_col is None or profit_col not in df.columns:
            return None
        
        # 获取最新的净利润（最后一行，最新的报告期）
        latest_profit = df[profit_col].iloc[-1]
        
        try:
            profit_value = float(latest_profit)
            return profit_value
        except (ValueError, TypeError):
            return None
        
    except Exception as e:
        # 如果获取失败，返回None（表示无法判断，不排除该股票）
        return None


def scan_magic_13_turn(code, stock_info_dict):
    """扫描单只股票的神奇13转形态（排除ST股票和业绩为负的股票）"""
    try:
        # 先检查股票名称，排除ST股票
        name = stock_info_dict.get(code, code)
        if isinstance(stock_info_dict.get(code), dict):
            name = stock_info_dict[code].get('name', code)
        
        # 排除ST股票
        if is_st_stock(name):
            return None
        
        code_prefix = code_with_prefix(code)
        df = fetch_daily(code_prefix)
        
        if df is None:
            return None
        
        has_pattern, pattern_type, details = detect_magic_13_turn(df)
        
        if not has_pattern:
            return None
        
        # 检查业绩，排除业绩为负的股票
        profit = get_stock_profit(code)
        if profit is not None and profit < 0:
            return None
        
        sector = "未知"
        if isinstance(stock_info_dict.get(code), dict):
            sector = stock_info_dict[code].get('sector', '未知')
            name = stock_info_dict[code].get('name', code)
        
        return {
            'code': code,
            'name': name,
            'sector': sector,
            'pattern_type': pattern_type,
            'details': details
        }
        
    except Exception as e:
        return None


def detect_buy_points(df):
    """
    ⭐ 简化的买点判断逻辑：
    - 买点1：B点新高后第一次回踩到A点附近，当前价格在回踩位附近
    - 买点2：B点新高后第二次回踩到A点附近，当前价格在回踩位附近
    """
    n = df.shape[0]
    if n < 60:
        return False, None, None

    highs = df['high'].values
    lows = df['low'].values
    closes = df['close'].values
    dates = df['date'].values
    
    latest_idx = n - 1
    current_price = closes[latest_idx]
    
    # 扫描可能的 A 点
    for i in range(LOOKBACK, n - MIN_GAP - RETEST_WINDOW):
        window_high = highs[i - LOOKBACK:i + 1]
        window_low = lows[i - LOOKBACK:i + 1]
        
        A_price = window_high.max()
        A_low = window_low.min()
        
        if highs[i] != A_price:
            continue
        
        # 寻找突破点 B
        for j in range(i + MIN_GAP, n - RETEST_WINDOW):
            if highs[j] < A_price * (1 + BREAKOUT_PCT):
                continue
            
            B_price = highs[j]
            B_idx = j
            
            # 在突破后的窗口内寻找回踩
            retest_start = B_idx + 1
            retest_end = min(B_idx + RETEST_WINDOW, n)
            
            retest_events = []
            
            for k in range(retest_start, retest_end):
                retest_low = lows[k]
                
                # ⭐ 回踩到A点附近，但不破A点最低价
                if (A_low * 0.92 <= retest_low <= A_price * (1 + RETEST_TOL_PCT)) and retest_low > A_low:
                    retest_events.append({
                        'idx': k,
                        'low': retest_low,
                        'date': dates[k]
                    })
            
            if len(retest_events) < 1:
                continue
            
            # 取前两次回踩
            retest1 = retest_events[0]
            retest2 = retest_events[1] if len(retest_events) >= 2 else None
            
            # ===== 买点1判断 =====
            # ⭐ 当前价格在第一次回踩位附近（±8%）
            if abs(current_price - retest1['low']) / retest1['low'] <= BUY_POINT_PRICE_TOL:
                # 当前价格高于A点最低价（确认不破支撑）
                if current_price > A_low:
                    days_A_to_B = (dates[B_idx] - dates[i]).days
                    return True, "买点1", {
                        'A_date': dates[i],
                        'A_price': A_price,
                        'A_low': A_low,
                        'B_date': dates[B_idx],
                        'B_price': B_price,
                        'days_A_to_B': days_A_to_B,
                        'retest1_date': retest1['date'],
                        'retest1_low': retest1['low'],
                        'current_price': current_price
                    }
            
            # ===== 买点2判断 =====
            # ⭐ 有第二次回踩，且当前价格在第二次回踩位附近（±8%）
            if retest2:
                if abs(current_price - retest2['low']) / retest2['low'] <= BUY_POINT_PRICE_TOL:
                    if current_price > A_low:
                        days_A_to_B = (dates[B_idx] - dates[i]).days
                        return True, "买点2", {
                            'A_date': dates[i],
                            'A_price': A_price,
                            'A_low': A_low,
                            'B_date': dates[B_idx],
                            'B_price': B_price,
                            'days_A_to_B': days_A_to_B,
                            'retest1_date': retest1['date'],
                            'retest1_low': retest1['low'],
                            'retest2_date': retest2['date'],
                            'retest2_low': retest2['low'],
                            'current_price': current_price
                        }
    
    return False, None, None


def scan_one_stock(code, stock_info_dict):
    """扫描单只股票"""
    try:
        code_prefix = code_with_prefix(code)
        df = fetch_daily(code_prefix)
        
        if df is None:
            return None
        
        found, buy_point_type, details = detect_buy_points(df)
        
        if not found:
            return None
        
        name = stock_info_dict.get(code, code)
        sector = "未知"
        
        if isinstance(stock_info_dict.get(code), dict):
            sector = stock_info_dict[code].get('sector', '未知')
            name = stock_info_dict[code].get('name', code)
        
        return {
            'code': code,
            'name': name,
            'sector': sector,
            'buy_point_type': buy_point_type,
            'details': details
        }
        
    except Exception as e:
        return None


def get_stock_info_dict():
    """获取股票信息字典（代码->名称+板块）- 带重试机制"""
    try:
        print("正在获取股票基本信息...")
        
        max_retries = 3
        df = None
        
        for attempt in range(1, max_retries + 1):
            try:
                print(f"  尝试获取股票列表... (第{attempt}/{max_retries}次)")
                df = ak.stock_zh_a_spot_em()
                
                if df is not None and not df.empty:
                    print(f"  ✅ 成功获取股票列表")
                    break
                    
            except Exception as e:
                print(f"  ⚠️  第{attempt}次尝试失败: {e}")
                if attempt < max_retries:
                    wait_time = attempt * 2
                    print(f"  等待{wait_time}秒后重试...")
                    time.sleep(wait_time)
        
        if df is None or df.empty:
            print("  ❌ 无法获取股票列表，使用空字典")
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
        
    except Exception as e:
        print(f"⚠️  获取股票信息失败: {e}")
        return {}


def get_all_stock_codes():
    """获取全市场股票列表 - 带重试机制"""
    try:
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            try:
                print(f"正在获取全市场股票列表... (第{attempt}/{max_retries}次)")
                df = ak.stock_zh_a_spot_em()
                
                if df is not None and not df.empty:
                    if '代码' in df.columns:
                        codes = df['代码'].astype(str).tolist()
                    elif 'code' in df.columns:
                        codes = df['code'].astype(str).tolist()
                    else:
                        codes = df.iloc[:, 0].astype(str).tolist()
                    
                    codes = [c for c in codes if is_valid_stock_code(c)]
                    
                    print(f"✅ 成功获取 {len(codes)} 只股票代码")
                    return codes
                    
            except Exception as e:
                print(f"⚠️  第{attempt}次尝试失败: {e}")
                if attempt < max_retries:
                    wait_time = attempt * 3
                    print(f"等待{wait_time}秒后重试...")
                    time.sleep(wait_time)
        
        # 如果所有尝试都失败，使用备用方案
        print("\n⚠️  无法获取实时股票列表，使用备用方案...")
        print("提示：可以手动提供股票代码列表，或稍后重试")
        return []
        
    except Exception as e:
        print(f"❌ 获取股票列表失败: {e}")
        return []


def main_scan():
    """主扫描流程"""
    print("\n" + "="*60)
    print("🚀 W形态买点扫描开始（只筛选买点1和买点2）")
    print("="*60)
    
    # ⭐ 优化1：检查是否周末
    today = datetime.today()
    if is_weekend(today):
        print(f"\n⚠️  今天是{['周一','周二','周三','周四','周五','周六','周日'][today.weekday()]}，周末没有交易数据")
        print("建议在交易日运行此脚本")
        return
    
    # 1. 获取股票基本信息
    stock_info_dict = get_stock_info_dict()
    
    # 2. 获取全市场涨跌平统计
    market_stats = get_market_stats()
    
    # 2.5. 获取板块资金流入排行榜并发送TG消息
    print(f"\n{'='*60}")
    print("💰 获取板块资金流入排行榜...")
    print(f"{'='*60}")
    
    # 获取行业资金流排行榜（今日）
    # industry_flow_df = get_sector_fund_flow_rank(indicator='今日', sector_type='行业资金流', top_n=20)
    # if industry_flow_df is not None and not industry_flow_df.empty:
    #    send_sector_fund_flow_message(industry_flow_df, indicator='今日', sector_type='行业资金流')
    
    # 3. 统计连续涨停个股并发送TG消息
    sector_groups, zt_data_list = get_continuous_limit_up_stocks(stock_info_dict)
    
    if sector_groups and zt_data_list:
        send_continuous_limit_up_message(sector_groups, zt_data_list, market_stats)
    
    # 4. 扫描神奇13转形态并发送TG消息（只发送下跌13转）
    magic_13_results = scan_all_magic_13_turn(stock_info_dict)
    if magic_13_results:
        has_results = False
        # 只发送下跌13转，上升13转已注释
        # for pattern_type in ['上升13转', '下跌13转']:
        for pattern_type in ['下跌13转']:  # 只保留下跌13转
            if magic_13_results.get(pattern_type) and len(magic_13_results[pattern_type]) > 0:
                send_magic_13_turn_message(pattern_type, magic_13_results[pattern_type])
                has_results = True
        
        if not has_results:
            print("ℹ️  未检测到下跌13转形态")
    
    # 5. 获取全市场股票列表（带重试）
    # print(f"\n{'='*60}")
    # print("📊 开始W形态扫描...")
    # print(f"{'='*60}")
    # 
    # all_codes = get_all_stock_codes()
    # 
    # if not all_codes:
    #     print("❌ 无法获取股票列表，扫描终止")
    #     return
    # 
    # print(f"总共需扫描: {len(all_codes)} 只股票")
    # 
    # # 5. 扫描W形态（只筛选买点1和买点2）
    # results = {
    #     '买点1': [],
    #     '买点2': []
    # }
    # 
    # with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    #     futures = [executor.submit(scan_one_stock, code, stock_info_dict) for code in all_codes]
    #     
    #     completed = 0
    #     for future in as_completed(futures):
    #         completed += 1
    #         if completed % 100 == 0:
    #             print(f"已扫描 {completed}/{len(all_codes)}...")
    #         
    #         result = future.result()
    #         if result:
    #             buy_point_type = result['buy_point_type']
    #             results[buy_point_type].append(result)
    # 
    # print(f"\n{'='*60}")
    # print("✅ 扫描完成")
    # print(f"{'='*60}")
    # print(f"买点1: {len(results['买点1'])} 只")
    # print(f"买点2: {len(results['买点2'])} 只")
    # 
    # # 6. 发送买点1和买点2的消息到TG
    # for buy_point_type in ['买点1', '买点2']:
    #     if results[buy_point_type]:
    #         send_buy_point_message(buy_point_type, results[buy_point_type])
    # 
    # print("\n🎉 所有任务完成！")
    
    print("\n🎉 任务完成！")


def scan_all_magic_13_turn(stock_info_dict):
    """扫描全市场股票的神奇13转形态"""
    try:
        print(f"\n{'='*60}")
        print("🔮 开始扫描神奇13转形态...")
        print(f"{'='*60}")
        
        # 直接从stock_info_dict获取股票代码列表（避免重复获取）
        all_codes = list(stock_info_dict.keys())
        
        if not all_codes:
            print("❌ 股票信息字典为空，扫描终止")
            return {'上升13转': [], '下跌13转': []}
        
        # 过滤有效股票代码
        all_codes = [code for code in all_codes if is_valid_stock_code(code)]
        
        print(f"总共需扫描: {len(all_codes)} 只股票")
        
        results = {
            '上升13转': [],
            '下跌13转': []
        }
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(scan_magic_13_turn, code, stock_info_dict) for code in all_codes]
            
            completed = 0
            for future in as_completed(futures):
                completed += 1
                if completed % 100 == 0:
                    print(f"已扫描 {completed}/{len(all_codes)}...")
                
                result = future.result()
                if result:
                    pattern_type = result['pattern_type']
                    results[pattern_type].append(result)
        
        print(f"\n{'='*60}")
        print("✅ 扫描完成")
        print(f"{'='*60}")
        print(f"上升13转: {len(results['上升13转'])} 只")
        print(f"下跌13转: {len(results['下跌13转'])} 只")
        
        # 如果有结果，打印前几个示例
        if len(results['上升13转']) > 0:
            print(f"\n上升13转示例: {results['上升13转'][0]['code']} {results['上升13转'][0]['name']}")
        if len(results['下跌13转']) > 0:
            print(f"下跌13转示例: {results['下跌13转'][0]['code']} {results['下跌13转'][0]['name']}")
        
        return results
        
    except Exception as e:
        print(f"❌ 扫描神奇13转失败: {e}")
        import traceback
        traceback.print_exc()
        return {'上升13转': [], '下跌13转': []}


def send_magic_13_turn_message(pattern_type, stocks):
    """发送神奇13转消息到Telegram（按板块分组）"""
    if not stocks:
        return
    
    try:
        sector_groups = {}
        for stock in stocks:
            sector = stock['sector']
            if sector not in sector_groups:
                sector_groups[sector] = []
            sector_groups[sector].append(stock)
        
        sorted_sectors = sorted(sector_groups.items(), key=lambda x: len(x[1]), reverse=True)
        
        # 根据形态类型选择emoji
        emoji = "📈" if pattern_type == "下跌13转" else "📉"
        signal_text = "买入信号" if pattern_type == "下跌13转" else "卖出信号"
        
        message_lines = [
            f"*{emoji} {pattern_type} ({len(stocks)}只) - {signal_text}*",
            f"",
        ]
        
        for sector, sector_stocks in sorted_sectors:
            # 转义板块名称中的特殊字符
            escaped_sector = escape_markdown(sector)
            message_lines.append(f"*{escaped_sector}* ({len(sector_stocks)}只):")
            
            for stock in sector_stocks:
                details = stock['details']
                start_date_str = pd.to_datetime(details['start_date']).strftime('%m-%d')
                end_date_str = pd.to_datetime(details['end_date']).strftime('%m-%d')
                price_change = details['price_change_pct']
                current_price = details['current_price']
                
                # 转义股票名称中的特殊字符（如*ST）
                escaped_name = escape_markdown(stock['name'])
                escaped_code = escape_markdown(stock['code'])
                escaped_signal = escape_markdown(details['signal'])
                
                change_str = f"+{price_change:.2f}%" if price_change >= 0 else f"{price_change:.2f}%"
                
                info_line = (
                    f"  • {escaped_code} {escaped_name}\n"
                    f"    日期: {start_date_str}→{end_date_str} | 涨幅: {change_str} | 当前价: {current_price:.2f}\n"
                    f"    信号: {escaped_signal}"
                )
                message_lines.append(info_line)
            
            message_lines.append("")
        
        message = "\n".join(message_lines)
        
        if len(message) > 4000:
            items = []
            for sector, sector_stocks in sorted_sectors:
                # 转义板块名称中的特殊字符
                escaped_sector = escape_markdown(sector)
                items.append(f"*{escaped_sector}* ({len(sector_stocks)}只):")
                for stock in sector_stocks:
                    details = stock['details']
                    start_date_str = pd.to_datetime(details['start_date']).strftime('%m-%d')
                    end_date_str = pd.to_datetime(details['end_date']).strftime('%m-%d')
                    price_change = details['price_change_pct']
                    change_str = f"+{price_change:.2f}%" if price_change >= 0 else f"{price_change:.2f}%"
                    # 转义股票名称和代码中的特殊字符
                    escaped_name = escape_markdown(stock['name'])
                    escaped_code = escape_markdown(stock['code'])
                    escaped_signal = escape_markdown(details['signal'])
                    items.append(f"{escaped_code} {escaped_name} | {start_date_str}→{end_date_str} | {change_str} | {escaped_signal}")
            
            send_long_message(f"{emoji} {pattern_type} ({len(stocks)}只) - {signal_text}", items, "  ")
        else:
            send_telegram(message)
        
        print(f"✅ 已发送{pattern_type}消息到Telegram")
        
    except Exception as e:
        print(f"❌ 发送{pattern_type}消息失败: {e}")
        import traceback
        traceback.print_exc()


def send_buy_point_message(buy_point_type, stocks):
    """发送买点消息到Telegram（按板块分组）"""
    if not stocks:
        return
    
    try:
        sector_groups = {}
        for stock in stocks:
            sector = stock['sector']
            if sector not in sector_groups:
                sector_groups[sector] = []
            sector_groups[sector].append(stock)
        
        sorted_sectors = sorted(sector_groups.items(), key=lambda x: len(x[1]), reverse=True)
        
        message_lines = [
            f"*🎯 {buy_point_type} ({len(stocks)}只)*",
            f"",
        ]
        
        for sector, sector_stocks in sorted_sectors:
            message_lines.append(f"*{sector}* ({len(sector_stocks)}只):")
            
            for stock in sector_stocks:
                details = stock['details']
                A_date_str = pd.to_datetime(details['A_date']).strftime('%m-%d')
                B_date_str = pd.to_datetime(details['B_date']).strftime('%m-%d')
                days_A_to_B = details['days_A_to_B']
                retest_price = details['retest1_low']
                current_price = details['current_price']
                
                info_line = (
                    f"  • {stock['code']} {stock['name']}\n"
                    f"    A:{A_date_str} B:{B_date_str} ({days_A_to_B}天) 回踩:{retest_price:.2f} 当前:{current_price:.2f}"
                )
                message_lines.append(info_line)
            
            message_lines.append("")
        
        message = "\n".join(message_lines)
        
        if len(message) > 4000:
            items = []
            for sector, sector_stocks in sorted_sectors:
                items.append(f"*{sector}* ({len(sector_stocks)}只):")
                for stock in sector_stocks:
                    details = stock['details']
                    A_date_str = pd.to_datetime(details['A_date']).strftime('%m-%d')
                    B_date_str = pd.to_datetime(details['B_date']).strftime('%m-%d')
                    items.append(f"{stock['code']} {stock['name']} A:{A_date_str} B:{B_date_str}")
            
            send_long_message(f"🎯 {buy_point_type} ({len(stocks)}只)", items, "  ")
        else:
            send_telegram(message)
        
        print(f"✅ 已发送{buy_point_type}消息到Telegram")
        
    except Exception as e:
        print(f"❌ 发送{buy_point_type}消息失败: {e}")


def get_sector_fund_flow_example():
    """
    ⭐ 示例：获取板块资金流入排行榜
    可以单独调用此函数来获取板块资金流数据
    """
    print("\n" + "="*60)
    print("💰 板块资金流入排行榜示例")
    print("="*60)
    
    # 示例1：获取今日行业资金流排行榜（前20名）
    print("\n【示例1】今日概念资金流排行榜（前20名）")
    df1 = get_sector_fund_flow_rank(indicator='今日', sector_type='概念资金流', top_n=20)
    if df1 is not None and not df1.empty:
        print("\n前10名：")
        # 动态获取列名
        cols_to_show = ['名称']
        for col in df1.columns:
            if '涨跌幅' in col or ('主力净流入' in col and ('净额' in col or '净占比' in col)):
                cols_to_show.append(col)
        print(df1[cols_to_show[:4]].head(10).to_string())
        # 发送到Telegram
        send_sector_fund_flow_message(df1, indicator='今日', sector_type='概念资金流')
    
    # 示例2：获取5日概念资金流排行榜（前15名）
    print("\n【示例2】5日概念资金流排行榜（前20名）")
    df2 = get_sector_fund_flow_rank(indicator='5日', sector_type='概念资金流', top_n=20)
    if df2 is not None and not df2.empty:
        print("\n前10名：")
        # 动态获取列名
        cols_to_show = ['名称']
        for col in df2.columns:
            if '涨跌幅' in col or ('主力净流入' in col and ('净额' in col or '净占比' in col)):
                cols_to_show.append(col)
        print(df2[cols_to_show[:4]].head(10).to_string())
        # 发送到Telegram
        send_sector_fund_flow_message(df2, indicator='5日', sector_type='概念资金流')
    
    # 示例3：获取10日地域资金流排行榜（前10名）
    print("\n【示例3】10日概念资金流排行榜（前20名）")
    df3 = get_sector_fund_flow_rank(indicator='10日', sector_type='概念资金流', top_n=20)
    if df3 is not None and not df3.empty:
        print("\n全部：")
        # 动态获取列名
        cols_to_show = ['名称']
        for col in df3.columns:
            if '涨跌幅' in col or ('主力净流入' in col and ('净额' in col or '净占比' in col)):
                cols_to_show.append(col)
        print(df3[cols_to_show[:4]].to_string())
        # 发送到Telegram
        send_sector_fund_flow_message(df3, indicator='10日', sector_type='概念资金流')
    
    print("\n✅ 板块资金流排行榜获取完成！")


if __name__ == "__main__":
    # 运行主扫描流程（包含板块资金流排行榜）
    main_scan()
    
    # 如果只想获取板块资金流排行榜，可以取消下面的注释：
    get_sector_fund_flow_example()