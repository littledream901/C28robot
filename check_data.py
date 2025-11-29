import requests
import json
from pprint import pprint
from datetime import datetime
from collections import defaultdict
import time
import sys

# ====================================================================
# A. 全局配置与 API 参数
# ====================================================================

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
BASE_URL = "http://admin.pikaqiu.cfd/api/psndoc/listReport"

# --- API 请求 Body 参数 ---
PAYLOAD_TEMPLATE = {
    "sortOrder": "asc",  
    "pageSize": 200,      
    "pageNumber": 1,
    "id": "c6d22d9cef1498bb9885bd7e20ff502b", # 目标用户ID
    "moneyType": "",     
    "lot_type": "0"
}

# --- HTTP Headers (请务必更新您的授权令牌！) ---
HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "zh-CN,zh;q=0.9",
    "authorization": "Bearerer;eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJMb3R0ZXJ5IiwiaWF0IjoxNzY0MTM3MjIxLCJ1c2VySWQiOiJyb29tMTUyNzMiLCJhY2NvdW50TmFtZSI6InBpa2FxaXUiLCJ1c2VyTmFtZSI6IuaTjeS9nOWRmCIsInJvb21Db2RlIjoiMTAwMDEiLCJyb29tTmFtZSI6IuearuWNoeS4mCIsImV4cGlyZVRpbWUiOiIyMDI1MTIwNTIyMTgwNiIsImNyZWF0ZVRpbWUiOiIyMDIwMDYyNTIyMjQwMyIsInN0YXR1cyI6IiIsImtleSI6InJvb20xNTI3My1wYyIsInBhc3N3b3JkIjoiREVEOEM2OUMyN0QzOUUyMDI3MzYxQUY2MTJERUU0OUJFNzhCRTdCMCIsIm51bSI6MCwiZXhwIjoxNzY0OTQ0Mjg2fQ.CIzUY2djVUlnPS0Ahf_scVFhcq-dvDVa21PyDZOOodE",
    "content-type": "application/json",
    "x-requested-with": "XMLHttpRequest",
}

# --- 核心交易类型定义 (请确保与您系统一致) ---
WAGERING_TYPES = {'下注'}         
CANCELLATION_TYPES = {'取消下注'} 
WINNINGS_TYPES = {'中奖加分', '回水加分', '佣金加分'} 
CORE_TYPES = WAGERING_TYPES | CANCELLATION_TYPES | WINNINGS_TYPES


# ====================================================================
# B. 数据获取函数 (保持不变)
# ====================================================================

def fetch_all_transaction_records(base_url, payload_template, headers):
    """自动循环获取所有页面的交易记录。"""
    all_records = []
    current_page = 1
    total_pages = 1
    
    user_id = payload_template.get('id', '未知用户')
    sort_order = payload_template.get('sortOrder', 'N/A')
    
    print(f"🔄 正在尝试获取用户 {user_id} 的交易记录 (排序方式: ID {sort_order.upper()})...")

    while current_page <= total_pages:
        current_payload = payload_template.copy()
        current_payload['pageNumber'] = current_page
        
        # URL GET 参数
        url_params = {
            "pageSize": current_payload.get('pageSize'),
            "pageNumber": current_page
        }
        
        try:
            response = requests.post(
                base_url, 
                headers=headers, 
                params=url_params,               
                data=json.dumps(current_payload),  
                timeout=30
            )
            response.raise_for_status()
            
            response_json = response.json()
            if 'data' not in response_json or 'records' not in response_json['data']:
                print("❌ API响应结构异常，缺少 'data' 或 'records' 字段。")
                return None

            data_section = response_json['data']
            
            if current_page == 1:
                total_pages = data_section.get('pages', 1)
                total_records_count = data_section.get('total', 0)
                print(f"📊 首次请求成功，发现总共有 {total_pages} 页数据 ({total_records_count} 条)。")

            records = data_section['records']
            
            # --- 日志记录逻辑 ---
            if records:
                first_id = records[0]['id']
                last_id = records[-1]['id']
                first_time = records[0].get('create_time', 'N/A')
                last_time = records[-1].get('create_time', 'N/A')
                num_records = len(records)
                
                print(f"✅ 已获取第 {current_page}/{total_pages} 页, 记录数: {num_records}")
                print(f"   ID 范围: {first_id} -> {last_id}")
                print(f"   时间范围: {first_time} -> {last_time}")
            else:
                 print(f"✅ 已获取第 {current_page}/{total_pages} 页, 记录数: 0 (结束或空页)")
            # ---------------------------

            all_records.extend(records)
            
            if not records and current_page > 1:
                break

            current_page += 1
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            print(f"\n❌ HTTP错误，可能授权失败。状态码: {e.response.status_code}")
            print(f"服务器响应: {e.response.text[:200]}...")
            return None
        except requests.exceptions.RequestException as e:
            print(f"\n❌ 请求失败: {e}")
            return None
        except KeyError as e:
            print(f"❌ 记录结构错误: 缺少键 {e}。无法打印范围信息。")
            return None


    print(f"\n🎉 所有 {len(all_records)} 条记录获取完毕。")
    return all_records

# ====================================================================
# D. 交易指标分析函数 (保持不变)
# ====================================================================

def transaction_metrics_analysis(sorted_data, category_totals):
    """
    计算并打印关键的交易指标，如总投注额、净投注额、派彩额、总流水等。
    """
    print("\n--- 5. 交易指标 (高级指标分析) ---")
    
    # 使用全局定义的类型集合
    metrics = defaultdict(float)
    
    metrics['Total_Wagered'] = 0.0
    for name in WAGERING_TYPES:
        metrics['Total_Wagered'] += abs(category_totals.get(name, 0.0))
        
    metrics['Total_Cancelled_Wagered'] = 0.0
    for name in CANCELLATION_TYPES:
        metrics['Total_Cancelled_Wagered'] += abs(category_totals.get(name, 0.0))

    metrics['Net_Wagered'] = round(metrics['Total_Wagered'] - metrics['Total_Cancelled_Wagered'], 2)

    metrics['Total_Paid_Out'] = 0.0
    for name in WINNINGS_TYPES:
        metrics['Total_Paid_Out'] += category_totals.get(name, 0.0)

    # 计算总流水
    metrics['Total_Turnover'] = 0.0
    for balance in category_totals.values():
        metrics['Total_Turnover'] += abs(balance)

    metrics['Net_PnL_Game'] = round(metrics['Total_Paid_Out'] - metrics['Net_Wagered'], 2)
    
    print("-" * 50)
    print("📈 游戏核心指标：")
    print(f"  下注总额 (Gross Wagered):{metrics['Total_Wagered']:>15.2f}")
    print(f"  取消下注总额:         -{metrics['Total_Cancelled_Wagered']:>14.2f}")
    print(f"  **净投注额 (Net Wagered):**{metrics['Net_Wagered']:>15.2f}")
    print("-" * 50)
    print(f"  总派彩额 (Paid Out):  {metrics['Total_Paid_Out']:>15.2f}")
    print(f"  **游戏净盈亏 (PnL):** {metrics['Net_PnL_Game']:>15.2f}")
    print("-" * 50)
    
    print(f"📊 所有交易总流水:    {metrics['Total_Turnover']:>15.2f}")
    print("-" * 50)


# ====================================================================
# E. 交易日期维度分析函数 (最终修改：包含所有交易类型)
# ====================================================================

def time_series_analysis(sorted_data, all_transaction_types):
    """
    按日期计算核心指标，并列出所有非核心游戏类型的每日总额。
    """
    print("\n--- 6. 交易日期维度分析 ---")
    
    daily_metrics = defaultdict(lambda: defaultdict(float))

    # 找出除了核心游戏指标之外的所有交易类型
    other_types_list = sorted(list(all_transaction_types - CORE_TYPES))
    
    # ------------------ 1. 数据聚合 ------------------
    for record in sorted_data:
        try:
            dt = datetime.strptime(record['create_time'], DATE_FORMAT)
        except (ValueError, KeyError):
            continue 

        date_key = dt.strftime("%Y-%m-%d")
        balance = record['balance']
        dict_name = record['dict_name']
        
        # 统计每日的笔数
        daily_metrics[date_key]['Count'] += 1
        
        # 统计核心游戏指标
        if dict_name in WAGERING_TYPES:
            daily_metrics[date_key]['Gross_Wagered'] += abs(balance)
        elif dict_name in CANCELLATION_TYPES:
            daily_metrics[date_key]['Cancelled_Wagered'] += abs(balance)
        elif dict_name in WINNINGS_TYPES:
            daily_metrics[date_key]['Paid_Out'] += balance
        
        # 统计其他所有交易类型 (使用其名称作为键)
        if dict_name not in CORE_TYPES:
            daily_metrics[date_key][dict_name] += balance

    
    # ------------------ 2. 报告输出 ------------------
    print("\n📅 每日核心指标统计 (包含所有其他交易类型)：")
    
    # 动态构建表头
    column_width = 12
    date_width = 10
    count_width = 6
    
    header_core = (
        f"{'日期':<{date_width}} | {'笔数':<{count_width}} | "
        f"{'净投注额':>{column_width}} | {'总派彩额':>{column_width}} | {'净盈亏(PnL)':>{column_width}}"
    )
    
    header_other = ""
    for t in other_types_list:
        header_other += f" | {t:>{column_width}}"
        
    print("-" * (len(header_core) + len(header_other) + 3)) 
    print(header_core + header_other)
    print("-" * (len(header_core) + len(header_other) + 3))
    
    # 打印数据行
    for date_key in sorted(daily_metrics.keys()):
        m = daily_metrics[date_key]
        
        gross_wagered = m['Gross_Wagered']
        cancelled_wagered = m['Cancelled_Wagered']
        paid_out = m['Paid_Out']
        
        net_wagered = round(gross_wagered - cancelled_wagered, 2)
        net_pnl = round(paid_out - net_wagered, 2)
        
        # 核心数据列
        row_str = (
            f"{date_key:<{date_width}} | {int(m['Count']):<{count_width}} | "
            f"{net_wagered:>{column_width}.2f} | {paid_out:>{column_width}.2f} | {net_pnl:>{column_width}.2f}"
        )
        
        # 其他交易类型数据列
        for t in other_types_list:
            row_str += f" | {m[t]:>{column_width}.2f}"

        print(row_str)
        
    print("-" * (len(header_core) + len(header_other) + 3))


# ====================================================================
# F. 数据分析与一致性检查函数 (修改 C 部分，传递所有交易类型)
# ====================================================================

def analyze_and_check_consistency_full(data_list):
    """
    检查交易记录列表的余额一致性，并生成详细的交易报告。
    """
    if not data_list:
        print("无数据可供分析。")
        return False

    # --- 0. 数据去重与清洗 ---
    print("\n--- 0. 数据去重与清洗 ---")
    unique_records = {}
    for record in data_list:
        key = tuple(record.get(k) for k in ['id', 'create_time', 'balance', 'before_balance'])
        unique_records[key] = record
    
    data_list_unique = list(unique_records.values())

    print(f"🔄 原始记录数: {len(data_list)}, 去重后记录数: {len(data_list_unique)}")
    
    if len(data_list_unique) == 0:
        return False
    
    # --- 1. ID排序 (只按 ID 升序) ---
    print("\n--- 1. 排序数据 (只按 ID 升序) ---")
    
    try:
        sorted_data = sorted(data_list_unique, key=lambda x: x['id']) 
        
        print("🔍 排序后的前10条记录样本（验证 ID 升序）：")
        for i in range(min(10, len(sorted_data))):
             r = sorted_data[i]
             after_bal = round(r['before_balance'] + r['balance'], 2)
             print(f"  [{i+1:>2}] ID:{r['id']} Time:{r.get('create_time', 'N/A')} Before:{r['before_balance']:.2f} Balance:{r['balance']:+.2f} After:{after_bal:.2f}")
             
    except KeyError as e:
        print(f"❌ **键错误**: 记录中缺少必要字段 ({e})，无法排序。")
        return False

    # 2. 迭代检查余额一致性
    print("\n--- 2. 迭代检查余额一致性 ---")
    inconsistent_records = []
    is_consistent = True
    
    for i in range(len(sorted_data) - 1):
        current = sorted_data[i]
        next_record = sorted_data[i+1]
        
        calculated_after_balance = round(current['before_balance'] + current['balance'], 2)
        actual_before_balance_next = next_record['before_balance']
        
        if calculated_after_balance != actual_before_balance_next:
            is_consistent = False
            
            if len(inconsistent_records) < 10: 
                print(f"🛑 不一致点 {i+1}/{len(sorted_data) - 1}: ID {current['id']} -> ID {next_record['id']}")
                print(f"  计算值: {calculated_after_balance:.2f} vs 实际值: {actual_before_balance_next:.2f}, 差异: {round(actual_before_balance_next - calculated_after_balance, 2):+.2f}")

            inconsistent_records.append({"current_id": current['id'], "next_id": next_record['id'], "discrepancy": round(actual_before_balance_next - calculated_after_balance, 2)})

    # 打印最终一致性检查结果
    print("\n--- 2. 一致性检查结果 ---")
    if not is_consistent:
        print(f"❌ **一致性检查失败**: 共发现 {len(inconsistent_records)} 处不一致。")
        pprint(inconsistent_records[:10])
        return False
    print("✅ **一致性检查通过**: 所有交易记录的余额字段都前后衔接。")

    # 3. 交易类型 (dict_name) 检测和汇总 (用于生成步骤 3和5，并提取所有类型名称)
    print("\n--- 3. 交易类型及金额汇总 ---")
    category_totals = defaultdict(float)
    category_counts = defaultdict(int)
    all_transaction_types = set()
    
    for record in sorted_data:
        dict_name = record['dict_name']
        balance = record['balance']
        category_totals[dict_name] = round(category_totals[dict_name] + balance, 2)
        category_counts[dict_name] += 1
        all_transaction_types.add(dict_name) # 收集所有类型名称
        
    print("📋 **发现的交易类型 (Dict Name) 列表及统计：**")
    print("-" * 40)
    print(f"{'交易类型':<12} | {'笔数':<5} | {'总金额变动':>10}")
    print("-" * 40)
    
    for name in sorted(category_totals.keys()):
        total = category_totals[name]
        count = category_counts[name]
        print(f"{name:<12} | {count:<5} | {total:+.2f}")

    # 4. 总结余额变动和完整性验证
    print("\n--- 4. 余额变动总结 ---")
    
    start_balance = sorted_data[0]['before_balance']
    end_balance = round(sorted_data[-1]['before_balance'] + sorted_data[-1]['balance'], 2)
    total_net_change = round(end_balance - start_balance, 2)
    sum_of_all_balances = round(sum(category_totals.values()), 2)
    
    print(f"💰 初始余额: {start_balance:.2f}")
    print(f"💶 最终余额: {end_balance:.2f}")
    print(f"📈 总余额净变动: {total_net_change:+.2f}")
    
    if sum_of_all_balances == total_net_change:
        print(f"✅ 汇总金额 ({sum_of_all_balances:+.2f}) 与净变动一致。")
    else:
        print(f"⚠️ 汇总金额 ({sum_of_all_balances:+.2f}) 与净变动不一致 ({total_net_change:+.2f})！")
        print(f"差异金额: {round(sum_of_all_balances - total_net_change, 2):+.2f} (很可能缺失了交易类型)")

    # 5. 调用交易指标分析
    transaction_metrics_analysis(sorted_data, category_totals)

    # 6. 新增：调用日期维度分析，并传递所有交易类型名称
    time_series_analysis(sorted_data, all_transaction_types)

    return True


# ====================================================================
# G. 主执行逻辑
# ====================================================================

if __name__ == "__main__":
    transaction_data = fetch_all_transaction_records(BASE_URL, PAYLOAD_TEMPLATE, HEADERS)
    
    if transaction_data:
        print("\n" + "="*50)
        print(f"开始分析 {len(transaction_data)} 条原始交易记录...")
        print("="*50)
        analyze_and_check_consistency_full(transaction_data)
    else:
        print("\n分析终止，未能成功获取交易数据。请检查API URL、授权令牌或网络设置。")
