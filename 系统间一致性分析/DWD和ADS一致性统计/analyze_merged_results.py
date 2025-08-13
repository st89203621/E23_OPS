#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析合并后的ODS、DWD、ADS数据一致性结果
"""

import csv
import glob
import os
from typing import Dict, List

def find_latest_merged_file() -> str:
    """查找最新的合并结果文件"""
    pattern = "output/merged_ods_dwd_ads_consistency_*.csv"
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"未找到合并结果文件: {pattern}")
    return max(files, key=os.path.getctime)

def analyze_results(csv_file: str):
    """分析结果数据"""
    print("📊 ODS、DWD、ADS三方数据一致性分析报告")
    print("=" * 80)
    print(f"📁 数据文件: {csv_file}")
    print(f"📅 查询日期: 2025-08-09")
    print("=" * 80)
    
    # 统计变量
    total_protocols = 0
    ods_available = 0
    dwd_available = 0
    ads_available = 0
    
    perfect_consistency = 0  # 三方完全一致
    good_consistency = 0     # 一致性 >= 95%
    warning_consistency = 0  # 一致性 >= 90%
    poor_consistency = 0     # 一致性 < 90%
    
    # 详细数据
    protocols_data = []
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            total_protocols += 1
            protocol = row['protocol']
            
            # 检查数据可用性
            has_ods = bool(row['ods_total'].strip())
            has_dwd = bool(row['dwd_total'].strip())
            has_ads = bool(row['ads_total'].strip())
            
            if has_ods:
                ods_available += 1
            if has_dwd:
                dwd_available += 1
            if has_ads:
                ads_available += 1
            
            # 计算一致性状态
            ods_dwd_rate = None  # 这个字段在当前CSV中不存在
            ods_ads_rate = float(row['ods_ads_consistency_rate']) if row['ods_ads_consistency_rate'] else None
            dwd_ads_rate = float(row['dwd_ads_consistency_rate']) if row['dwd_ads_consistency_rate'] else None
            
            # 计算最低一致性率
            rates = [r for r in [ods_dwd_rate, ods_ads_rate, dwd_ads_rate] if r is not None]
            min_rate = min(rates) if rates else None
            
            if min_rate is not None:
                if min_rate == 100.0:
                    perfect_consistency += 1
                    status = "🟢 完美"
                elif min_rate >= 95.0:
                    good_consistency += 1
                    status = "🟢 良好"
                elif min_rate >= 90.0:
                    warning_consistency += 1
                    status = "🟡 警告"
                else:
                    poor_consistency += 1
                    status = "🔴 差"
            else:
                status = "❓ 未知"
            
            protocols_data.append({
                'protocol': protocol,
                'has_ods': has_ods,
                'has_dwd': has_dwd,
                'has_ads': has_ads,
                'ods_total': row['ods_total'],
                'dwd_total': row['dwd_total'],
                'ads_total': row['ads_total'],
                'ods_dwd_rate': ods_dwd_rate,
                'ods_ads_rate': ods_ads_rate,
                'dwd_ads_rate': dwd_ads_rate,
                'min_rate': min_rate,
                'status': status
            })
    
    # 打印统计摘要
    print(f"\n📈 数据可用性统计:")
    print(f"  总协议数: {total_protocols}")
    print(f"  ODS数据可用: {ods_available} ({ods_available/total_protocols*100:.1f}%)")
    print(f"  DWD数据可用: {dwd_available} ({dwd_available/total_protocols*100:.1f}%)")
    print(f"  ADS数据可用: {ads_available} ({ads_available/total_protocols*100:.1f}%)")
    
    print(f"\n📊 一致性统计:")
    print(f"  🟢 完美一致 (100%): {perfect_consistency} ({perfect_consistency/total_protocols*100:.1f}%)")
    print(f"  🟢 良好一致 (≥95%): {good_consistency} ({good_consistency/total_protocols*100:.1f}%)")
    print(f"  🟡 警告一致 (≥90%): {warning_consistency} ({warning_consistency/total_protocols*100:.1f}%)")
    print(f"  🔴 差一致 (<90%): {poor_consistency} ({poor_consistency/total_protocols*100:.1f}%)")
    
    # 打印详细协议信息
    print(f"\n📋 协议详细信息:")
    print("-" * 80)
    print(f"{'协议':<20} {'ODS':<12} {'DWD':<12} {'ADS':<12} {'最低一致性':<10} {'状态'}")
    print("-" * 80)
    
    for data in sorted(protocols_data, key=lambda x: x['min_rate'] if x['min_rate'] else -1):
        ods_str = f"{int(data['ods_total']):,}" if data['ods_total'] else "N/A"
        dwd_str = f"{int(data['dwd_total']):,}" if data['dwd_total'] else "N/A"
        ads_str = f"{int(data['ads_total']):,}" if data['ads_total'] else "N/A"
        rate_str = f"{data['min_rate']:.1f}%" if data['min_rate'] is not None else "N/A"
        
        print(f"{data['protocol']:<20} {ods_str:<12} {dwd_str:<12} {ads_str:<12} {rate_str:<10} {data['status']}")

def main():
    """主函数"""
    try:
        merged_file = find_latest_merged_file()
        analyze_results(merged_file)
    except FileNotFoundError as e:
        print(f"❌ 错误: {e}")
    except Exception as e:
        print(f"❌ 分析过程中出错: {e}")

if __name__ == "__main__":
    main()
