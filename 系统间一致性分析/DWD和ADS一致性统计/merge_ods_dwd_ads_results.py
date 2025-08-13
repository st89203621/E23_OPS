#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将DWD和ADS的数据结果补充到ODS的CSV文件中
"""

import csv
import os
import glob
from datetime import datetime
from typing import Dict, List, Optional

def find_latest_csv_file(pattern: str) -> Optional[str]:
    """查找最新的CSV文件"""
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getctime)

def read_dwd_ads_results(csv_file: str) -> Dict[str, Dict]:
    """读取DWD和ADS的结果数据"""
    results = {}
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            protocol = row['protocol']
            results[protocol] = {
                'dwd_total': row['hive_total'],
                'ads_total': row['es_total'],
                'dwd_ads_diff': row['total_diff'],
                'dwd_ads_consistency_rate': row['consistency_rate'],
                'dwd_ads_status': row['status'],
                'dwd_ads_duration': row['duration'],
                'dwd_ads_error': row['error']
            }
    
    return results

def create_merged_csv(ods_csv_file: str, dwd_ads_results: Dict[str, Dict], output_file: str):
    """创建合并后的CSV文件"""
    
    # 定义新的字段名
    fieldnames = [
        'protocol', 'query_date',
        'ods_total', 'dwd_total', 'ads_total',
        'ods_dwd_diff', 'ods_ads_diff', 'dwd_ads_diff',
        'ods_dwd_consistency_rate', 'ods_ads_consistency_rate', 'dwd_ads_consistency_rate',
        'ods_dwd_status', 'ods_ads_status', 'dwd_ads_status',
        'ods_duration', 'dwd_ads_duration',
        'ods_error', 'dwd_ads_error'
    ]
    
    merged_data = []
    
    # 如果ODS文件存在，读取ODS数据
    if os.path.exists(ods_csv_file):
        with open(ods_csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                protocol = row['protocol']
                
                # 创建合并后的行数据
                merged_row = {
                    'protocol': protocol,
                    'query_date': row['query_date'],
                    'ods_total': row.get('ods_total', ''),
                    'dwd_total': '',
                    'ads_total': '',
                    'ods_dwd_diff': row.get('ods_dwd_diff', ''),
                    'ods_ads_diff': row.get('ods_ads_diff', ''),
                    'dwd_ads_diff': '',
                    'ods_dwd_consistency_rate': row.get('ods_dwd_consistency_rate', ''),
                    'ods_ads_consistency_rate': row.get('ods_ads_consistency_rate', ''),
                    'dwd_ads_consistency_rate': '',
                    'ods_dwd_status': row.get('ods_dwd_status', ''),
                    'ods_ads_status': row.get('ods_ads_status', ''),
                    'dwd_ads_status': '',
                    'ods_duration': row.get('duration', ''),
                    'dwd_ads_duration': '',
                    'ods_error': row.get('error', ''),
                    'dwd_ads_error': ''
                }
                
                # 如果有对应的DWD和ADS数据，补充进去
                if protocol in dwd_ads_results:
                    dwd_ads_data = dwd_ads_results[protocol]
                    merged_row.update({
                        'dwd_total': dwd_ads_data['dwd_total'],
                        'ads_total': dwd_ads_data['ads_total'],
                        'dwd_ads_diff': dwd_ads_data['dwd_ads_diff'],
                        'dwd_ads_consistency_rate': dwd_ads_data['dwd_ads_consistency_rate'],
                        'dwd_ads_status': dwd_ads_data['dwd_ads_status'],
                        'dwd_ads_duration': dwd_ads_data['dwd_ads_duration'],
                        'dwd_ads_error': dwd_ads_data['dwd_ads_error']
                    })
                
                merged_data.append(merged_row)
    
    # 添加只在DWD和ADS中存在但不在ODS中的协议
    if os.path.exists(ods_csv_file):
        existing_protocols = {row['protocol'] for row in merged_data}
    else:
        existing_protocols = set()
    
    for protocol, dwd_ads_data in dwd_ads_results.items():
        if protocol not in existing_protocols:
            merged_row = {
                'protocol': protocol,
                'query_date': '2025-08-09',  # 从DWD和ADS数据中获取
                'ods_total': '',
                'dwd_total': dwd_ads_data['dwd_total'],
                'ads_total': dwd_ads_data['ads_total'],
                'ods_dwd_diff': '',
                'ods_ads_diff': '',
                'dwd_ads_diff': dwd_ads_data['dwd_ads_diff'],
                'ods_dwd_consistency_rate': '',
                'ods_ads_consistency_rate': '',
                'dwd_ads_consistency_rate': dwd_ads_data['dwd_ads_consistency_rate'],
                'ods_dwd_status': '',
                'ods_ads_status': '',
                'dwd_ads_status': dwd_ads_data['dwd_ads_status'],
                'ods_duration': '',
                'dwd_ads_duration': dwd_ads_data['dwd_ads_duration'],
                'ods_error': '',
                'dwd_ads_error': dwd_ads_data['dwd_ads_error']
            }
            merged_data.append(merged_row)
    
    # 写入合并后的CSV文件
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged_data)
    
    return len(merged_data)

def main():
    """主函数"""
    print("🔄 开始合并ODS、DWD和ADS数据结果...")
    print("=" * 60)
    
    # 查找最新的DWD和ADS结果文件
    dwd_ads_pattern = "output/dwd_ads_consistency_concurrent_*.csv"
    dwd_ads_file = find_latest_csv_file(dwd_ads_pattern)
    
    if not dwd_ads_file:
        print(f"❌ 未找到DWD和ADS结果文件: {dwd_ads_pattern}")
        return
    
    print(f"📁 找到DWD和ADS结果文件: {dwd_ads_file}")
    
    # 查找最新的ODS结果文件
    ods_pattern = "output/ods_dwd_ads_consistency_concurrent_*.csv"
    ods_file = find_latest_csv_file(ods_pattern)
    
    if ods_file:
        print(f"📁 找到ODS结果文件: {ods_file}")
    else:
        print("⚠️  未找到ODS结果文件，将只使用DWD和ADS数据")
    
    # 读取DWD和ADS结果
    print("📖 读取DWD和ADS数据...")
    dwd_ads_results = read_dwd_ads_results(dwd_ads_file)
    print(f"✅ 读取到 {len(dwd_ads_results)} 个协议的DWD和ADS数据")
    
    # 生成输出文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"output/merged_ods_dwd_ads_consistency_{timestamp}.csv"
    
    # 创建合并后的CSV文件
    print("🔄 合并数据...")
    total_rows = create_merged_csv(ods_file if ods_file else "", dwd_ads_results, output_file)
    
    print(f"✅ 合并完成！")
    print(f"📊 总共处理 {total_rows} 个协议")
    print(f"💾 输出文件: {output_file}")
    
    # 显示文件内容预览
    print(f"\n📋 文件内容预览:")
    print("-" * 60)
    with open(output_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for i, line in enumerate(lines[:6]):  # 显示前5行数据
            print(f"{i+1:2d}: {line.strip()}")
        if len(lines) > 6:
            print(f"... 还有 {len(lines) - 6} 行数据")

if __name__ == "__main__":
    main()
