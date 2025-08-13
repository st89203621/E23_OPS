#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的ODS、DWD、ADS数据一致性结果分析
"""

import csv
import glob
import os

def find_latest_merged_file() -> str:
    """查找最新的合并结果文件"""
    pattern = "output/merged_ods_dwd_ads_consistency_*.csv"
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"未找到合并结果文件: {pattern}")
    return max(files, key=os.path.getctime)

def main():
    """主函数"""
    try:
        merged_file = find_latest_merged_file()
        print("📊 ODS、DWD、ADS三方数据一致性分析报告")
        print("=" * 80)
        print(f"📁 数据文件: {merged_file}")
        print("=" * 80)
        
        # 统计变量
        total_protocols = 0
        ods_available = 0
        dwd_available = 0
        ads_available = 0
        
        protocols_data = []
        
        with open(merged_file, 'r', encoding='utf-8') as f:
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
                
                # 获取一致性率
                ods_ads_rate = float(row['ods_ads_consistency_rate']) if row['ods_ads_consistency_rate'] else None
                dwd_ads_rate = float(row['dwd_ads_consistency_rate']) if row['dwd_ads_consistency_rate'] else None
                
                protocols_data.append({
                    'protocol': protocol,
                    'has_ods': has_ods,
                    'has_dwd': has_dwd,
                    'has_ads': has_ads,
                    'ods_total': row['ods_total'],
                    'dwd_total': row['dwd_total'],
                    'ads_total': row['ads_total'],
                    'ods_ads_rate': ods_ads_rate,
                    'dwd_ads_rate': dwd_ads_rate,
                })
        
        # 打印统计摘要
        print(f"\n📈 数据可用性统计:")
        print(f"  总协议数: {total_protocols}")
        print(f"  ODS数据可用: {ods_available} ({ods_available/total_protocols*100:.1f}%)")
        print(f"  DWD数据可用: {dwd_available} ({dwd_available/total_protocols*100:.1f}%)")
        print(f"  ADS数据可用: {ads_available} ({ads_available/total_protocols*100:.1f}%)")
        
        # 打印详细协议信息
        print(f"\n📋 协议详细信息:")
        print("-" * 100)
        print(f"{'协议':<20} {'ODS数据量':<15} {'DWD数据量':<15} {'ADS数据量':<15} {'ODS-ADS一致性':<12} {'DWD-ADS一致性'}")
        print("-" * 100)
        
        for data in protocols_data:
            ods_str = f"{int(data['ods_total']):,}" if data['ods_total'] else "N/A"
            dwd_str = f"{int(data['dwd_total']):,}" if data['dwd_total'] else "N/A"
            ads_str = f"{int(data['ads_total']):,}" if data['ads_total'] else "N/A"
            ods_ads_str = f"{data['ods_ads_rate']:.2f}%" if data['ods_ads_rate'] is not None else "N/A"
            dwd_ads_str = f"{data['dwd_ads_rate']:.2f}%" if data['dwd_ads_rate'] is not None else "N/A"
            
            print(f"{data['protocol']:<20} {ods_str:<15} {dwd_str:<15} {ads_str:<15} {ods_ads_str:<12} {dwd_ads_str}")
        
        # 找出需要关注的协议
        print(f"\n⚠️  需要关注的协议 (一致性 < 95%):")
        print("-" * 50)
        
        problem_found = False
        for data in protocols_data:
            issues = []
            if data['ods_ads_rate'] is not None and data['ods_ads_rate'] < 95.0:
                issues.append(f"ODS-ADS: {data['ods_ads_rate']:.2f}%")
            if data['dwd_ads_rate'] is not None and data['dwd_ads_rate'] < 95.0:
                issues.append(f"DWD-ADS: {data['dwd_ads_rate']:.2f}%")
            
            if issues:
                problem_found = True
                print(f"  {data['protocol']}: {', '.join(issues)}")
        
        if not problem_found:
            print("  🎉 所有协议一致性都很好！")
        
        # 找出缺失数据的协议
        print(f"\n❓ 数据缺失的协议:")
        print("-" * 40)
        
        missing_ods = [d['protocol'] for d in protocols_data if not d['has_ods']]
        missing_dwd = [d['protocol'] for d in protocols_data if not d['has_dwd']]
        missing_ads = [d['protocol'] for d in protocols_data if not d['has_ads']]
        
        if missing_ods:
            print(f"  缺失ODS数据: {', '.join(missing_ods)}")
        if missing_dwd:
            print(f"  缺失DWD数据: {', '.join(missing_dwd)}")
        if missing_ads:
            print(f"  缺失ADS数据: {', '.join(missing_ads)}")
        
        if not (missing_ods or missing_dwd or missing_ads):
            print("  🎉 所有协议都有完整的三方数据！")
            
    except FileNotFoundError as e:
        print(f"❌ 错误: {e}")
    except Exception as e:
        print(f"❌ 分析过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
