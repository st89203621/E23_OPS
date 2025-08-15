#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
关联率登记CSV文件更新脚本
功能：从output目录中的数据自动更新现场数据统计指标定期登记-关联率.csv文件
作者：AI Assistant
创建时间：2025-08-15
使用方法：python update_csv_simple.py
"""

import os
import csv
from datetime import datetime, timedelta

def read_output_data(date_str):
    """读取指定日期的output数据"""
    output_file = f"output/{date_str}/radius_connect_{date_str}.csv"
    
    if not os.path.exists(output_file):
        print(f"文件不存在: {output_file}")
        return None
    
    data = {}
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[1:], 1):  # 跳过标题行
                parts = line.strip().split(',')
                if len(parts) >= 7:
                    stat_type = parts[0]
                    uparea_id = parts[3]  # 第4列是uparea_id
                    success_count = int(parts[4])  # 第5列是关联成功数
                    total_count = int(parts[5])    # 第6列是关联总数
                    rate = float(parts[6])         # 第7列是关联率
                    
                    # 存储数据
                    key = f"{stat_type}_{uparea_id}"
                    data[key] = {
                        'success': success_count,
                        'total': total_count,
                        'rate': rate
                    }
    except Exception as e:
        print(f"读取文件 {output_file} 时出错: {e}")
        return None
    
    return data

def update_csv_manually():
    """
    更新CSV文件的主函数
    注意：如需更新其他日期，请修改dates列表中的日期
    """

    # 需要更新的日期列表（可根据需要修改）
    dates = ["2025-08-10", "2025-08-11", "2025-08-12", "2025-08-13", "2025-08-14"]
    all_data = {}
    
    for date_str in dates:
        data = read_output_data(date_str)
        if data:
            all_data[date_str] = data
            print(f"成功读取 {date_str} 的数据")
    
    if not all_data:
        print("没有可用数据")
        return
    
    # 读取原始CSV文件
    with open("现场数据统计指标定期登记-关联率.csv", 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
    
    # 备份原文件
    backup_file = f"现场数据统计指标定期登记-关联率_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(backup_file, 'w', encoding='utf-8-sig') as f:
        f.writelines(lines)
    print(f"原文件已备份为: {backup_file}")
    
    # 更新第一行（日期标题）
    first_line = lines[0].strip()
    # 在开头插入新日期
    new_dates_header = "2025/8/14,,,2025/8/13,,,2025/8/12,,,2025/8/11,,,2025/8/10,,,"
    # 找到第一个逗号后的位置插入
    parts = first_line.split(',', 5)  # 分割前5个字段
    if len(parts) >= 5:
        lines[0] = f"{parts[0]},{parts[1]},{parts[2]},{parts[3]},{parts[4]},{new_dates_header}{parts[5] if len(parts) > 5 else ''}\n"
    
    # 更新第二行（列标题）
    second_line = lines[1].strip()
    new_headers = "百分比,分子,分母,百分比,分子,分母,百分比,分子,分母,百分比,分子,分母,百分比,分子,分母,"
    parts = second_line.split(',', 5)
    if len(parts) >= 5:
        lines[1] = f"{parts[0]},{parts[1]},{parts[2]},{parts[3]},{parts[4]},{new_headers}{parts[5] if len(parts) > 5 else ''}\n"
    
    # 定义数据映射
    data_mapping = [
        # (行号, 类型, 指标, 站点)
        (3, "NF固网", "关联率", "A1", "210213"),
        (4, "NF固网", "关联率", "B1", "220214"),
        (5, "NF固网", "关联率", "C1", "230215"),
        (6, "NF固网", "准确率", "A1", "210213"),
        (7, "NF固网", "准确率", "B1", "220214"),
        (8, "NF固网", "准确率", "C1", "230215"),
        (12, "NF移网", "关联率", "A1", "210213"),
        (13, "NF移网", "关联率", "B1", "220214"),
        (14, "NF移网", "关联率", "C1", "230215"),
        (15, "NF移网", "准确率", "A1", "210213"),
        (16, "NF移网", "准确率", "B1", "220214"),
        (17, "NF移网", "准确率", "C1", "230215"),
        (22, "PR固网", "关联率", "A1", "210213"),
        (23, "PR固网", "关联率", "B1", "220214"),
        (24, "PR固网", "关联率", "C1", "230215"),
        (25, "PR固网", "准确率", "A1", "210213"),
        (26, "PR固网", "准确率", "B1", "220214"),
        (27, "PR固网", "准确率", "C1", "230215"),
        (32, "PR移网", "关联率", "A1", "210213"),
        (33, "PR移网", "关联率", "B1", "220214"),
        (34, "PR移网", "关联率", "C1", "230215"),
        (35, "PR移网", "准确率", "A1", "210213"),
        (36, "PR移网", "准确率", "B1", "220214"),
        (37, "PR移网", "准确率", "C1", "230215"),
    ]
    
    # 更新数据行
    for row_idx, type_name, indicator, site, uparea_id in data_mapping:
        if row_idx < len(lines):
            line = lines[row_idx].strip()
            parts = line.split(',')
            
            # 为每个日期添加数据
            new_data = []
            for date_str in dates:
                if date_str in all_data:
                    data = all_data[date_str]
                    
                    # 构建查找键
                    if type_name == "NF固网":
                        if indicator == "关联率":
                            key = f"nf固网关联率_{uparea_id}"
                        else:
                            key = f"nf固网关联准确率_{uparea_id}"
                    elif type_name == "NF移网":
                        if indicator == "关联率":
                            key = f"nf移网关联率_{uparea_id}"
                        else:
                            key = f"nf移网关联准确率_{uparea_id}"
                    elif type_name == "PR固网":
                        if indicator == "关联率":
                            key = f"pr固网关联率_{uparea_id}"
                        else:
                            key = f"pr固网关联准确率_{uparea_id}"
                    elif type_name == "PR移网":
                        if indicator == "关联率":
                            key = f"pr移网关联率_{uparea_id}"
                        else:
                            key = f"pr移网关联准确率_{uparea_id}"
                    
                    if key in data:
                        item = data[key]
                        rate_val = f"{item['rate']:.2%}"
                        success_val = str(item['success'])
                        total_val = str(item['total'])
                        new_data.extend([rate_val, success_val, total_val])
                        print(f"填充数据: 行{row_idx} {type_name}-{indicator}-{site} {date_str} -> {rate_val}")
                    else:
                        new_data.extend(["", "", ""])
                        print(f"未找到数据: {key}")
                else:
                    new_data.extend(["", "", ""])
            
            # 插入新数据到第5列之后
            if len(parts) >= 5:
                new_parts = parts[:5] + new_data + parts[5:]
                lines[row_idx] = ','.join(new_parts) + '\n'
    
    # 写入更新后的文件
    with open("现场数据统计指标定期登记-关联率.csv", 'w', encoding='utf-8-sig') as f:
        f.writelines(lines)
    
    print("CSV文件更新完成！")

if __name__ == "__main__":
    # 切换到脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    print("开始更新关联率登记CSV文件...")
    update_csv_manually()
    print("更新完成！")
