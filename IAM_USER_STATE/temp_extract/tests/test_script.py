#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试脚本 - 用于验证用户流量统计脚本的基本功能
"""

import os
import sys
import pandas as pd
from user_flow_stats import UserFlowStatsProcessor
import config


def create_test_excel():
    """创建测试用的Excel文件"""
    test_data = {
        '局点名称': ['测试局点A', '测试局点B', '测试局点C'],
        'IP地址': ['192.168.1.100', '192.168.1.101', '192.168.1.102']
    }
    
    df = pd.DataFrame(test_data)
    test_file_path = os.path.join(os.path.dirname(__file__), 'test_config.xlsx')
    df.to_excel(test_file_path, index=False, engine='openpyxl')
    
    print(f"✅ 创建测试Excel文件: {test_file_path}")
    return test_file_path


def test_excel_reading():
    """测试Excel文件读取功能"""
    print("\n🧪 测试Excel文件读取功能...")
    
    try:
        # 创建测试文件
        test_file = create_test_excel()
        
        # 测试读取
        processor = UserFlowStatsProcessor()
        config_data = processor.read_excel_config(test_file)
        
        print(f"✅ 成功读取 {len(config_data)} 条配置")
        for item in config_data:
            print(f"   - {item['station_name']}: {item['ip_address']}")
        
        # 清理测试文件
        os.remove(test_file)
        print("✅ Excel读取测试通过")
        
    except Exception as e:
        print(f"❌ Excel读取测试失败: {str(e)}")


def test_data_processing():
    """测试数据处理功能"""
    print("\n🧪 测试数据处理功能...")
    
    try:
        # 创建测试数据
        test_data = [
            {'id': '001', 'name': '用户A', 'total': 1000, 'source_ip': '192.168.1.100', 'station_name': '局点A'},
            {'id': '002', 'name': '用户B', 'total': 2000, 'source_ip': '192.168.1.101', 'station_name': '局点B'},
            {'id': '001', 'name': '用户A', 'total': 800, 'source_ip': '192.168.1.102', 'station_name': '局点C'},  # 重复用户，流量较小
            {'id': '003', 'name': '用户C', 'total': 1500, 'source_ip': '192.168.1.100', 'station_name': '局点A'},
            {'id': '004', 'name': '用户D', 'total': 500, 'source_ip': '192.168.1.101', 'station_name': '局点B'},
        ]
        
        processor = UserFlowStatsProcessor()
        processed_data = processor.process_user_data(test_data)
        
        print(f"✅ 处理前数据: {len(test_data)} 条")
        print(f"✅ 处理后数据: {len(processed_data)} 条")
        
        # 验证排序
        if len(processed_data) > 1:
            for i in range(len(processed_data) - 1):
                if processed_data[i]['total'] < processed_data[i + 1]['total']:
                    raise ValueError("数据排序错误")
        
        print("✅ 数据处理测试通过")
        
        # 显示处理结果
        print("\n📊 处理结果:")
        for i, user in enumerate(processed_data, 1):
            print(f"   {i}. {user['name']} - {user['total']}MB ({user['station_name']})")
        
    except Exception as e:
        print(f"❌ 数据处理测试失败: {str(e)}")


def test_excel_output():
    """测试Excel输出功能"""
    print("\n🧪 测试Excel输出功能...")
    
    try:
        # 创建测试数据
        test_data = [
            {
                'id': '001',
                'name': '测试用户A',
                'group': '管理员',
                'ip': '192.168.1.10',
                'up': 500.5,
                'down': 1500.8,
                'total': 2001.3,
                'session': 5,
                'status': '在线',
                'source_ip': '192.168.1.100',
                'station_name': '测试局点A'
            },
            {
                'id': '002',
                'name': '测试用户B',
                'group': '普通用户',
                'ip': '192.168.1.11',
                'up': 300.2,
                'down': 800.7,
                'total': 1100.9,
                'session': 3,
                'status': '在线',
                'source_ip': '192.168.1.101',
                'station_name': '测试局点B'
            }
        ]
        
        processor = UserFlowStatsProcessor()
        test_output_path = os.path.join(config.OUTPUT_DIR, 'test_output.xlsx')
        
        processor.create_output_excel(test_data, test_output_path)
        
        # 验证文件是否创建成功
        if os.path.exists(test_output_path):
            print(f"✅ Excel文件创建成功: {test_output_path}")
            
            # 读取验证
            df = pd.read_excel(test_output_path, engine='openpyxl')
            print(f"✅ 文件包含 {len(df)} 行数据")
            print("✅ Excel输出测试通过")
            
            # 清理测试文件
            os.remove(test_output_path)
        else:
            print("❌ Excel文件未创建")
        
    except Exception as e:
        print(f"❌ Excel输出测试失败: {str(e)}")


def main():
    """运行所有测试"""
    print("🚀 开始运行用户流量统计脚本测试")
    print("=" * 50)
    
    # 确保输出目录存在
    if not os.path.exists(config.OUTPUT_DIR):
        os.makedirs(config.OUTPUT_DIR)
        print(f"✅ 创建输出目录: {config.OUTPUT_DIR}")
    
    # 运行测试
    test_excel_reading()
    test_data_processing()
    test_excel_output()
    
    print("\n" + "=" * 50)
    print("🎉 所有测试完成！")
    print("\n💡 提示:")
    print("   - 如果所有测试都通过，说明脚本基本功能正常")
    print("   - 实际运行时需要确保设备API可访问")
    print("   - 可以修改config.py中的配置参数")


if __name__ == "__main__":
    main()
