#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试设备数据插入功能
验证设备级别数据能否正确插入到 nf_device_flow_statistics 表
"""

import os
import sys
from datetime import datetime

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(__file__))

import config
from user_flow_stats import UserFlowStatsProcessor

def test_device_data_insert():
    """测试设备数据插入功能"""
    print("=" * 60)
    print("测试设备数据插入功能")
    print("=" * 60)
    
    processor = UserFlowStatsProcessor()
    
    # 创建模拟设备数据
    mock_device_data = [
        {
            'machine_room': 'A2',
            'device_ip': '192.168.1.1',
            'device_type': '25G',
            'up_mbps': 100.123,
            'down_mbps': 200.456,
            'total_mbps': 300.579
        },
        {
            'machine_room': 'A3',
            'device_ip': '192.168.1.2',
            'device_type': '10G',
            'up_mbps': 50.789,
            'down_mbps': 150.234,
            'total_mbps': 201.023
        },
        {
            'machine_room': 'B1',
            'device_ip': '192.168.1.3',
            'device_type': '25G',
            'up_mbps': 80.555,
            'down_mbps': 120.333,
            'total_mbps': 200.888
        },
        {
            'machine_room': 'C1',
            'device_ip': '192.168.1.4',
            'device_type': '10G',
            'up_mbps': 60.111,
            'down_mbps': 90.222,
            'total_mbps': 150.333
        }
    ]
    
    print(f"📊 准备插入 {len(mock_device_data)} 条设备数据:")
    for i, device in enumerate(mock_device_data, 1):
        print(f"  {i}. {device['machine_room']} - {device['device_ip']} ({device['device_type']}) - {device['total_mbps']:.3f} Mbps")
    
    try:
        # 尝试插入数据
        result = processor.save_device_data_to_database(mock_device_data)
        
        if result:
            print(f"\n✅ 设备数据插入成功！")
            return True
        else:
            print(f"\n❌ 设备数据插入失败！")
            return False
            
    except Exception as e:
        print(f"\n❌ 设备数据插入异常: {str(e)}")
        return False

def test_device_api_call():
    """测试设备API调用功能"""
    print("\n" + "=" * 60)
    print("测试设备API调用功能")
    print("=" * 60)
    
    processor = UserFlowStatsProcessor()
    
    # 从配置文件读取一个设备IP进行测试
    try:
        config_data = processor.read_excel_config(config.INPUT_FILE_PATH)
        if not config_data:
            print("❌ 无法读取配置文件")
            return False
        
        # 选择第一个设备进行测试
        test_device = config_data[0]
        test_ip = test_device['ip_address']
        
        print(f"🔍 测试设备: {test_device['station_name']} ({test_ip})")
        
        # 调用设备API
        device_data = processor.call_device_api(test_ip)
        
        if device_data:
            print(f"✅ 设备API调用成功！")
            print(f"📊 获取到的数据:")
            print(f"  机房: {device_data.get('machine_room', 'N/A')}")
            print(f"  设备IP: {device_data.get('device_ip', 'N/A')}")
            print(f"  设备类型: {device_data.get('device_type', 'N/A')}")
            print(f"  上行流速: {device_data.get('up_mbps', 0):.3f} Mbps")
            print(f"  下行流速: {device_data.get('down_mbps', 0):.3f} Mbps")
            print(f"  总流速: {device_data.get('total_mbps', 0):.3f} Mbps")
            return True
        else:
            print(f"❌ 设备API调用失败！")
            return False
            
    except Exception as e:
        print(f"❌ 设备API调用异常: {str(e)}")
        return False

def check_database_records():
    """检查数据库中的记录"""
    print("\n" + "=" * 60)
    print("检查数据库记录")
    print("=" * 60)
    
    try:
        import pymysql
        
        # 连接数据库
        connection = pymysql.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME,
            charset='utf8mb4'
        )
        
        cursor = connection.cursor()
        
        # 检查设备表记录数
        cursor.execute(f"SELECT COUNT(*) FROM {config.DB_DEVICE_TABLE}")
        device_count = cursor.fetchone()[0]
        
        # 检查用户表记录数
        cursor.execute(f"SELECT COUNT(*) FROM {config.DB_USER_TABLE}")
        user_count = cursor.fetchone()[0]
        
        print(f"📊 数据库记录统计:")
        print(f"  设备表 ({config.DB_DEVICE_TABLE}): {device_count} 条记录")
        print(f"  用户表 ({config.DB_USER_TABLE}): {user_count} 条记录")
        
        # 如果设备表有记录，显示最新的几条
        if device_count > 0:
            cursor.execute(f"""
                SELECT machine_room, device_ip, device_type, 
                       up_flow_rate, down_flow_rate, total_flow_rate, 
                       record_time 
                FROM {config.DB_DEVICE_TABLE} 
                ORDER BY record_time DESC 
                LIMIT 5
            """)
            
            records = cursor.fetchall()
            print(f"\n📋 最新设备记录 (前5条):")
            print(f"  {'机房':<6} {'设备IP':<15} {'类型':<6} {'上行Mbps':<10} {'下行Mbps':<10} {'总流速Mbps':<12} {'记录时间'}")
            print(f"  {'-'*6} {'-'*15} {'-'*6} {'-'*10} {'-'*10} {'-'*12} {'-'*19}")
            
            for record in records:
                machine_room, device_ip, device_type, up_rate, down_rate, total_rate, record_time = record
                print(f"  {machine_room:<6} {device_ip:<15} {device_type:<6} {up_rate:<10.3f} {down_rate:<10.3f} {total_rate:<12.3f} {record_time}")
        
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ 数据库检查失败: {str(e)}")
        return False

def main():
    """主测试函数"""
    print("🧪 开始测试设备数据插入功能...")
    print(f"⏰ 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    tests = [
        ("设备数据插入", test_device_data_insert),
        ("数据库记录检查", check_database_records),
        ("设备API调用", test_device_api_call)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} 测试异常: {str(e)}")
            results.append((test_name, False))
    
    # 输出测试结果汇总
    print("\n" + "=" * 60)
    print("设备数据插入测试结果汇总")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n📊 测试统计: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有设备数据测试通过！")
        print("💾 设备数据可以正常插入到数据库")
    else:
        print("⚠️  部分测试失败，请检查设备数据插入逻辑。")

if __name__ == "__main__":
    main()
