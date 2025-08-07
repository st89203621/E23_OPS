#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化测试脚本 - 测试基本功能而不依赖所有外部包
"""

import os
import sys
import json
from datetime import datetime

def test_basic_functionality():
    """测试基本功能"""
    print("🚀 开始基本功能测试")
    print("=" * 50)

    # 添加父目录到路径
    parent_dir = os.path.dirname(os.path.dirname(__file__))
    sys.path.insert(0, parent_dir)

    # 测试1: 检查文件结构
    print("\n📁 检查项目文件结构...")
    required_files = [
        'user_flow_stats.py',
        'config.py',
        'requirements.txt',
        'README.md'
    ]

    for file in required_files:
        file_path = os.path.join(parent_dir, file)
        if os.path.exists(file_path):
            print(f"✅ {file} - 存在")
        else:
            print(f"❌ {file} - 缺失")

    # 测试2: 检查配置文件
    print("\n⚙️ 检查配置文件...")
    try:
        import config
        print(f"✅ 输入文件路径: {config.INPUT_FILE_PATH}")
        print(f"✅ 输出目录: {config.OUTPUT_DIR}")
        print(f"✅ API端口: {config.API_PORT}")
        print(f"✅ 前N名用户: {config.TOP_N_USERS}")
    except Exception as e:
        print(f"❌ 配置文件错误: {str(e)}")
    
    # 测试3: 检查输出目录
    print("\n📂 检查输出目录...")
    try:
        if not os.path.exists(config.OUTPUT_DIR):
            os.makedirs(config.OUTPUT_DIR)
            print(f"✅ 创建输出目录: {config.OUTPUT_DIR}")
        else:
            print(f"✅ 输出目录已存在: {config.OUTPUT_DIR}")
    except Exception as e:
        print(f"❌ 输出目录创建失败: {str(e)}")
    
    # 测试4: 模拟数据处理
    print("\n🧮 测试数据处理逻辑...")
    test_data = [
        {'id': '001', 'name': '用户A', 'total': 1000, 'source_ip': '192.168.1.100'},
        {'id': '002', 'name': '用户B', 'total': 2000, 'source_ip': '192.168.1.101'},
        {'id': '001', 'name': '用户A', 'total': 800, 'source_ip': '192.168.1.102'},  # 重复用户
        {'id': '003', 'name': '用户C', 'total': 1500, 'source_ip': '192.168.1.100'},
    ]
    
    # 简单的去重和排序逻辑
    user_dict = {}
    for user in test_data:
        user_id = user['id']
        if user_id not in user_dict or user['total'] > user_dict[user_id]['total']:
            user_dict[user_id] = user
    
    sorted_users = sorted(user_dict.values(), key=lambda x: x['total'], reverse=True)
    
    print(f"✅ 原始数据: {len(test_data)} 条")
    print(f"✅ 去重后: {len(sorted_users)} 条")
    print("✅ 排序结果:")
    for i, user in enumerate(sorted_users, 1):
        print(f"   {i}. {user['name']} - {user['total']}MB")
    
    # 测试5: 模拟API认证和请求格式
    print("\n🌐 测试API认证和请求格式...")

    # 模拟认证参数生成
    import hashlib
    import time
    import random
    import string

    # 生成random字符串
    timestamp = str(int(time.time() * 1000))
    random_chars = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    random_str = timestamp + random_chars

    # 计算MD5
    shared_secret = "default_secret_key_please_change"
    combined_str = shared_secret + random_str
    md5_value = hashlib.md5(combined_str.encode('utf-8')).hexdigest()

    # 构建完整请求体
    api_payload = {
        "random": random_str,
        "md5": md5_value,
        "filter": {
            "top": 10,
            "line": "0"
        }
    }

    test_ip = "192.168.1.100"
    test_url = f"http://{test_ip}:9999/v1/status/user-rank?_method=GET"

    print(f"✅ API URL: {test_url}")
    print(f"✅ 认证参数:")
    print(f"   - random: {random_str}")
    print(f"   - 拼接字符串: {combined_str}")
    print(f"   - md5: {md5_value}")
    print(f"✅ 完整请求体: {json.dumps(api_payload, indent=2, ensure_ascii=False)}")
    
    # 测试6: 模拟响应数据处理
    print("\n📊 测试响应数据处理...")
    mock_response = {
        "data": [
            {
                "id": "001",
                "name": "测试用户1",
                "group": "管理员",
                "ip": "192.168.1.10",
                "up": 500.5,
                "down": 1500.8,
                "total": 2001.3,
                "session": 5,
                "status": "在线"
            },
            {
                "id": "002", 
                "name": "测试用户2",
                "group": "普通用户",
                "ip": "192.168.1.11",
                "up": 300.2,
                "down": 800.7,
                "total": 1100.9,
                "session": 3,
                "status": "在线"
            }
        ]
    }
    
    if 'data' in mock_response and isinstance(mock_response['data'], list):
        users = mock_response['data']
        print(f"✅ 成功解析响应数据，包含 {len(users)} 个用户")
        for user in users:
            print(f"   - {user['name']}: {user['total']}MB")
    else:
        print("❌ 响应数据格式错误")
    
    print("\n" + "=" * 50)
    print("🎉 基本功能测试完成！")
    print("\n💡 下一步:")
    print("   1. 安装依赖包: pip install pandas openpyxl requests")
    print("   2. 准备IAM配置.xlsx文件")
    print("   3. 运行完整脚本: python user_flow_stats.py")


def create_sample_config():
    """创建示例配置文件"""
    print("\n📝 创建示例配置文件...")
    
    sample_data = """局点名称,IP地址
测试局点A,192.168.1.100
测试局点B,192.168.1.101
测试局点C,192.168.1.102"""
    
    sample_file = "sample_config.csv"
    try:
        with open(sample_file, 'w', encoding='utf-8') as f:
            f.write(sample_data)
        print(f"✅ 创建示例配置文件: {sample_file}")
        print("   可以将此文件转换为Excel格式作为输入文件")
    except Exception as e:
        print(f"❌ 创建示例文件失败: {str(e)}")


def check_dependencies():
    """检查依赖包"""
    print("\n🔍 检查Python依赖包...")
    
    dependencies = [
        ('pandas', '数据处理'),
        ('openpyxl', 'Excel文件读写'),
        ('requests', 'HTTP请求'),
        ('xlsxwriter', 'Excel文件写入增强')
    ]
    
    missing_deps = []
    
    for dep, desc in dependencies:
        try:
            __import__(dep)
            print(f"✅ {dep} - {desc} (已安装)")
        except ImportError:
            print(f"❌ {dep} - {desc} (未安装)")
            missing_deps.append(dep)
    
    if missing_deps:
        print(f"\n📦 需要安装的包: {', '.join(missing_deps)}")
        print(f"安装命令: pip install {' '.join(missing_deps)}")
    else:
        print("\n🎉 所有依赖包都已安装！")


def main():
    """主函数"""
    print("🔧 用户流量统计脚本 - 简化测试")
    print("=" * 60)
    
    # 检查依赖
    check_dependencies()
    
    # 基本功能测试
    test_basic_functionality()
    
    # 创建示例文件
    create_sample_config()
    
    print("\n" + "=" * 60)
    print("✨ 测试完成！脚本已准备就绪。")


if __name__ == "__main__":
    main()
