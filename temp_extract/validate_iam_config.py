#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IAM配置验证脚本
验证IAM配置文件是否正确，并测试API连通性
"""

import pandas as pd
import requests
import json
import hashlib
import random
import string
import time
from config import *

def generate_auth_params():
    """生成API认证参数"""
    # 生成随机字符串
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=RANDOM_LENGTH))
    
    # 生成时间戳
    timestamp = str(int(time.time()))
    
    # 计算签名
    sign_string = f"{SHARED_SECRET}{random_str}{timestamp}"
    sign = hashlib.md5(sign_string.encode()).hexdigest()
    
    return {
        "random": random_str,
        "timestamp": timestamp,
        "sign": sign
    }

def test_single_device(station_name, ip_address):
    """测试单个设备的API连通性"""
    try:
        # 构建API URL
        url = f"http://{ip_address}:{API_PORT}{API_ENDPOINT}"
        
        # 生成认证参数
        auth_params = generate_auth_params()
        
        # 构建请求数据
        payload = {
            **API_PAYLOAD,
            **auth_params
        }
        
        # 发送请求
        response = requests.post(
            url,
            json=payload,
            headers=API_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == 1:
                user_count = len(data.get('data', {}).get('list', []))
                return True, f"成功 - 返回{user_count}个用户"
            else:
                return False, f"API错误 - {data.get('msg', '未知错误')}"
        else:
            return False, f"HTTP错误 - {response.status_code}"
            
    except requests.exceptions.Timeout:
        return False, "连接超时"
    except requests.exceptions.ConnectionError:
        return False, "连接失败"
    except Exception as e:
        return False, f"异常: {str(e)}"

def validate_config_file():
    """验证配置文件格式"""
    try:
        # 读取配置文件
        df = pd.read_excel(INPUT_FILE_PATH, engine='openpyxl')
        
        print(f"✅ 配置文件读取成功: {INPUT_FILE_PATH}")
        print(f"   设备数量: {len(df)}")
        print(f"   列名: {list(df.columns)}")
        
        # 检查必要的列
        required_columns = ['局点名称', 'IP地址']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ 缺少必要的列: {missing_columns}")
            return False, df
        
        # 检查数据完整性
        empty_stations = df[df['局点名称'].isna() | (df['局点名称'] == '')].index.tolist()
        empty_ips = df[df['IP地址'].isna() | (df['IP地址'] == '')].index.tolist()
        
        if empty_stations:
            print(f"⚠️ 发现空的局点名称: 行 {[i+2 for i in empty_stations]}")
        
        if empty_ips:
            print(f"⚠️ 发现空的IP地址: 行 {[i+2 for i in empty_ips]}")
        
        # 显示前几个设备
        print("\n📋 设备列表预览:")
        for i, row in df.head(5).iterrows():
            print(f"   {i+1:2d}. {row['局点名称']:<12} - {row['IP地址']}")
        
        if len(df) > 5:
            print(f"   ... 还有 {len(df) - 5} 个设备")
        
        return True, df
        
    except Exception as e:
        print(f"❌ 配置文件验证失败: {str(e)}")
        return False, None

def test_api_connectivity(df, max_test_devices=3):
    """测试API连通性"""
    print(f"\n🔗 测试API连通性 (测试前{max_test_devices}台设备)...")
    print("-" * 60)
    
    success_count = 0
    
    for i, row in df.head(max_test_devices).iterrows():
        station_name = row['局点名称']
        ip_address = row['IP地址']
        
        print(f"测试 {i+1:2d}. {station_name:<12} ({ip_address})... ", end="")
        
        success, message = test_single_device(station_name, ip_address)
        
        if success:
            print(f"✅ {message}")
            success_count += 1
        else:
            print(f"❌ {message}")
    
    print("-" * 60)
    print(f"连通性测试结果: {success_count}/{max_test_devices} 台设备可访问")
    
    if success_count == 0:
        print("\n⚠️ 所有设备都无法连接，请检查:")
        print("   1. 网络连通性")
        print("   2. IAM服务是否运行在端口9999")
        print("   3. 共享密钥是否正确")
        print("   4. 服务器IP是否在设备白名单中")
    elif success_count < max_test_devices:
        print(f"\n⚠️ 部分设备无法连接，建议:")
        print("   1. 检查无法连接的设备状态")
        print("   2. 确认IAM服务配置")
        print("   3. 先使用可连接的设备进行测试")
    else:
        print(f"\n🎉 所有测试设备连接正常！")
        print("   可以开始运行用户流量统计")
    
    return success_count

def main():
    """主函数"""
    print("🔍 IAM配置验证器")
    print("=" * 60)
    
    # 1. 验证配置文件
    print("\n1️⃣ 验证配置文件格式...")
    is_valid, df = validate_config_file()
    
    if not is_valid:
        print("❌ 配置文件验证失败，请检查文件格式")
        return
    
    # 2. 显示当前配置
    print(f"\n2️⃣ 当前配置参数:")
    print(f"   - 输入文件: {os.path.basename(INPUT_FILE_PATH)}")
    print(f"   - API端口: {API_PORT}")
    print(f"   - 共享密钥: {SHARED_SECRET}")
    print(f"   - Top用户数: {TOP_N_USERS}")
    print(f"   - 请求超时: {REQUEST_TIMEOUT}秒")
    print(f"   - 最大重试: {MAX_RETRIES}次")
    
    # 3. 测试API连通性
    print(f"\n3️⃣ API连通性测试...")
    success_count = test_api_connectivity(df, max_test_devices=3)
    
    # 4. 显示后续步骤
    print(f"\n4️⃣ 后续步骤建议:")
    
    if success_count > 0:
        print("   ✅ 配置验证通过，可以开始统计:")
        print("      py user_flow_stats.py")
        print("\n   📊 可选的测试配置:")
        print("      - 使用5台设备测试: 将IAM配置_测试5台.xlsx重命名为IAM配置.xlsx")
        print("      - 使用10台设备测试: 将IAM配置_测试10台.xlsx重命名为IAM配置.xlsx")
    else:
        print("   ❌ 需要先解决连通性问题:")
        print("      1. 检查网络连接")
        print("      2. 确认IAM服务状态")
        print("      3. 验证共享密钥配置")
        print("      4. 检查防火墙设置")
    
    print(f"\n📁 可用的配置文件:")
    print(f"   - IAM配置.xlsx (当前使用，100台设备)")
    print(f"   - IAM配置_测试5台.xlsx (测试用)")
    print(f"   - IAM配置_测试10台.xlsx (测试用)")
    print(f"   - IAM配置_原始备份.xlsx (原始备份)")

if __name__ == "__main__":
    main()
