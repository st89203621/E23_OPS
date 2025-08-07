#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API认证功能测试脚本
测试认证参数生成和MD5计算是否正确
"""

import hashlib
import time
import random
import string
import json
import sys
import os

def test_md5_calculation():
    """测试MD5计算功能"""
    print("🔐 测试MD5计算功能...")
    
    # 使用文档中的示例
    test_cases = [
        {
            "shared_secret": "1",
            "random": "2",
            "expected_md5": "c20ad4d76fe97759aa27a0c99bff6710"
        },
        {
            "shared_secret": "default_secret_key_please_change",
            "random": "test123",
            "expected_md5": None  # 我们计算这个
        }
    ]
    
    for i, case in enumerate(test_cases, 1):
        shared_secret = case["shared_secret"]
        random_str = case["random"]
        expected = case["expected_md5"]
        
        # 计算MD5
        combined_str = shared_secret + random_str
        calculated_md5 = hashlib.md5(combined_str.encode('utf-8')).hexdigest()
        
        print(f"\n测试用例 {i}:")
        print(f"  共享密钥: '{shared_secret}'")
        print(f"  random: '{random_str}'")
        print(f"  拼接字符串: '{combined_str}'")
        print(f"  计算的MD5: {calculated_md5}")
        
        if expected:
            if calculated_md5 == expected:
                print(f"  ✅ MD5计算正确")
            else:
                print(f"  ❌ MD5计算错误，期望: {expected}")
        else:
            print(f"  ℹ️ 参考MD5值: {calculated_md5}")

def test_random_generation():
    """测试随机字符串生成"""
    print("\n🎲 测试随机字符串生成...")
    
    generated_randoms = set()
    
    for i in range(10):
        # 生成随机字符串
        timestamp = str(int(time.time() * 1000))
        random_chars = ''.join(random.choices(
            string.ascii_letters + string.digits, 
            k=16 - len(timestamp)
        ))
        random_str = timestamp + random_chars
        
        print(f"  生成 {i+1}: {random_str} (长度: {len(random_str)})")
        
        # 检查唯一性
        if random_str in generated_randoms:
            print(f"  ⚠️ 发现重复的random值: {random_str}")
        else:
            generated_randoms.add(random_str)
    
    print(f"✅ 生成了 {len(generated_randoms)} 个唯一的random值")

def test_auth_params_format():
    """测试认证参数格式"""
    print("\n📋 测试认证参数格式...")
    
    # 生成认证参数
    timestamp = str(int(time.time() * 1000))
    random_chars = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    random_str = timestamp + random_chars
    
    shared_secret = "default_secret_key_please_change"
    combined_str = shared_secret + random_str
    md5_value = hashlib.md5(combined_str.encode('utf-8')).hexdigest()
    
    # 构建认证参数
    auth_params = {
        "random": random_str,
        "md5": md5_value
    }
    
    # 构建完整的API请求体
    api_payload = {
        **auth_params,
        "filter": {
            "top": 10,
            "line": "0"
        }
    }
    
    print("认证参数:")
    print(f"  random: {auth_params['random']}")
    print(f"  md5: {auth_params['md5']}")
    
    print("\n完整API请求体:")
    print(json.dumps(api_payload, indent=2, ensure_ascii=False))
    
    # 验证JSON格式
    try:
        json_str = json.dumps(api_payload)
        parsed = json.loads(json_str)
        print("✅ JSON格式验证通过")
    except Exception as e:
        print(f"❌ JSON格式错误: {str(e)}")

def test_config_integration():
    """测试与配置文件的集成"""
    print("\n⚙️ 测试配置文件集成...")

    try:
        # 尝试导入配置
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
        import config
        
        print(f"✅ 共享密钥配置: {config.SHARED_SECRET}")
        print(f"✅ Random长度配置: {config.RANDOM_LENGTH}")
        print(f"✅ API端口: {config.API_PORT}")
        print(f"✅ API端点: {config.API_ENDPOINT}")
        
        # 测试使用配置的认证参数生成
        timestamp = str(int(time.time() * 1000))
        random_chars = ''.join(random.choices(
            string.ascii_letters + string.digits, 
            k=config.RANDOM_LENGTH - len(timestamp)
        ))
        random_str = timestamp + random_chars
        
        combined_str = config.SHARED_SECRET + random_str
        md5_value = hashlib.md5(combined_str.encode('utf-8')).hexdigest()
        
        print(f"\n使用配置生成的认证参数:")
        print(f"  random: {random_str} (长度: {len(random_str)})")
        print(f"  md5: {md5_value}")
        
    except ImportError as e:
        print(f"❌ 无法导入配置文件: {str(e)}")
    except Exception as e:
        print(f"❌ 配置测试失败: {str(e)}")

def test_user_flow_stats_integration():
    """测试与主脚本的集成"""
    print("\n🔗 测试主脚本集成...")

    try:
        # 尝试导入主脚本类
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
        from user_flow_stats import UserFlowStatsProcessor
        
        processor = UserFlowStatsProcessor()
        
        # 测试认证参数生成
        auth_params = processor.get_auth_params()
        
        print("✅ 主脚本认证功能测试:")
        print(f"  random: {auth_params['random']}")
        print(f"  md5: {auth_params['md5']}")
        
        # 验证MD5计算
        import config
        expected_md5 = hashlib.md5(
            (config.SHARED_SECRET + auth_params['random']).encode('utf-8')
        ).hexdigest()
        
        if auth_params['md5'] == expected_md5:
            print("✅ MD5计算验证通过")
        else:
            print("❌ MD5计算验证失败")
            
    except ImportError as e:
        print(f"❌ 无法导入主脚本: {str(e)}")
    except Exception as e:
        print(f"❌ 主脚本集成测试失败: {str(e)}")

def main():
    """主函数"""
    print("🔐 API认证功能测试")
    print("=" * 50)
    
    # 运行所有测试
    test_md5_calculation()
    test_random_generation()
    test_auth_params_format()
    test_config_integration()
    test_user_flow_stats_integration()
    
    print("\n" + "=" * 50)
    print("🎉 认证功能测试完成！")
    print("\n💡 重要提醒:")
    print("   1. 请修改config.py中的SHARED_SECRET为实际的共享密钥")
    print("   2. 确保random值在1小时内不重复使用")
    print("   3. MD5计算顺序：共享密钥 + random")
    print("   4. POST请求需要在JSON体中包含认证参数")

if __name__ == "__main__":
    main()
