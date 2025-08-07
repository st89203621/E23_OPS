#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API格式测试脚本
根据接口文档测试不同的请求格式
"""

import requests
import json
import hashlib
import time
import random
import string
import config

def generate_auth_params():
    """生成认证参数"""
    timestamp = str(int(time.time() * 1000))
    random_chars = ''.join(random.choices(
        string.ascii_letters + string.digits, 
        k=config.RANDOM_LENGTH - len(timestamp)
    ))
    random_str = timestamp + random_chars
    
    combined_str = config.SHARED_SECRET + random_str
    md5_value = hashlib.md5(combined_str.encode('utf-8')).hexdigest()
    
    return {
        "random": random_str,
        "md5": md5_value
    }

def test_api_format(ip_address, test_name, payload):
    """测试特定格式的API调用"""
    print(f"\n🧪 测试: {test_name}")
    print("-" * 40)
    
    url = f"http://{ip_address}:{config.API_PORT}{config.API_ENDPOINT}?_method=GET"
    
    print(f"📡 请求体:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    
    try:
        response = requests.post(
            url,
            headers=config.API_HEADERS,
            json=payload,
            timeout=config.REQUEST_TIMEOUT
        )
        
        print(f"📥 响应:")
        print(f"   状态码: {response.status_code}")
        
        try:
            response_data = response.json()
            print(f"   响应体: {json.dumps(response_data, indent=6, ensure_ascii=False)}")
            
            if response_data.get('code') == 0:
                print(f"   ✅ 成功!")
                if 'data' in response_data:
                    data = response_data['data']
                    if isinstance(data, list) and len(data) > 0:
                        print(f"   📊 获取到 {len(data)} 条用户数据")
                        # 显示前3个用户
                        for i, user in enumerate(data[:3], 1):
                            print(f"      用户{i}: {user.get('name', 'N/A')} - {user.get('total', 'N/A')} bytes")
                return True
            else:
                print(f"   ❌ 失败: {response_data.get('message', '未知错误')}")
                return False
                
        except json.JSONDecodeError:
            print(f"   响应体 (文本): {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ 请求异常: {str(e)}")
        return False

def main():
    """主函数"""
    print("🔧 API格式测试工具")
    print("=" * 60)
    
    ip_address = "172.16.80.106"
    
    # 测试格式1: 基本格式（当前使用的）
    auth_params1 = generate_auth_params()
    format1 = {
        "random": auth_params1["random"],
        "md5": auth_params1["md5"],
        "filter": {
            "top": 10,
            "line": "0"
        }
    }
    
    # 测试格式2: 只有认证参数，没有filter
    auth_params2 = generate_auth_params()
    format2 = {
        "random": auth_params2["random"],
        "md5": auth_params2["md5"]
    }
    
    # 测试格式3: 认证参数在外层，filter单独
    auth_params3 = generate_auth_params()
    format3 = {
        "random": auth_params3["random"],
        "md5": auth_params3["md5"],
        "filter": {
            "top": 10,
            "line": "0"
        }
    }
    
    # 测试格式4: 按照文档示例，包含可选字段
    auth_params4 = generate_auth_params()
    format4 = {
        "random": auth_params4["random"],
        "md5": auth_params4["md5"],
        "filter": {
            "top": 10,
            "line": "0",
            "groups": [],  # 空数组
            "users": [],   # 空数组
            "ips": []      # 空数组
        }
    }
    
    # 测试格式5: 只包含必要字段
    auth_params5 = generate_auth_params()
    format5 = {
        "random": auth_params5["random"],
        "md5": auth_params5["md5"],
        "filter": {
            "top": 10,
            "line": "0"
        }
    }
    
    # 测试格式6: 尝试不同的top值
    auth_params6 = generate_auth_params()
    format6 = {
        "random": auth_params6["random"],
        "md5": auth_params6["md5"],
        "filter": {
            "top": 60,  # 使用文档中的示例值
            "line": "0"
        }
    }
    
    # 执行测试
    test_results = []
    
    test_results.append(test_api_format(ip_address, "格式1: 基本格式", format1))
    test_results.append(test_api_format(ip_address, "格式2: 只有认证参数", format2))
    test_results.append(test_api_format(ip_address, "格式3: 标准格式", format3))
    test_results.append(test_api_format(ip_address, "格式4: 包含空数组字段", format4))
    test_results.append(test_api_format(ip_address, "格式5: 最小必要字段", format5))
    test_results.append(test_api_format(ip_address, "格式6: 使用top=60", format6))
    
    # 总结结果
    print(f"\n" + "=" * 60)
    print(f"📊 测试结果总结:")
    success_count = sum(test_results)
    total_count = len(test_results)
    
    for i, result in enumerate(test_results, 1):
        status = "✅ 成功" if result else "❌ 失败"
        print(f"   格式{i}: {status}")
    
    print(f"\n成功率: {success_count}/{total_count}")
    
    if success_count > 0:
        print(f"\n🎉 找到可用的格式！请使用成功的格式更新脚本。")
    else:
        print(f"\n⚠️ 所有格式都失败了，可能需要检查:")
        print(f"   1. 认证参数是否正确")
        print(f"   2. IP白名单设置")
        print(f"   3. 接口文档是否有更新")

if __name__ == "__main__":
    main()
