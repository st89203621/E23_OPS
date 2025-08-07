#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试认证参数位置
根据不同的认证参数放置位置进行测试
"""

import requests
import json
import hashlib
import time
import random
import string
import config
from urllib.parse import urlencode

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

def test_format(test_name, url, headers, data=None, params=None):
    """通用测试函数"""
    print(f"\n🧪 测试: {test_name}")
    print("-" * 50)
    print(f"URL: {url}")
    if params:
        print(f"URL参数: {params}")
    print(f"请求头: {json.dumps(headers, indent=2)}")
    if data:
        print(f"请求体: {json.dumps(data, indent=2, ensure_ascii=False)}")
    
    try:
        if data:
            response = requests.post(url, headers=headers, json=data, params=params, timeout=30)
        else:
            response = requests.post(url, headers=headers, params=params, timeout=30)
        
        print(f"📥 响应 (状态码: {response.status_code}):")
        try:
            response_data = response.json()
            print(json.dumps(response_data, indent=2, ensure_ascii=False))
            return response_data.get('code') == 0
        except:
            print(f"响应文本: {response.text}")
            return False
    except Exception as e:
        print(f"❌ 异常: {str(e)}")
        return False

def main():
    """主函数"""
    print("🔧 认证参数位置测试")
    print("=" * 60)
    
    ip_address = "172.16.80.106"
    base_url = f"http://{ip_address}:{config.API_PORT}{config.API_ENDPOINT}"
    
    results = []
    
    # 测试1: 认证参数在URL查询参数中
    auth_params = generate_auth_params()
    url_with_auth = f"{base_url}?_method=GET&random={auth_params['random']}&md5={auth_params['md5']}"
    body_data = {
        "filter": {
            "top": 10,
            "line": "0"
        }
    }
    results.append(test_format(
        "认证参数在URL中，业务参数在Body中",
        url_with_auth,
        config.API_HEADERS,
        body_data
    ))
    
    # 测试2: 所有参数都在URL中
    auth_params = generate_auth_params()
    url_params = {
        "_method": "GET",
        "random": auth_params['random'],
        "md5": auth_params['md5'],
        "top": 10,
        "line": "0"
    }
    results.append(test_format(
        "所有参数都在URL中",
        base_url,
        config.API_HEADERS,
        params=url_params
    ))
    
    # 测试3: 认证参数在Header中
    auth_params = generate_auth_params()
    headers_with_auth = {
        **config.API_HEADERS,
        "X-Random": auth_params['random'],
        "X-MD5": auth_params['md5']
    }
    url_with_method = f"{base_url}?_method=GET"
    body_data = {
        "filter": {
            "top": 10,
            "line": "0"
        }
    }
    results.append(test_format(
        "认证参数在Header中",
        url_with_method,
        headers_with_auth,
        body_data
    ))
    
    # 测试4: 尝试GET方法而不是POST
    auth_params = generate_auth_params()
    get_url = f"{base_url}?_method=GET&random={auth_params['random']}&md5={auth_params['md5']}&top=10&line=0"
    try:
        print(f"\n🧪 测试: GET方法")
        print("-" * 50)
        print(f"URL: {get_url}")
        response = requests.get(get_url, headers={"Accept-Language": "zh-CN"}, timeout=30)
        print(f"📥 响应 (状态码: {response.status_code}):")
        try:
            response_data = response.json()
            print(json.dumps(response_data, indent=2, ensure_ascii=False))
            results.append(response_data.get('code') == 0)
        except:
            print(f"响应文本: {response.text}")
            results.append(False)
    except Exception as e:
        print(f"❌ 异常: {str(e)}")
        results.append(False)
    
    # 测试5: 只有filter，没有认证参数（看看是否认证是可选的）
    url_simple = f"{base_url}?_method=GET"
    body_simple = {
        "filter": {
            "top": 10,
            "line": "0"
        }
    }
    results.append(test_format(
        "无认证参数，只有业务参数",
        url_simple,
        config.API_HEADERS,
        body_simple
    ))
    
    # 测试6: 尝试不同的数据类型
    auth_params = generate_auth_params()
    url_with_method = f"{base_url}?_method=GET"
    body_with_types = {
        "random": auth_params['random'],
        "md5": auth_params['md5'],
        "filter": {
            "top": 10,        # 数字
            "line": "0"       # 字符串
        }
    }
    results.append(test_format(
        "确保数据类型正确",
        url_with_method,
        config.API_HEADERS,
        body_with_types
    ))
    
    # 总结
    print(f"\n" + "=" * 60)
    print(f"📊 测试结果总结:")
    test_names = [
        "认证参数在URL中",
        "所有参数都在URL中", 
        "认证参数在Header中",
        "GET方法",
        "无认证参数",
        "数据类型测试"
    ]
    
    success_count = sum(results)
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✅ 成功" if result else "❌ 失败"
        print(f"   {name}: {status}")
    
    print(f"\n成功率: {success_count}/{len(results)}")
    
    if success_count == 0:
        print(f"\n💡 建议:")
        print(f"   1. 检查接口文档是否有认证参数的具体说明")
        print(f"   2. 确认_method=GET参数是否必需")
        print(f"   3. 尝试联系API提供方确认正确的调用格式")
        print(f"   4. 检查是否需要特殊的Content-Type")

if __name__ == "__main__":
    main()
