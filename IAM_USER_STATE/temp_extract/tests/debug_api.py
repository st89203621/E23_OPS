#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API调用调试脚本
用于诊断API认证和调用问题
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
    # 生成random字符串
    timestamp = str(int(time.time() * 1000))
    random_chars = ''.join(random.choices(
        string.ascii_letters + string.digits, 
        k=config.RANDOM_LENGTH - len(timestamp)
    ))
    random_str = timestamp + random_chars
    
    # 计算MD5
    combined_str = config.SHARED_SECRET + random_str
    md5_value = hashlib.md5(combined_str.encode('utf-8')).hexdigest()
    
    return {
        "random": random_str,
        "md5": md5_value,
        "combined_str": combined_str
    }

def test_api_call(ip_address):
    """测试API调用"""
    print(f"\n🔍 测试API调用: {ip_address}")
    print("=" * 50)
    
    # 生成认证参数
    auth_params = generate_auth_params()
    
    print(f"📋 认证参数:")
    print(f"   共享密钥: '{config.SHARED_SECRET}'")
    print(f"   random: '{auth_params['random']}'")
    print(f"   拼接字符串: '{auth_params['combined_str']}'")
    print(f"   MD5: '{auth_params['md5']}'")
    
    # 构建请求
    url = f"http://{ip_address}:{config.API_PORT}{config.API_ENDPOINT}?_method=GET"
    
    request_payload = {
        "random": auth_params["random"],
        "md5": auth_params["md5"],
        **config.API_PAYLOAD
    }
    
    print(f"\n🌐 HTTP请求信息:")
    print(f"   URL: {url}")
    print(f"   方法: POST")
    print(f"   请求头: {json.dumps(config.API_HEADERS, indent=6, ensure_ascii=False)}")
    print(f"   请求体: {json.dumps(request_payload, indent=6, ensure_ascii=False)}")
    
    try:
        print(f"\n⏳ 发送请求...")
        response = requests.post(
            url,
            headers=config.API_HEADERS,
            json=request_payload,
            timeout=config.REQUEST_TIMEOUT
        )
        
        print(f"\n📥 响应信息:")
        print(f"   状态码: {response.status_code}")
        print(f"   响应头: {dict(response.headers)}")
        
        # 尝试解析响应内容
        try:
            response_data = response.json()
            print(f"   响应体 (JSON): {json.dumps(response_data, indent=6, ensure_ascii=False)}")
            
            # 检查是否有用户数据
            if 'data' in response_data and isinstance(response_data['data'], list):
                user_count = len(response_data['data'])
                print(f"   ✅ 成功获取 {user_count} 条用户数据")
                
                # 显示前几个用户的信息
                for i, user in enumerate(response_data['data'][:3], 1):
                    print(f"      用户{i}: {user.get('name', 'N/A')} - {user.get('total', 'N/A')}MB")
                
                return True
            else:
                print(f"   ⚠️ 响应格式异常或无用户数据")
                return False
                
        except json.JSONDecodeError:
            print(f"   响应体 (文本): {response.text[:500]}")
            print(f"   ❌ 响应不是有效的JSON格式")
            return False
            
    except requests.exceptions.Timeout:
        print(f"   ❌ 请求超时 ({config.REQUEST_TIMEOUT}秒)")
        return False
    except requests.exceptions.ConnectionError:
        print(f"   ❌ 连接失败，无法连接到 {ip_address}:{config.API_PORT}")
        return False
    except Exception as e:
        print(f"   ❌ 请求异常: {str(e)}")
        return False

def test_network_connectivity(ip_address):
    """测试网络连通性"""
    print(f"\n🌐 测试网络连通性: {ip_address}")
    print("-" * 30)
    
    try:
        # 简单的HTTP连接测试
        test_url = f"http://{ip_address}:{config.API_PORT}"
        response = requests.get(test_url, timeout=5)
        print(f"✅ 网络连通性正常，状态码: {response.status_code}")
        return True
    except requests.exceptions.Timeout:
        print(f"❌ 连接超时")
        return False
    except requests.exceptions.ConnectionError:
        print(f"❌ 无法连接到设备")
        return False
    except Exception as e:
        print(f"⚠️ 连接测试异常: {str(e)}")
        return False

def main():
    """主函数"""
    print("🔧 API调用调试工具")
    print("=" * 60)
    
    # 显示配置信息
    print(f"📋 当前配置:")
    print(f"   共享密钥: '{config.SHARED_SECRET}'")
    print(f"   API端口: {config.API_PORT}")
    print(f"   API端点: {config.API_ENDPOINT}")
    print(f"   超时时间: {config.REQUEST_TIMEOUT}秒")
    
    # 测试设备IP
    test_ip = "172.16.80.106"
    
    # 1. 测试网络连通性
    connectivity_ok = test_network_connectivity(test_ip)
    
    # 2. 测试API调用
    if connectivity_ok:
        api_ok = test_api_call(test_ip)
    else:
        print(f"\n⚠️ 跳过API测试，因为网络连通性测试失败")
        api_ok = False
    
    # 3. 总结
    print(f"\n" + "=" * 60)
    print(f"📊 测试结果总结:")
    print(f"   网络连通性: {'✅ 正常' if connectivity_ok else '❌ 失败'}")
    print(f"   API调用: {'✅ 成功' if api_ok else '❌ 失败'}")
    
    if not api_ok:
        print(f"\n💡 故障排除建议:")
        if not connectivity_ok:
            print(f"   1. 检查设备IP地址是否正确: {test_ip}")
            print(f"   2. 检查设备是否在线")
            print(f"   3. 检查网络连接")
            print(f"   4. 检查防火墙设置")
        else:
            print(f"   1. 检查共享密钥是否正确")
            print(f"   2. 检查API端点路径是否正确")
            print(f"   3. 检查认证参数格式")
            print(f"   4. 查看设备端的日志")
    else:
        print(f"\n🎉 API调用测试成功！可以运行完整脚本了。")

if __name__ == "__main__":
    main()
