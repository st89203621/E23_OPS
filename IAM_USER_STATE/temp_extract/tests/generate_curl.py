#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成cURL命令脚本
用于在Apifox等API测试工具中使用
"""

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
        "md5": md5_value
    }

def generate_curl_command(ip_address="172.16.80.106"):
    """生成cURL命令"""

    # 生成认证参数
    auth_params = generate_auth_params()

    # 根据测试结果构建正确的请求格式：认证参数在URL中，业务参数在Body中
    request_payload = {
        "filter": {
            "top": 10,
            "line": "0"
        }
    }

    # 构建URL，将认证参数放在查询参数中
    url = f"http://{ip_address}:{config.API_PORT}{config.API_ENDPOINT}?_method=GET&random={auth_params['random']}&md5={auth_params['md5']}"

    # 生成cURL命令
    curl_command = f"""curl -X POST '{url}' \\
  -H 'Content-Type: application/json' \\
  -H 'Accept-Language: zh-CN' \\
  -d '{json.dumps(request_payload, ensure_ascii=False)}'"""

    return curl_command, auth_params, request_payload

def main():
    """主函数"""
    print("🔧 cURL命令生成器")
    print("=" * 60)
    
    # 显示当前配置
    print(f"📋 当前配置:")
    print(f"   共享密钥: '{config.SHARED_SECRET}'")
    print(f"   API端口: {config.API_PORT}")
    print(f"   API端点: {config.API_ENDPOINT}")
    print(f"   Random长度: {config.RANDOM_LENGTH}")
    
    # 生成cURL命令
    curl_cmd, auth_params, payload = generate_curl_command()
    
    print(f"\n🔐 生成的认证参数:")
    print(f"   random: {auth_params['random']}")
    print(f"   md5: {auth_params['md5']}")
    print(f"   计算过程: MD5('{config.SHARED_SECRET}' + '{auth_params['random']}')")
    
    print(f"\n📡 完整请求体:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    
    print(f"\n📋 生成的cURL命令:")
    print("=" * 60)
    print(curl_cmd)
    print("=" * 60)
    
    # 生成Apifox导入格式
    print(f"\n📱 Apifox导入信息:")
    print(f"   URL: http://172.16.80.106:{config.API_PORT}{config.API_ENDPOINT}?_method=GET")
    print(f"   方法: POST")
    print(f"   Content-Type: application/json")
    print(f"   Accept-Language: zh-CN")
    
    # 保存到文件
    with open('api_curl_command.txt', 'w', encoding='utf-8') as f:
        f.write("# API调用cURL命令\n")
        f.write("# 生成时间: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n\n")
        f.write("## 配置信息\n")
        f.write(f"共享密钥: {config.SHARED_SECRET}\n")
        f.write(f"API端口: {config.API_PORT}\n")
        f.write(f"API端点: {config.API_ENDPOINT}\n\n")
        f.write("## 认证参数\n")
        f.write(f"random: {auth_params['random']}\n")
        f.write(f"md5: {auth_params['md5']}\n\n")
        f.write("## cURL命令\n")
        f.write(curl_cmd + "\n\n")
        f.write("## 请求体JSON\n")
        f.write(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    
    print(f"\n💾 cURL命令已保存到文件: api_curl_command.txt")
    
    # 生成新的认证参数（用于多次测试）
    print(f"\n🔄 如需重新生成认证参数，请再次运行此脚本")
    print(f"   注意: random值每次都不同，确保请求的唯一性")
    
    print(f"\n💡 使用说明:")
    print(f"   1. 复制上面的cURL命令到Apifox")
    print(f"   2. 或者手动创建POST请求，使用上面的URL和请求体")
    print(f"   3. 确保请求头包含Content-Type和Accept-Language")
    print(f"   4. 如果需要测试多次，请重新生成新的认证参数")

if __name__ == "__main__":
    main()
