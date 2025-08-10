#!/usr/bin/env python3
"""
下载DWD和ADS一致性统计所需的Python包
在有网络的环境中运行此脚本来下载所有依赖包
"""

import os
import subprocess
import sys
from pathlib import Path

def check_pip():
    """检查pip命令"""
    for cmd in ['pip', 'pip3']:
        try:
            result = subprocess.run([cmd, '--version'], capture_output=True, check=True, text=True)
            print(f"✓ 找到 {cmd}: {result.stdout.strip()}")
            return cmd
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    return None

def download_packages():
    """下载所有需要的Python包"""
    
    # 检查pip
    pip_cmd = check_pip()
    if not pip_cmd:
        print("❌ 错误: 未找到pip或pip3命令")
        print("请先安装Python和pip")
        return False
    
    # 创建下载目录
    download_dir = Path(__file__).parent
    print(f"📁 下载目录: {download_dir}")
    
    # 需要下载的包列表
    packages = [
        # 基础依赖
        'six',
        'setuptools',
        'wheel',
        
        # 配置文件解析
        'PyYAML',
        
        # Hive连接相关
        'thrift',
        'pure-sasl',
        'sasl',
        'thrift-sasl',
        'PyHive',
        
        # Elasticsearch连接相关
        'urllib3<3.0.0',
        'certifi',
        'elastic-transport',
        'elasticsearch<9.0.0',
    ]
    
    print(f"📦 准备下载 {len(packages)} 个包...")
    print("=" * 60)
    
    success_count = 0
    failed_packages = []
    
    for i, package in enumerate(packages, 1):
        print(f"\n[{i}/{len(packages)}] 下载: {package}")
        
        try:
            # 下载包及其依赖
            cmd = [pip_cmd, 'download', package, '--dest', str(download_dir)]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            print(f"✓ {package} 下载成功")
            success_count += 1
            
        except subprocess.CalledProcessError as e:
            print(f"❌ {package} 下载失败")
            print(f"   错误: {e.stderr}")
            failed_packages.append(package)
    
    # 报告结果
    print("\n" + "=" * 60)
    print("📊 下载完成统计")
    print("=" * 60)
    print(f"✅ 成功: {success_count}/{len(packages)} 个包")
    
    if failed_packages:
        print(f"❌ 失败: {len(failed_packages)} 个包")
        for pkg in failed_packages:
            print(f"   - {pkg}")
        print("\n💡 提示: 失败的包可能需要手动处理或在目标环境中在线安装")
    
    # 显示下载的文件
    print(f"\n📁 下载的文件 (在 {download_dir}):")
    downloaded_files = list(download_dir.glob('*.whl')) + list(download_dir.glob('*.tar.gz'))
    downloaded_files = [f for f in downloaded_files if f.name not in ['download_packages.py', 'offline_installer.py', 'requirements-offline.txt']]
    
    if downloaded_files:
        for file in sorted(downloaded_files):
            size_mb = file.stat().st_size / (1024 * 1024)
            print(f"   📄 {file.name} ({size_mb:.1f} MB)")
        
        total_size = sum(f.stat().st_size for f in downloaded_files) / (1024 * 1024)
        print(f"\n📊 总大小: {total_size:.1f} MB")
    else:
        print("   (没有找到下载的包文件)")
    
    print("\n🎉 下载完成！")
    print("💡 现在您可以将整个目录复制到目标服务器并运行离线安装")
    
    return success_count > 0

def main():
    print("=" * 60)
    print("🚀 DWD和ADS一致性统计 - Python包下载器")
    print("=" * 60)
    print("此脚本将下载所有必需的Python包以供离线安装使用")
    print("")
    
    if download_packages():
        print("\n✅ 下载任务完成")
        return 0
    else:
        print("\n❌ 下载任务失败")
        return 1

if __name__ == "__main__":
    sys.exit(main())
