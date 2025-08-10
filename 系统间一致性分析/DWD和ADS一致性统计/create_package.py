#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建DWD和ADS一致性统计完整部署包
在Windows环境中运行，创建可以直接拷贝到Linux服务器的部署包
"""

import os
import shutil
import subprocess
import sys
from datetime import datetime

def print_colored(text, color='white'):
    """打印彩色文本"""
    colors = {
        'red': '\033[0;31m',
        'green': '\033[0;32m',
        'yellow': '\033[1;33m',
        'blue': '\033[0;34m',
        'cyan': '\033[0;36m',
        'white': '\033[0m'
    }
    print(f"{colors.get(color, colors['white'])}{text}{colors['white']}")

def check_pip():
    """检查pip命令"""
    for cmd in ['pip', 'pip3']:
        try:
            subprocess.run([cmd, '--version'], capture_output=True, check=True)
            return cmd
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    return None

def download_packages(pip_cmd, package_dir):
    """下载Python包"""
    packages = [
        'PyYAML',
        'PyHive',
        'elasticsearch<8.0.0',
        'thrift',
        'sasl',
        'pure-sasl',
        'six',
        'future',
        'setuptools',
        'wheel'
    ]
    
    print_colored("开始下载Python包...", 'yellow')
    
    # 创建包目录
    os.makedirs(package_dir, exist_ok=True)
    
    # 下载包
    for package in packages:
        print_colored(f"下载: {package}", 'blue')
        try:
            subprocess.run([
                pip_cmd, 'download', package, 
                '--dest', package_dir
            ], check=True, capture_output=True)
            print_colored(f"✓ {package} 下载成功", 'green')
        except subprocess.CalledProcessError as e:
            print_colored(f"✗ {package} 下载失败: {e}", 'red')
    
    return True

def create_install_script(package_dir):
    """创建离线安装脚本"""
    install_script = os.path.join(package_dir, 'install_offline.sh')
    
    script_content = '''#!/bin/bash

# Python包离线安装脚本

# 颜色定义
RED='\\033[0;31m'
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
BLUE='\\033[0;34m'
NC='\\033[0m' # No Color

echo -e "${BLUE}Python包离线安装工具${NC}"
echo "========================================"
echo "安装时间: $(date)"
echo ""

# 检查pip
if ! command -v pip &> /dev/null; then
    if command -v pip3 &> /dev/null; then
        PIP_CMD="pip3"
    else
        echo -e "${RED}错误: 未找到pip或pip3命令${NC}"
        echo "请先安装Python3和pip3:"
        echo "sudo apt-get update"
        echo "sudo apt-get install python3 python3-pip"
        exit 1
    fi
else
    PIP_CMD="pip"
fi

echo -e "${GREEN}使用命令: $PIP_CMD${NC}"

# 检查是否有包文件
if ! ls *.whl *.tar.gz >/dev/null 2>&1; then
    echo -e "${RED}错误: 当前目录下没有找到Python包文件${NC}"
    echo "请确保已将下载的包文件复制到当前目录"
    exit 1
fi

echo -e "${YELLOW}找到的包文件:${NC}"
ls -1 *.whl *.tar.gz 2>/dev/null

echo ""
echo -e "${YELLOW}开始离线安装...${NC}"

# 检查是否为root用户
if [ "$EUID" -eq 0 ]; then
    USER_FLAG=""
    echo -e "${YELLOW}检测到root用户，进行系统级安装${NC}"
else
    USER_FLAG="--user"
    echo -e "${YELLOW}检测到普通用户，进行用户级安装${NC}"
fi

# 安装包
$PIP_CMD install *.whl *.tar.gz --no-index --find-links . $USER_FLAG --force-reinstall

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}========================================"
    echo -e "安装完成${NC}"
    echo -e "${GREEN}========================================"
    
    # 验证安装
    echo -e "${YELLOW}验证安装...${NC}"
    python3 -c "
try:
    import yaml
    print('✓ PyYAML: OK')
except ImportError as e:
    print('✗ PyYAML: FAILED -', e)

try:
    from pyhive import hive
    print('✓ PyHive: OK')
except ImportError as e:
    print('✗ PyHive: FAILED -', e)

try:
    from elasticsearch import Elasticsearch
    print('✓ elasticsearch: OK')
except ImportError as e:
    print('✗ elasticsearch: FAILED -', e)
"
    
    echo ""
    echo -e "${GREEN}所有依赖包安装完成！${NC}"
    echo "现在可以运行DWD和ADS一致性统计脚本了"
    
else
    echo ""
    echo -e "${RED}========================================"
    echo -e "安装失败${NC}"
    echo -e "${RED}========================================"
    echo "请检查错误信息并手动安装"
    exit 1
fi
'''
    
    with open(install_script, 'w', encoding='utf-8') as f:
        f.write(script_content)
    
    print_colored(f"✓ 创建安装脚本: {install_script}", 'green')

def create_usage_guide(package_dir):
    """创建使用说明"""
    usage_file = os.path.join(package_dir, '使用说明.txt')
    
    usage_content = f'''DWD和ADS一致性统计 - 部署包使用说明
======================================

此部署包包含了运行DWD和ADS一致性统计所需的所有文件和依赖。

文件结构:
├── config.yaml                    # 配置文件
├── data_quality_monitor.py         # 主程序
├── es_client.py                    # ES客户端
├── hive_client.py                  # Hive客户端
├── setup_and_run.sh               # 一键安装运行脚本
├── python_packages_offline/       # 离线Python包目录
│   ├── install_offline.sh         # 离线安装脚本
│   └── *.whl, *.tar.gz            # Python包文件
├── requirements.txt               # 依赖列表
├── README_offline_install.md      # 详细安装指南
└── 使用说明.txt                   # 本文件

使用方法:
=========

方法1: 一键安装运行（推荐）
--------------------------
1. 将整个目录复制到Linux服务器
2. 进入目录: cd {os.path.basename(package_dir)}
3. 执行: ./setup_and_run.sh

此脚本会自动:
- 检查和安装系统依赖
- 安装Python依赖包
- 测试数据库连接
- 执行数据一致性统计

方法2: 手动安装
--------------
1. 进入python_packages_offline目录
2. 执行: ./install_offline.sh
3. 返回主目录
4. 执行: python3 data_quality_monitor.py

配置说明:
=========
- Hive地址: 172.16.80.10:10000
- ES地址: 192.168.14.1:9200
- 查询日期: 自动使用昨天日期
- 如需修改配置，请编辑config.yaml文件

输出结果:
=========
- CSV报告: output/dwd_ads_consistency_*.csv
- 日志文件: logs/data_quality_*.log

注意事项:
=========
1. 确保服务器能连接到Hive和ES
2. 建议使用root用户或具有sudo权限的用户
3. 如遇到问题，请查看日志文件

技术支持:
=========
如有问题，请检查:
1. 网络连接是否正常
2. Hive和ES服务是否可用
3. Python版本是否为3.6+
4. 系统依赖是否完整安装

创建时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
'''
    
    with open(usage_file, 'w', encoding='utf-8') as f:
        f.write(usage_content)
    
    print_colored(f"✓ 创建使用说明: {usage_file}", 'green')

def main():
    """主函数"""
    print_colored("========================================", 'blue')
    print_colored("创建DWD和ADS一致性统计部署包", 'blue')
    print_colored("========================================", 'blue')
    print_colored(f"创建时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 'white')
    print("")
    
    # 检查当前目录
    required_files = ['config.yaml', 'data_quality_monitor.py', 'es_client.py', 'hive_client.py']
    for file in required_files:
        if not os.path.exists(file):
            print_colored(f"错误: 缺少文件 {file}", 'red')
            print_colored("请在DWD和ADS一致性统计目录中运行此脚本", 'red')
            sys.exit(1)
    
    # 检查pip
    pip_cmd = check_pip()
    if not pip_cmd:
        print_colored("错误: 未找到pip或pip3命令", 'red')
        print_colored("请先安装Python和pip", 'red')
        sys.exit(1)
    
    print_colored(f"使用命令: {pip_cmd}", 'green')
    
    # 创建部署包目录
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    package_name = f"dwd_ads_consistency_{timestamp}"
    package_dir = package_name
    
    print_colored(f"创建部署包目录: {package_dir}", 'yellow')
    os.makedirs(package_dir, exist_ok=True)
    
    # 复制核心文件
    print_colored("复制核心文件...", 'yellow')
    core_files = [
        'config.yaml',
        'data_quality_monitor.py',
        'es_client.py',
        'hive_client.py',
        'setup_and_run.sh',
        'requirements.txt',
        'README_offline_install.md'
    ]
    
    for file in core_files:
        if os.path.exists(file):
            shutil.copy2(file, package_dir)
            print_colored(f"✓ 复制: {file}", 'green')
    
    # 下载Python包
    python_packages_dir = os.path.join(package_dir, 'python_packages_offline')
    if download_packages(pip_cmd, python_packages_dir):
        print_colored("✓ Python包下载完成", 'green')
    else:
        print_colored("✗ Python包下载失败", 'red')
        sys.exit(1)
    
    # 创建安装脚本
    create_install_script(python_packages_dir)
    
    # 创建使用说明
    create_usage_guide(package_dir)
    
    # 显示结果
    print("")
    print_colored("========================================", 'green')
    print_colored("部署包创建完成！", 'green')
    print_colored("========================================", 'green')
    print_colored(f"包目录: {package_dir}", 'white')
    print("")
    
    # 显示包内容
    print_colored("包内容:", 'yellow')
    for root, dirs, files in os.walk(package_dir):
        level = root.replace(package_dir, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files[:10]:  # 只显示前10个文件
            print(f"{subindent}{file}")
        if len(files) > 10:
            print(f"{subindent}... 还有 {len(files) - 10} 个文件")
    
    # 计算包大小
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(package_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)
    
    size_mb = total_size / (1024 * 1024)
    print("")
    print_colored(f"包大小: {size_mb:.1f} MB", 'yellow')
    
    print("")
    print_colored("使用方法:", 'blue')
    print(f"1. 将 '{package_dir}' 目录复制到Linux服务器")
    print(f"2. 在服务器上执行: cd {package_dir} && ./setup_and_run.sh")
    print("")
    print_colored("🎉 部署包创建成功！", 'green')

if __name__ == "__main__":
    main()
