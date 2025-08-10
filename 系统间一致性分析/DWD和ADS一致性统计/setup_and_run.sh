#!/bin/bash

# DWD和ADS一致性统计 - 一键安装和运行脚本
# 拷贝到服务器后直接执行此脚本即可

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${BLUE}========================================"
echo -e "DWD和ADS一致性统计 - 一键部署运行${NC}"
echo -e "${BLUE}========================================"
echo "执行时间: $(date)"
echo "脚本目录: $SCRIPT_DIR"
echo ""

# 检查是否为root用户
if [ "$EUID" -eq 0 ]; then
    SUDO_CMD=""
    PIP_USER=""
    echo -e "${YELLOW}检测到root用户，将进行系统级安装${NC}"
else
    SUDO_CMD="sudo"
    PIP_USER="--user"
    echo -e "${YELLOW}检测到普通用户，将进行用户级安装${NC}"
fi

# 步骤1：检查和安装系统依赖
echo -e "${CYAN}步骤1: 检查系统环境${NC}"
echo "----------------------------------------"

# 检查操作系统
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$NAME
    echo "操作系统: $OS"
else
    echo -e "${YELLOW}无法检测操作系统，假设为Linux${NC}"
fi

# 检查Python3
echo -e "${YELLOW}检查Python3...${NC}"
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✓ $PYTHON_VERSION${NC}"
    PYTHON_CMD="python3"
else
    echo -e "${RED}✗ Python3未安装${NC}"
    echo "正在安装Python3..."
    if command -v apt-get &> /dev/null; then
        $SUDO_CMD apt-get update
        $SUDO_CMD apt-get install -y python3 python3-pip python3-dev
    elif command -v yum &> /dev/null; then
        $SUDO_CMD yum install -y python3 python3-pip python3-devel
    elif command -v dnf &> /dev/null; then
        $SUDO_CMD dnf install -y python3 python3-pip python3-devel
    else
        echo -e "${RED}无法自动安装Python3，请手动安装${NC}"
        exit 1
    fi
fi

# 检查pip
echo -e "${YELLOW}检查pip...${NC}"
if command -v pip3 &> /dev/null; then
    echo -e "${GREEN}✓ pip3可用${NC}"
    PIP_CMD="pip3"
elif command -v pip &> /dev/null; then
    echo -e "${GREEN}✓ pip可用${NC}"
    PIP_CMD="pip"
else
    echo -e "${RED}✗ pip未安装${NC}"
    echo "正在安装pip..."
    if command -v apt-get &> /dev/null; then
        $SUDO_CMD apt-get install -y python3-pip
    elif command -v yum &> /dev/null; then
        $SUDO_CMD yum install -y python3-pip
    elif command -v dnf &> /dev/null; then
        $SUDO_CMD dnf install -y python3-pip
    fi
    PIP_CMD="pip3"
fi

# 安装系统依赖
echo -e "${YELLOW}安装系统依赖...${NC}"
if command -v apt-get &> /dev/null; then
    $SUDO_CMD apt-get install -y libsasl2-dev libsasl2-modules-gssapi-mit gcc python3-dev
elif command -v yum &> /dev/null; then
    $SUDO_CMD yum install -y cyrus-sasl-devel gcc python3-devel
elif command -v dnf &> /dev/null; then
    $SUDO_CMD dnf install -y cyrus-sasl-devel gcc python3-devel
fi

echo ""

# 步骤2：安装Python依赖
echo -e "${CYAN}步骤2: 安装Python依赖${NC}"
echo "----------------------------------------"

# 检查是否有离线包
if [ -d "offline_packages" ]; then
    echo -e "${YELLOW}发现离线包目录，使用离线安装...${NC}"

    # 检查是否有离线安装脚本
    if [ -f "offline_packages/offline_installer.py" ]; then
        echo -e "${GREEN}使用专用离线安装脚本${NC}"
        cd offline_packages
        $PYTHON_CMD offline_installer.py
        INSTALL_RESULT=$?
        cd "$SCRIPT_DIR"

        if [ $INSTALL_RESULT -eq 0 ]; then
            echo -e "${GREEN}✓ 离线安装完成${NC}"
            OFFLINE_INSTALL=true
        else
            echo -e "${RED}✗ 离线安装失败，切换到在线安装${NC}"
            OFFLINE_INSTALL=false
        fi
    else
        # 传统离线安装方式
        cd offline_packages
        if ls *.whl *.tar.gz >/dev/null 2>&1; then
            echo -e "${GREEN}找到离线包文件，使用传统方式安装${NC}"
            $PIP_CMD install *.whl *.tar.gz --no-index --find-links . $PIP_USER --force-reinstall
            if [ $? -eq 0 ]; then
                OFFLINE_INSTALL=true
            else
                OFFLINE_INSTALL=false
            fi
        else
            echo -e "${RED}离线包目录为空，切换到在线安装${NC}"
            OFFLINE_INSTALL=false
        fi
        cd "$SCRIPT_DIR"
    fi
else
    echo -e "${YELLOW}未发现离线包目录，使用在线安装...${NC}"
    OFFLINE_INSTALL=false
fi

# 在线安装（如果离线安装失败或不可用）
if [ "$OFFLINE_INSTALL" = false ]; then
    echo -e "${YELLOW}在线安装Python包...${NC}"

    # 升级pip
    $PIP_CMD install --upgrade pip $PIP_USER

    # 安装包
    PACKAGES=("PyYAML" "elasticsearch<8.0.0" "thrift" "sasl" "pure-sasl" "PyHive")

    for package in "${PACKAGES[@]}"; do
        echo -e "${BLUE}安装: $package${NC}"
        $PIP_CMD install "$package" $PIP_USER
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ $package 安装成功${NC}"
        else
            echo -e "${RED}✗ $package 安装失败，尝试替代方案${NC}"
            # 尝试不同的安装方式
            $PIP_CMD install "$package" $PIP_USER --no-cache-dir
        fi
    done
fi

echo ""

# 步骤3：验证安装
echo -e "${CYAN}步骤3: 验证Python依赖${NC}"
echo "----------------------------------------"

$PYTHON_CMD -c "
import sys
success = True

try:
    import yaml
    print('✓ PyYAML: OK')
except ImportError as e:
    print('✗ PyYAML: FAILED -', e)
    success = False

try:
    from pyhive import hive
    print('✓ PyHive: OK')
except ImportError as e:
    print('✗ PyHive: FAILED -', e)
    success = False

try:
    from elasticsearch import Elasticsearch
    print('✓ elasticsearch: OK')
except ImportError as e:
    print('✗ elasticsearch: FAILED -', e)
    success = False

if not success:
    print('\\n依赖安装不完整，请检查错误信息')
    sys.exit(1)
else:
    print('\\n所有依赖安装成功！')
"

if [ $? -ne 0 ]; then
    echo -e "${RED}依赖验证失败，请检查安装${NC}"
    exit 1
fi

echo ""

# 步骤4：检查配置文件
echo -e "${CYAN}步骤4: 检查配置文件${NC}"
echo "----------------------------------------"

if [ ! -f "config.yaml" ]; then
    echo -e "${RED}错误: 配置文件 config.yaml 不存在${NC}"
    exit 1
fi

echo -e "${GREEN}✓ 配置文件存在${NC}"

# 显示配置信息
echo -e "${YELLOW}当前配置:${NC}"
echo "Hive地址: $(grep -A1 "^hive:" config.yaml | grep "host:" | awk '{print $2}' | tr -d '"')"
echo "ES地址: $(grep -A3 "^elasticsearch:" config.yaml | grep "host:" | awk '{print $2}' | tr -d '"')"
echo "查询日期: $(grep "default_query_date:" config.yaml | awk '{print $2}' | tr -d '"')"

echo ""

# 步骤5：创建输出目录
echo -e "${CYAN}步骤5: 准备运行环境${NC}"
echo "----------------------------------------"

mkdir -p output logs
echo -e "${GREEN}✓ 创建输出目录${NC}"

# 获取昨天日期
YESTERDAY=$(date -d "1 day ago" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d 2>/dev/null || date --date="yesterday" +%Y-%m-%d)
echo -e "${GREEN}✓ 查询日期: $YESTERDAY${NC}"

echo ""

# 步骤6：测试连接
echo -e "${CYAN}步骤6: 测试数据库连接${NC}"
echo "----------------------------------------"

echo -e "${YELLOW}测试Hive连接...${NC}"
$PYTHON_CMD -c "
import yaml
import sys
sys.path.append('.')
from hive_client import HiveClient

try:
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    hive_config = config['hive']
    client = HiveClient(
        host=hive_config['host'],
        port=hive_config['port'],
        username=hive_config.get('username'),
        password=hive_config.get('password'),
        database=hive_config['database'],
        auth=hive_config.get('auth', 'PLAIN')
    )
    
    if client.connect():
        print('✓ Hive连接测试成功')
        client.disconnect()
    else:
        print('✗ Hive连接测试失败')
        sys.exit(1)
except Exception as e:
    print(f'✗ Hive连接测试异常: {e}')
    sys.exit(1)
"

if [ $? -ne 0 ]; then
    echo -e "${RED}Hive连接测试失败，请检查配置${NC}"
    echo -e "${YELLOW}继续执行可能会失败，是否继续？(y/N)${NC}"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo -e "${YELLOW}测试ES连接...${NC}"
$PYTHON_CMD -c "
import yaml
import sys
sys.path.append('.')
from es_client import ESClient

try:
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    es_config = config['elasticsearch']
    client = ESClient(
        host=es_config['host'],
        port=es_config['port'],
        timeout=es_config.get('timeout', 30)
    )
    
    if client.connect():
        print('✓ ES连接测试成功')
        client.disconnect()
    else:
        print('✗ ES连接测试失败')
        sys.exit(1)
except Exception as e:
    print(f'✗ ES连接测试异常: {e}')
    sys.exit(1)
"

if [ $? -ne 0 ]; then
    echo -e "${RED}ES连接测试失败，请检查配置${NC}"
    echo -e "${YELLOW}继续执行可能会失败，是否继续？(y/N)${NC}"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""

# 步骤7：执行数据一致性统计
echo -e "${CYAN}步骤7: 执行数据一致性统计${NC}"
echo "========================================"

# 记录开始时间
START_TIME=$(date +%s)

echo -e "${BLUE}开始执行DWD和ADS数据一致性统计...${NC}"
echo "查询日期: $YESTERDAY"
echo ""

# 执行主程序
$PYTHON_CMD data_quality_monitor.py

# 检查执行结果
if [ $? -eq 0 ]; then
    # 记录结束时间
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    
    echo ""
    echo -e "${GREEN}========================================"
    echo -e "数据一致性统计执行完成！${NC}"
    echo -e "${GREEN}========================================"
    echo "执行时长: ${DURATION}秒"
    echo "查询日期: $YESTERDAY"
    echo "输出目录: $SCRIPT_DIR/output"
    echo "日志目录: $SCRIPT_DIR/logs"
    
    # 显示最新的结果文件
    if [ -d "output" ]; then
        LATEST_CSV=$(ls -t output/dwd_ads_consistency_*.csv 2>/dev/null | head -1)
        if [ -n "$LATEST_CSV" ]; then
            echo "结果文件: $LATEST_CSV"
            echo ""
            echo -e "${YELLOW}结果文件预览:${NC}"
            if command -v column &> /dev/null; then
                head -6 "$LATEST_CSV" | column -t -s ','
            else
                head -6 "$LATEST_CSV"
            fi
        fi
    fi
    
    echo ""
    echo -e "${GREEN}🎉 部署和执行成功完成！${NC}"
    
else
    echo ""
    echo -e "${RED}========================================"
    echo -e "数据一致性统计执行失败${NC}"
    echo -e "${RED}========================================"
    echo "请检查日志文件获取详细错误信息"
    echo "日志目录: $SCRIPT_DIR/logs"
    exit 1
fi

echo ""
echo -e "${BLUE}使用说明:${NC}"
echo "1. 结果文件保存在 output/ 目录下"
echo "2. 日志文件保存在 logs/ 目录下"
echo "3. 可以使用Excel等工具打开CSV结果文件进行分析"
echo "4. 如需修改配置，请编辑 config.yaml 文件"
echo "5. 如需重新运行，直接执行: ./setup_and_run.sh"
