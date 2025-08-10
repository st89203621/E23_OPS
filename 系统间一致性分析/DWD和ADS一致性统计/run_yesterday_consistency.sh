#!/bin/bash

# DWD和ADS一致性统计脚本 - 昨天数据
# 使用方法: ./run_yesterday_consistency.sh

# 设置脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}DWD和ADS一致性统计 - 昨天数据${NC}"
echo "========================================"
echo "执行时间: $(date)"
echo "脚本目录: $SCRIPT_DIR"
echo ""

# 检查Python环境
echo -e "${YELLOW}检查Python环境...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}错误: 需要安装Python3${NC}"
    exit 1
fi

# 检查必要的Python包
echo -e "${YELLOW}检查Python依赖包...${NC}"
python3 -c "
import sys
missing_packages = []

try:
    import yaml
except ImportError:
    missing_packages.append('PyYAML')

try:
    from pyhive import hive
except ImportError:
    missing_packages.append('PyHive')

try:
    from elasticsearch import Elasticsearch
except ImportError:
    missing_packages.append('elasticsearch')

if missing_packages:
    print('缺少以下Python包:')
    for pkg in missing_packages:
        print(f'  - {pkg}')
    print('请使用以下命令安装:')
    print(f'pip install {\" \".join(missing_packages)}')
    sys.exit(1)
else:
    print('所有依赖包已安装')
"

if [ $? -ne 0 ]; then
    echo -e "${RED}Python依赖检查失败${NC}"
    exit 1
fi

# 检查配置文件
echo -e "${YELLOW}检查配置文件...${NC}"
if [ ! -f "config.yaml" ]; then
    echo -e "${RED}错误: 配置文件 config.yaml 不存在${NC}"
    exit 1
fi
echo -e "${GREEN}配置文件检查通过${NC}"

# 创建输出目录
echo -e "${YELLOW}创建输出目录...${NC}"
mkdir -p output
mkdir -p logs
echo -e "${GREEN}输出目录创建完成${NC}"

# 获取昨天日期
YESTERDAY=$(date -d "1 day ago" +%Y-%m-%d)
echo -e "${BLUE}查询日期: $YESTERDAY${NC}"

# 测试Hive连接
echo -e "${YELLOW}测试Hive连接...${NC}"
python3 -c "
import yaml
from hive_client import HiveClient

with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

hive_config = config['hive']
try:
    client = HiveClient(
        host=hive_config['host'],
        port=hive_config['port'],
        username=hive_config.get('username'),
        password=hive_config.get('password'),
        database=hive_config['database'],
        auth=hive_config.get('auth', 'PLAIN')
    )
    if client.connect():
        print('Hive连接测试成功')
        client.disconnect()
    else:
        print('Hive连接测试失败')
        exit(1)
except Exception as e:
    print(f'Hive连接测试异常: {e}')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo -e "${RED}Hive连接测试失败${NC}"
    exit 1
fi
echo -e "${GREEN}Hive连接测试成功${NC}"

# 测试ES连接
echo -e "${YELLOW}测试ES连接...${NC}"
python3 -c "
import yaml
from es_client import ESClient

with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

es_config = config['elasticsearch']
try:
    client = ESClient(
        host=es_config['host'],
        port=es_config['port'],
        timeout=es_config.get('timeout', 30)
    )
    if client.connect():
        print('ES连接测试成功')
        client.disconnect()
    else:
        print('ES连接测试失败')
        exit(1)
except Exception as e:
    print(f'ES连接测试异常: {e}')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo -e "${RED}ES连接测试失败${NC}"
    exit 1
fi
echo -e "${GREEN}ES连接测试成功${NC}"

# 执行数据一致性统计
echo ""
echo -e "${BLUE}开始执行数据一致性统计...${NC}"
echo "========================================"

# 记录开始时间
START_TIME=$(date +%s)

# 执行主程序
python3 data_quality_monitor.py

# 检查执行结果
if [ $? -eq 0 ]; then
    # 记录结束时间
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    
    echo ""
    echo -e "${GREEN}========================================"
    echo -e "数据一致性统计执行完成${NC}"
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
            echo -e "${YELLOW}结果文件前5行预览:${NC}"
            head -6 "$LATEST_CSV" | column -t -s ','
        fi
    fi
    
else
    echo ""
    echo -e "${RED}========================================"
    echo -e "数据一致性统计执行失败${NC}"
    echo -e "${RED}========================================"
    echo "请检查日志文件获取详细错误信息"
    exit 1
fi

echo ""
echo -e "${BLUE}使用说明:${NC}"
echo "1. 结果文件保存在 output/ 目录下"
echo "2. 日志文件保存在 logs/ 目录下"
echo "3. 可以使用Excel等工具打开CSV结果文件进行分析"
echo "4. 如需修改查询日期，请编辑 config.yaml 文件"
