#!/bin/bash

# DWD和ADS一致性统计 - 修复thrift问题并运行
# 一键解决thrift兼容性问题

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================"
echo -e "修复thrift兼容性问题并运行统计${NC}"
echo -e "${BLUE}========================================"

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${YELLOW}步骤1: 创建thrift兼容性模块${NC}"
echo "----------------------------------------"

# 创建用户Python包目录
USER_SITE="/root/.local/lib/python3.12/site-packages"
mkdir -p "$USER_SITE"

# 创建thrift兼容性目录
THRIFT_DIR="$USER_SITE/thrift"
mkdir -p "$THRIFT_DIR"

# 创建thrift兼容性模块
cat > "$THRIFT_DIR/__init__.py" << 'EOF'
# Thrift兼容性模块 - 使用系统thriftpy
try:
    # 首先尝试导入真正的thrift
    from thrift import *
    print("使用标准thrift库")
except ImportError:
    try:
        # 使用系统的thriftpy作为替代
        import thriftpy
        from thriftpy import *
        print("使用thriftpy作为thrift替代")
        
        # 创建一些PyHive需要的兼容性映射
        import sys
        if hasattr(thriftpy, 'transport'):
            sys.modules['thrift.transport'] = thriftpy.transport
        if hasattr(thriftpy, 'protocol'):
            sys.modules['thrift.protocol'] = thriftpy.protocol
            
    except ImportError:
        print("警告: 既没有thrift也没有thriftpy，PyHive可能无法工作")
        raise
EOF

echo -e "${GREEN}✓ 创建thrift兼容性模块: $THRIFT_DIR${NC}"

echo ""
echo -e "${YELLOW}步骤2: 验证Python依赖${NC}"
echo "----------------------------------------"

# 验证所有依赖
python3 -c "
import sys
sys.path.insert(0, '$USER_SITE')

packages_to_verify = ['yaml', 'elasticsearch', 'thrift', 'pyhive']
all_verified = True

for module_name in packages_to_verify:
    try:
        if module_name == 'pyhive':
            from pyhive import hive
            print('✓ pyhive - 导入成功')
        else:
            __import__(module_name)
            print('✓ {} - 导入成功'.format(module_name))
    except ImportError as e:
        print('✗ {} - 导入失败: {}'.format(module_name, e))
        all_verified = False

if all_verified:
    print('\n🎉 所有核心包验证成功！')
    exit(0)
else:
    print('\n⚠️ 部分包验证失败')
    exit(1)
"

VERIFY_RESULT=$?

if [ $VERIFY_RESULT -eq 0 ]; then
    echo -e "${GREEN}✓ 所有依赖验证成功${NC}"
else
    echo -e "${RED}✗ 依赖验证失败，但继续尝试运行${NC}"
fi

echo ""
echo -e "${YELLOW}步骤3: 测试Hive和ES连接${NC}"
echo "----------------------------------------"

# 测试Hive连接
echo -e "${BLUE}测试Hive连接...${NC}"
python3 -c "
import sys
sys.path.insert(0, '$USER_SITE')

import yaml
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
        exit(1)
except Exception as e:
    print('✗ Hive连接测试异常:', e)
    exit(1)
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Hive连接测试成功${NC}"
else
    echo -e "${RED}✗ Hive连接测试失败${NC}"
    echo -e "${YELLOW}继续执行可能会失败，是否继续？(y/N)${NC}"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# 测试ES连接
echo -e "${BLUE}测试ES连接...${NC}"
python3 -c "
import sys
sys.path.insert(0, '$USER_SITE')

import yaml
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
        exit(1)
except Exception as e:
    print('✗ ES连接测试异常:', e)
    exit(1)
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ ES连接测试成功${NC}"
else
    echo -e "${RED}✗ ES连接测试失败${NC}"
    echo -e "${YELLOW}继续执行可能会失败，是否继续？(y/N)${NC}"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo -e "${YELLOW}步骤4: 执行数据一致性统计${NC}"
echo "========================================"

# 记录开始时间
START_TIME=$(date +%s)

# 获取昨天日期
YESTERDAY=$(date -d "1 day ago" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d 2>/dev/null || date --date="yesterday" +%Y-%m-%d)
echo -e "${BLUE}查询日期: $YESTERDAY${NC}"
echo ""

# 创建输出目录
mkdir -p output logs

# 执行主程序
PYTHONPATH="$USER_SITE:$PYTHONPATH" python3 data_quality_monitor.py

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
    echo -e "${GREEN}🎉 修复和执行成功完成！${NC}"
    
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
echo "4. 如需重新运行，直接执行: ./fix_thrift_and_run.sh"
