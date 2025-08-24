#!/bin/bash

# 修复失败的Hadoop节点Doris配置脚本
# 针对bd-s101到bd-s110和bd-nm-101到bd-nm-110节点

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}修复失败的Hadoop节点Doris配置${NC}"
echo -e "${BLUE}========================================${NC}"

# 检查inventory文件
INVENTORY_FILE="inventory/hadoop.ini"
if [[ ! -f "$INVENTORY_FILE" ]]; then
    echo -e "${RED}错误: 找不到inventory文件: $INVENTORY_FILE${NC}"
    exit 1
fi

# 定义失败的服务器列表 (排除被注释的bd-s103, bd-s105, bd-s106)
FAILED_SERVERS="bd-s101,bd-s102,bd-s104,bd-s107,bd-s108,bd-s109,bd-s110,bd-nm-101,bd-nm-102,bd-nm-103,bd-nm-104,bd-nm-105,bd-nm-106,bd-nm-107,bd-nm-108,bd-nm-109,bd-nm-110"

echo -e "${YELLOW}目标服务器:${NC}"
echo "- DataNode: bd-s101,bd-s102,bd-s104,bd-s107,bd-s108,bd-s109,bd-s110 (排除被注释的bd-s103,bd-s105,bd-s106)"
echo "- NodeManager: bd-nm-101 到 bd-nm-110"
echo "- 总计: 17台服务器"
echo ""

# 确认执行
read -p "是否继续修复这些失败的服务器? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}操作已取消${NC}"
    exit 0
fi

echo -e "${GREEN}开始修复...${NC}"

# 步骤1: 测试连接
echo -e "${YELLOW}步骤1: 测试服务器连接${NC}"
ansible -i $INVENTORY_FILE $FAILED_SERVERS -m ping --one-line

if [ $? -ne 0 ]; then
    echo -e "${RED}警告: 部分服务器连接失败，继续处理可连接的服务器${NC}"
fi

# 步骤2: 检查当前状态
echo -e "${YELLOW}步骤2: 检查当前hosts配置${NC}"
ansible -i $INVENTORY_FILE $FAILED_SERVERS -m shell -a "echo '=== 主机名 ===' && hostname && echo '=== 当前doris配置 ===' && grep -i doris /etc/hosts || echo '未找到doris配置'" --one-line

# 步骤3: 备份hosts文件
echo -e "${YELLOW}步骤3: 备份hosts文件${NC}"
ansible -i $INVENTORY_FILE $FAILED_SERVERS -m copy -a "src=/etc/hosts dest=/etc/hosts.backup.$(date +%Y%m%d_%H%M%S) remote_src=yes" --one-line

# 步骤4: 清理旧的doris配置
echo -e "${YELLOW}步骤4: 清理旧的doris配置${NC}"
ansible -i $INVENTORY_FILE $FAILED_SERVERS -m blockinfile -a "path=/etc/hosts marker='# {mark} DORIS CLUSTER HOSTS' state=absent" --one-line

# 步骤5: 添加新的doris配置
echo -e "${YELLOW}步骤5: 添加新的doris配置${NC}"
ansible -i $INVENTORY_FILE $FAILED_SERVERS -m blockinfile -a "path=/etc/hosts marker='# {mark} DORIS CLUSTER HOSTS' block='# Doris集群hosts配置 - 新IP段 192.168.28.x
192.168.28.101 doris-s1
192.168.28.102 doris-s2
192.168.28.103 doris-s3
192.168.28.104 doris-s4
192.168.28.105 doris-s5
192.168.28.106 doris-s6
192.168.28.107 doris-s7
192.168.28.108 doris-s8
192.168.28.109 doris-s9
192.168.28.110 doris-s10
192.168.28.111 doris-s11
192.168.28.112 doris-s12
192.168.28.113 doris-s13
192.168.28.114 doris-s14
192.168.28.115 doris-s15
192.168.28.116 doris-s16
192.168.28.117 doris-s17
192.168.28.118 doris-s18
192.168.28.119 doris-s19
192.168.28.120 doris-s20
192.168.28.121 doris-s21
192.168.28.122 doris-s22
192.168.28.123 doris-s23
192.168.28.124 doris-s24
192.168.28.125 doris-s25
192.168.28.126 doris-s26
192.168.28.127 doris-s27
192.168.28.128 doris-s28
192.168.28.129 doris-s29
192.168.28.130 doris-s30
192.168.28.131 doris-s31
192.168.28.132 doris-s32
192.168.28.133 doris-s33' backup=yes" --one-line

# 步骤6: 验证配置
echo -e "${YELLOW}步骤6: 验证配置${NC}"
ansible -i $INVENTORY_FILE $FAILED_SERVERS -m shell -a "echo '=== 验证doris配置 ===' && grep -A 5 'DORIS CLUSTER HOSTS' /etc/hosts && echo '=== 测试解析 ===' && nslookup doris-s1 2>/dev/null || echo 'DNS解析测试完成'" --one-line

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✅ 修复完成!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# 步骤7: 生成验证报告
echo -e "${YELLOW}生成验证报告...${NC}"
echo "=== 修复结果验证 ===" > doris_fix_report_$(date +%Y%m%d_%H%M%S).txt
ansible -i $INVENTORY_FILE $FAILED_SERVERS -m shell -a "hostname && grep -c doris /etc/hosts" --one-line >> doris_fix_report_$(date +%Y%m%d_%H%M%S).txt 2>&1

echo -e "${YELLOW}后续验证命令:${NC}"
echo "# 检查特定服务器配置"
echo "ansible -i $INVENTORY_FILE bd-s101 -m shell -a 'grep doris /etc/hosts'"
echo ""
echo "# 测试doris域名解析"
echo "ansible -i $INVENTORY_FILE bd-s101:bd-s110 -m shell -a 'ping -c 1 doris-s1'"
echo ""
echo "# 重新运行原始playbook验证"
echo "ansible-playbook -i $INVENTORY_FILE your-original-playbook.yml --limit '$FAILED_SERVERS'"
