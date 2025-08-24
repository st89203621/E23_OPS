#!/bin/bash

# 最简单的Doris集群hosts配置更新脚本
# 用法: ./update_doris_hosts.sh

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Doris集群hosts配置更新脚本${NC}"
echo -e "${BLUE}========================================${NC}"

# 检查inventory文件
INVENTORY_FILE="inventory/doris_new.ini"
if [[ ! -f "$INVENTORY_FILE" ]]; then
    echo -e "${RED}错误: 找不到inventory文件: $INVENTORY_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}配置信息:${NC}"
echo "- 目标: 所有Doris FE/BE节点"
echo "- 操作: 更新/etc/hosts文件"
echo "- 新IP段: 192.168.28.x"
echo "- Inventory: $INVENTORY_FILE"
echo ""

# 确认执行
read -p "是否继续执行? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}操作已取消${NC}"
    exit 0
fi

echo -e "${GREEN}开始执行...${NC}"

# 方法1: 使用ansible ad-hoc命令批量更新
echo -e "${YELLOW}方法1: 使用ansible批量更新${NC}"

# 先备份原始hosts文件
echo "1. 备份原始hosts文件..."
ansible -i $INVENTORY_FILE doris_cluster -m copy -a "src=/etc/hosts dest=/etc/hosts.backup.$(date +%Y%m%d_%H%M%S) remote_src=yes" --one-line

# 移除旧的Doris配置
echo "2. 移除旧的Doris配置..."
ansible -i $INVENTORY_FILE doris_cluster -m blockinfile -a "path=/etc/hosts marker='# {mark} DORIS CLUSTER HOSTS' state=absent" --one-line

# 添加新的Doris配置
echo "3. 添加新的Doris配置..."
ansible -i $INVENTORY_FILE doris_cluster -m blockinfile -a "path=/etc/hosts marker='# {mark} DORIS CLUSTER HOSTS' block='# Doris集群hosts配置 - 新IP段 192.168.28.x
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

# 验证配置
echo "4. 验证配置..."
ansible -i $INVENTORY_FILE doris_cluster -m shell -a "grep 'doris-s12' /etc/hosts" --one-line

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✅ 配置更新完成!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}验证方法:${NC}"
echo "# 检查特定节点的hosts配置"
echo "ansible -i $INVENTORY_FILE doris-s1 -m shell -a 'tail -10 /etc/hosts'"
echo ""
echo "# 测试域名解析"
echo "ansible -i $INVENTORY_FILE doris-s1 -m shell -a 'ping -c 2 doris-s12'"
