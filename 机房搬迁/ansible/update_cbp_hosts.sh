#!/bin/bash

# CBP应用服务器集群hosts配置更新脚本
# 用法: ./update_cbp_hosts.sh

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}CBP应用服务器集群hosts配置更新脚本${NC}"
echo -e "${BLUE}========================================${NC}"

# 检查inventory文件
INVENTORY_FILE="inventory/app_server.ini"
if [[ ! -f "$INVENTORY_FILE" ]]; then
    echo -e "${RED}错误: 找不到inventory文件: $INVENTORY_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}配置信息:${NC}"
echo "- 目标: 所有CBP应用服务器节点"
echo "- 操作: 更新/etc/hosts文件"
echo "- 新IP段: 192.168.28.x"
echo "- 服务器数量: 20台"
echo "- Inventory: $INVENTORY_FILE"
echo ""

# 显示服务器列表
echo -e "${YELLOW}目标服务器:${NC}"
echo "- Nebula服务器: nebula-s1, nebula-s2, nebula-s3"
echo "- 数据库服务器: mysql-s1, oracle-s1"
echo "- 应用服务器: app-s2 到 app-s12, app-s21, app-s22"
echo "- Redis服务器: redis-s1, redis-s2"
echo ""

# 确认执行
read -p "是否继续执行? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}操作已取消${NC}"
    exit 0
fi

echo -e "${GREEN}开始执行...${NC}"

# 测试连接
echo -e "${YELLOW}步骤1: 测试服务器连接${NC}"
ansible -i $INVENTORY_FILE cbp_all_servers -m ping --one-line

if [ $? -ne 0 ]; then
    echo -e "${RED}错误: 部分服务器连接失败，请检查网络和SSH配置${NC}"
    exit 1
fi

# 备份原始hosts文件
echo -e "${YELLOW}步骤2: 备份原始hosts文件${NC}"
ansible -i $INVENTORY_FILE cbp_all_servers -m copy -a "src=/etc/hosts dest=/etc/hosts.backup.$(date +%Y%m%d_%H%M%S) remote_src=yes" --one-line

# 移除旧的CBP配置
echo -e "${YELLOW}步骤3: 移除旧的CBP配置${NC}"
ansible -i $INVENTORY_FILE cbp_all_servers -m blockinfile -a "path=/etc/hosts marker='# {mark} CBP CLUSTER HOSTS' state=absent" --one-line

# 添加新的CBP配置
echo -e "${YELLOW}步骤4: 添加新的CBP配置${NC}"
ansible -i $INVENTORY_FILE cbp_all_servers -m blockinfile -a "path=/etc/hosts marker='# {mark} CBP CLUSTER HOSTS' block='# CBP应用服务器集群hosts配置 - 新IP段 192.168.28.x
# Nebula图数据库服务器
192.168.28.1 nebula-s1
192.168.28.2 nebula-s2
192.168.28.3 nebula-s3
# 数据库服务器
192.168.28.4 mysql-s1
192.168.28.5 oracle-s1
# 应用服务器
192.168.28.6 app-s2
192.168.28.7 app-s3
192.168.28.8 app-s4
192.168.28.9 app-s5
192.168.28.10 app-s6
192.168.28.11 app-s7
192.168.28.12 app-s8
192.168.28.13 app-s9
192.168.28.14 app-s10
192.168.28.15 app-s11
192.168.28.16 app-s12
192.168.28.17 app-s21
192.168.28.18 app-s22
# Redis缓存服务器
192.168.28.19 redis-s1
192.168.28.20 redis-s2' backup=yes" --one-line

# 验证配置
echo -e "${YELLOW}步骤5: 验证配置${NC}"
ansible -i $INVENTORY_FILE cbp_all_servers -m shell -a "grep 'nebula-s1' /etc/hosts" --one-line

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✅ CBP集群hosts配置更新完成!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}验证方法:${NC}"
echo "# 检查特定节点的hosts配置"
echo "ansible -i $INVENTORY_FILE nebula-s1 -m shell -a 'tail -20 /etc/hosts'"
echo ""
echo "# 测试域名解析"
echo "ansible -i $INVENTORY_FILE mysql-s1 -m shell -a 'ping -c 2 redis-s1'"
echo ""
echo "# 检查所有服务器的配置"
echo "ansible -i $INVENTORY_FILE cbp_all_servers -m shell -a 'grep -A 25 \"CBP CLUSTER HOSTS\" /etc/hosts'"
