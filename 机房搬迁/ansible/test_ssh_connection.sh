#!/bin/bash

# CBP应用服务器SSH连接测试脚本
# 用法: ./test_ssh_connection.sh

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}CBP应用服务器SSH连接测试${NC}"
echo -e "${BLUE}========================================${NC}"

# 检查inventory文件
INVENTORY_FILE="inventory/app_server.ini"
if [[ ! -f "$INVENTORY_FILE" ]]; then
    echo -e "${RED}错误: 找不到inventory文件: $INVENTORY_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}测试方法选择:${NC}"
echo "1. 使用密码认证测试 (推荐)"
echo "2. 使用SSH密钥测试"
echo "3. 测试单个服务器连接"
echo "4. 批量设置SSH密钥"
echo ""

read -p "请选择测试方法 (1-4): " -n 1 -r
echo

case $REPLY in
    1)
        echo -e "${YELLOW}使用密码认证测试所有服务器...${NC}"
        ansible -i $INVENTORY_FILE cbp_all_servers -m ping --ask-pass
        ;;
    2)
        echo -e "${YELLOW}使用SSH密钥测试所有服务器...${NC}"
        ansible -i $INVENTORY_FILE cbp_all_servers -m ping
        ;;
    3)
        echo -e "${YELLOW}测试单个服务器连接...${NC}"
        echo "可用服务器列表:"
        echo "- nebula-s1 (192.168.13.1)"
        echo "- mysql-s1 (192.168.13.4)"
        echo "- oracle-s1 (192.168.13.5)"
        echo "- app-s2 (192.168.13.7)"
        echo "- redis-s1 (192.168.13.78)"
        echo ""
        read -p "请输入服务器名称: " server_name
        echo -e "${YELLOW}测试连接到 $server_name...${NC}"
        ansible -i $INVENTORY_FILE $server_name -m ping --ask-pass
        ;;
    4)
        echo -e "${YELLOW}批量设置SSH密钥...${NC}"
        echo "这将为所有CBP服务器设置SSH密钥认证"
        echo ""
        read -p "确认继续? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${YELLOW}开始设置SSH密钥...${NC}"
            
            # 检查是否存在SSH密钥
            if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
                echo -e "${YELLOW}生成SSH密钥...${NC}"
                ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -N ""
            fi
            
            # 服务器IP列表
            servers=(
                "192.168.13.1"  # nebula-s1
                "192.168.13.2"  # nebula-s2
                "192.168.13.3"  # nebula-s3
                "192.168.13.4"  # mysql-s1
                "192.168.13.5"  # oracle-s1
                "192.168.13.7"  # app-s2
                "192.168.13.8"  # app-s3
                "192.168.13.9"  # app-s4
                "192.168.13.10" # app-s5
                "192.168.13.11" # app-s6
                "192.168.13.12" # app-s7
                "192.168.13.13" # app-s8
                "192.168.13.14" # app-s9
                "192.168.13.15" # app-s10
                "192.168.13.16" # app-s11
                "192.168.13.17" # app-s12
                "192.168.13.30" # app-s21
                "192.168.13.31" # app-s22
                "192.168.13.78" # redis-s1
                "192.168.13.79" # redis-s2
            )
            
            echo "需要为以下服务器设置SSH密钥:"
            for server in "${servers[@]}"; do
                echo "- root@$server"
            done
            echo ""
            
            read -p "请输入SSH密码: " -s password
            echo ""
            
            for server in "${servers[@]}"; do
                echo -e "${YELLOW}设置 root@$server...${NC}"
                sshpass -p "$password" ssh-copy-id -o StrictHostKeyChecking=no root@$server 2>/dev/null || echo -e "${RED}失败: $server${NC}"
            done
            
            echo -e "${GREEN}SSH密钥设置完成!${NC}"
            echo "现在可以使用密钥认证:"
            echo "ansible -i $INVENTORY_FILE cbp_all_servers -m ping"
        fi
        ;;
    *)
        echo -e "${YELLOW}操作已取消${NC}"
        exit 0
        ;;
esac

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}测试完成!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}如果连接成功，可以执行:${NC}"
echo "# 使用密码认证执行playbook"
echo "ansible-playbook -i $INVENTORY_FILE update-cbp-hosts.yml --ask-pass"
echo ""
echo "# 或使用密钥认证执行playbook"
echo "ansible-playbook -i $INVENTORY_FILE update-cbp-hosts.yml"
