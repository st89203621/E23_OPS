#!/bin/bash
# CBP业务系统IP批量修改工具验证脚本
# 用于验证所有必要文件是否正确创建

set -euo pipefail

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}CBP业务系统IP批量修改工具验证${NC}"
echo -e "${BLUE}========================================${NC}"
echo

# 检查必要文件
echo -e "${YELLOW}1. 检查核心文件...${NC}"

files=(
    "cbp-ip-batch-update.yml"
    "cbp-ip-precheck.yml"
    "cbp-ip-rollback.yml"
    "run_cbp_ip_update.sh"
    "inventory/cbp_servers.ini"
    "vars/ip_mapping.yml"
    "CBP_IP批量修改使用说明.md"
)

missing_files=()
for file in "${files[@]}"; do
    if [[ -f "${SCRIPT_DIR}/${file}" ]]; then
        echo -e "  ${GREEN}✓${NC} ${file}"
    else
        echo -e "  ${RED}✗${NC} ${file} (缺失)"
        missing_files+=("${file}")
    fi
done

if [[ ${#missing_files[@]} -gt 0 ]]; then
    echo -e "${RED}发现缺失文件，请检查！${NC}"
    exit 1
fi

echo

# 检查YAML语法
echo -e "${YELLOW}2. 检查YAML文件语法...${NC}"

yaml_files=(
    "cbp-ip-batch-update.yml"
    "cbp-ip-precheck.yml"
    "cbp-ip-rollback.yml"
    "vars/ip_mapping.yml"
)

for yaml_file in "${yaml_files[@]}"; do
    if command -v python3 &> /dev/null; then
        if python3 -c "import yaml; yaml.safe_load(open('${SCRIPT_DIR}/${yaml_file}'))" 2>/dev/null; then
            echo -e "  ${GREEN}✓${NC} ${yaml_file} 语法正确"
        else
            echo -e "  ${RED}✗${NC} ${yaml_file} 语法错误"
        fi
    else
        echo -e "  ${YELLOW}!${NC} ${yaml_file} (无法验证，Python3未安装)"
    fi
done

echo

# 检查IP映射配置
echo -e "${YELLOW}3. 检查IP映射配置...${NC}"

if [[ -f "${SCRIPT_DIR}/vars/ip_mapping.yml" ]]; then
    ip_count=$(grep -c "old_ip:" "${SCRIPT_DIR}/vars/ip_mapping.yml" || echo "0")
    echo -e "  ${GREEN}✓${NC} 发现 ${ip_count} 个IP映射配置"
    
    # 检查IP顺序（从大到小）
    if command -v python3 &> /dev/null; then
        python3 << 'EOF'
import yaml
import sys

try:
    with open('机房搬迁/ansible/vars/ip_mapping.yml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    if 'ip_mappings' in config:
        ips = [mapping['old_ip'] for mapping in config['ip_mappings']]
        sorted_ips = sorted(ips, key=lambda x: tuple(map(int, x.split('.'))), reverse=True)
        
        if ips == sorted_ips:
            print("  \033[0;32m✓\033[0m IP地址已按从大到小正确排序")
        else:
            print("  \033[0;31m✗\033[0m IP地址排序不正确，可能导致部分替换问题")
            sys.exit(1)
    else:
        print("  \033[0;31m✗\033[0m 未找到ip_mappings配置")
        sys.exit(1)
        
except Exception as e:
    print(f"  \033[0;31m✗\033[0m 检查IP排序时出错: {e}")
    sys.exit(1)
EOF
    fi
else
    echo -e "  ${RED}✗${NC} IP映射配置文件不存在"
fi

echo

# 检查服务器清单
echo -e "${YELLOW}4. 检查服务器清单配置...${NC}"

if [[ -f "${SCRIPT_DIR}/inventory/cbp_servers.ini" ]]; then
    server_count=$(grep -c "ansible_host=" "${SCRIPT_DIR}/inventory/cbp_servers.ini" || echo "0")
    echo -e "  ${GREEN}✓${NC} 发现 ${server_count} 个服务器配置"
    
    # 检查必要的组
    groups=("cbp_all_servers" "nginx_servers" "app_servers")
    for group in "${groups[@]}"; do
        if grep -q "\[${group}\]" "${SCRIPT_DIR}/inventory/cbp_servers.ini"; then
            echo -e "  ${GREEN}✓${NC} 服务器组 ${group} 已配置"
        else
            echo -e "  ${RED}✗${NC} 服务器组 ${group} 缺失"
        fi
    done
else
    echo -e "  ${RED}✗${NC} 服务器清单文件不存在"
fi

echo

# 检查脚本权限
echo -e "${YELLOW}5. 检查脚本权限...${NC}"

if [[ -f "${SCRIPT_DIR}/run_cbp_ip_update.sh" ]]; then
    if [[ -x "${SCRIPT_DIR}/run_cbp_ip_update.sh" ]]; then
        echo -e "  ${GREEN}✓${NC} run_cbp_ip_update.sh 具有执行权限"
    else
        echo -e "  ${YELLOW}!${NC} run_cbp_ip_update.sh 缺少执行权限"
        echo -e "    请执行: chmod +x ${SCRIPT_DIR}/run_cbp_ip_update.sh"
    fi
fi

echo

# 显示使用建议
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}验证完成！${NC}"
echo -e "${BLUE}========================================${NC}"
echo
echo -e "${GREEN}下一步操作建议：${NC}"
echo
echo -e "1. ${YELLOW}配置SSH密钥认证${NC}（如未配置）:"
echo "   ssh-keygen -t rsa -b 4096"
echo "   for host in 192.168.13.{1..17,30,31,78,79}; do ssh-copy-id root@\$host; done"
echo
echo -e "2. ${YELLOW}测试服务器连通性${NC}:"
echo "   ansible -i inventory/cbp_servers.ini all -m ping"
echo
echo -e "3. ${YELLOW}执行预检查${NC}:"
echo "   ./run_cbp_ip_update.sh precheck"
echo
echo -e "4. ${YELLOW}查看详细使用说明${NC}:"
echo "   cat CBP_IP批量修改使用说明.md"
echo
echo -e "${GREEN}工具已准备就绪！${NC}"
echo
