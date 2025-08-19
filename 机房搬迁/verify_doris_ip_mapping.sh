#!/bin/bash

# Doris IP映射验证脚本
# 验证doris_new.ini文件中的IP映射是否与设备列表一致

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 预期的IP映射关系（基于设备列表）
declare -A EXPECTED_MAPPING=(
    ["doris-s1"]="192.168.28.101"
    ["doris-s2"]="192.168.28.102"
    ["doris-s3"]="192.168.28.103"
    ["doris-s4"]="192.168.28.104"
    ["doris-s5"]="192.168.28.105"
    ["doris-s6"]="192.168.28.106"
    ["doris-s7"]="192.168.28.107"
    ["doris-s8"]="192.168.28.108"
    ["doris-s9"]="192.168.28.109"
    ["doris-s10"]="192.168.28.110"
    ["doris-s11"]="192.168.28.111"
    ["doris-s12"]="192.168.28.112"
    ["doris-s13"]="192.168.28.113"
    ["doris-s14"]="192.168.28.114"
    ["doris-s15"]="192.168.28.115"
    ["doris-s16"]="192.168.28.116"
    ["doris-s17"]="192.168.28.117"
    ["doris-s18"]="192.168.28.118"
    ["doris-s19"]="192.168.28.119"
    ["doris-s20"]="192.168.28.120"
    ["doris-s21"]="192.168.28.121"
    ["doris-s22"]="192.168.28.122"
    ["doris-s23"]="192.168.28.123"
    ["doris-s24"]="192.168.28.124"
    ["doris-s25"]="192.168.28.125"
    ["doris-s26"]="192.168.28.126"
    ["doris-s27"]="192.168.28.127"
    ["doris-s28"]="192.168.28.128"
    ["doris-s29"]="192.168.28.129"
    ["doris-s30"]="192.168.28.130"
    ["doris-s31"]="192.168.28.131"
    ["doris-s32"]="192.168.28.132"
    ["doris-s33"]="192.168.28.133"
)

# FE节点列表
FE_NODES=("doris-s1" "doris-s3" "doris-s11" "doris-s12" "doris-s20")
MASTER_NODE="doris-s12"

# 工作目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DORIS_INI_FILE="$SCRIPT_DIR/ansible/inventory/doris_new.ini"

echo -e "${BLUE}=================================="
echo -e "Doris IP映射验证脚本"
echo -e "==================================${NC}"
echo ""

# 检查文件是否存在
if [[ ! -f "$DORIS_INI_FILE" ]]; then
    log_error "doris_new.ini文件不存在: $DORIS_INI_FILE"
    exit 1
fi

log_info "验证文件: $DORIS_INI_FILE"
echo ""

# 验证IP映射
log_info "验证IP映射关系..."
all_correct=true

for hostname in "${!EXPECTED_MAPPING[@]}"; do
    expected_ip="${EXPECTED_MAPPING[$hostname]}"
    
    # 从文件中提取实际IP
    actual_ip=$(grep "^$hostname " "$DORIS_INI_FILE" | head -1 | sed -n 's/.*ansible_host=\([0-9.]*\).*/\1/p')
    
    if [[ "$actual_ip" == "$expected_ip" ]]; then
        log_success "$hostname: $actual_ip ✓"
    else
        log_error "$hostname: 期望 $expected_ip, 实际 $actual_ip ✗"
        all_correct=false
    fi
done

echo ""

# 验证FE节点配置
log_info "验证FE节点配置..."
fe_correct=true

for fe_node in "${FE_NODES[@]}"; do
    if grep -q "^\[doris_fe\]" -A 20 "$DORIS_INI_FILE" | grep -q "^$fe_node "; then
        log_success "FE节点 $fe_node 配置正确 ✓"
    else
        log_error "FE节点 $fe_node 配置缺失 ✗"
        fe_correct=false
    fi
done

# 验证Master节点
if grep -q "^$MASTER_NODE.*role=master" "$DORIS_INI_FILE"; then
    log_success "Master节点 $MASTER_NODE 配置正确 ✓"
else
    log_error "Master节点 $MASTER_NODE 配置错误 ✗"
    fe_correct=false
fi

echo ""

# 验证网络配置
log_info "验证网络配置..."
network_correct=true

# 检查priority_networks配置
if grep -q "priority_networks=192.168.28.0/22" "$DORIS_INI_FILE"; then
    log_success "priority_networks配置正确 ✓"
else
    log_warning "priority_networks配置可能需要调整"
    network_correct=false
fi

# 检查网关配置
if grep -q "gateway=192.168.31.254" "$DORIS_INI_FILE"; then
    log_success "网关配置正确 ✓"
else
    log_warning "网关配置可能需要调整"
    network_correct=false
fi

# 检查子网掩码配置
if grep -q "subnet_mask=255.255.252.0" "$DORIS_INI_FILE"; then
    log_success "子网掩码配置正确 ✓"
else
    log_warning "子网掩码配置可能需要调整"
    network_correct=false
fi

echo ""

# 总结
log_info "验证总结:"
if [[ "$all_correct" == "true" && "$fe_correct" == "true" ]]; then
    log_success "✅ 所有IP映射和配置验证通过！"
    echo ""
    log_info "关键配置信息:"
    echo "  - FE节点: ${FE_NODES[*]}"
    echo "  - Master节点: $MASTER_NODE"
    echo "  - IP范围: 192.168.28.101-133"
    echo "  - 子网掩码: 255.255.252.0 (/22)"
    echo "  - 网关: 192.168.31.254"
    echo ""
    log_success "配置文件已准备就绪，可以用于机房搬迁！"
else
    log_error "❌ 发现配置错误，请检查并修正！"
    exit 1
fi

if [[ "$network_correct" == "false" ]]; then
    log_warning "⚠️  网络配置可能需要根据实际环境进行调整"
fi
