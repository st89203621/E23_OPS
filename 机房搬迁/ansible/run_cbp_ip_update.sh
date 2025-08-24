#!/bin/bash
# CBP业务系统IP批量修改执行脚本
# 作者: 系统运维团队
# 日期: 2025-08-24
# 用法: ./run_cbp_ip_update.sh [precheck|update|rollback|help]

set -euo pipefail

# 脚本配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INVENTORY_FILE="${SCRIPT_DIR}/inventory/cbp_servers.ini"
LOG_DIR="/var/log/ansible"
LOG_FILE="${LOG_DIR}/cbp_ip_update_$(date +%Y%m%d_%H%M%S).log"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log() {
    echo -e "${1}" | tee -a "${LOG_FILE}"
}

log_info() {
    log "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - ${1}"
}

log_warn() {
    log "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - ${1}"
}

log_error() {
    log "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - ${1}"
}

log_success() {
    log "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - ${1}"
}

# 检查依赖
check_dependencies() {
    log_info "检查依赖环境..."
    
    # 检查ansible
    if ! command -v ansible-playbook &> /dev/null; then
        log_error "Ansible未安装，请先安装Ansible"
        exit 1
    fi
    
    # 检查Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python3未安装，请先安装Python3"
        exit 1
    fi
    
    # 检查inventory文件
    if [[ ! -f "${INVENTORY_FILE}" ]]; then
        log_error "Inventory文件不存在: ${INVENTORY_FILE}"
        exit 1
    fi
    
    # 创建日志目录
    mkdir -p "${LOG_DIR}"
    
    log_success "依赖检查通过"
}

# 显示帮助信息
show_help() {
    cat << EOF
CBP业务系统IP批量修改工具
========================

用法: $0 [命令] [选项]

命令:
  precheck    执行预检查，评估系统状态和风险
  update      执行IP批量修改（需要先通过预检查）
  rollback    回滚IP修改到之前的状态
  help        显示此帮助信息

选项:
  --dry-run   预演模式，不实际修改文件
  --force     强制执行，跳过确认提示
  --verbose   详细输出模式

示例:
  $0 precheck                    # 执行预检查
  $0 update                      # 执行IP批量修改
  $0 update --dry-run            # 预演IP修改
  $0 rollback                    # 回滚修改
  $0 help                        # 显示帮助

文件说明:
  - inventory/cbp_servers.ini    服务器清单配置
  - vars/ip_mapping.yml          IP映射配置
  - cbp-ip-precheck.yml          预检查playbook
  - cbp-ip-batch-update.yml      IP批量修改playbook
  - cbp-ip-rollback.yml          回滚playbook

注意事项:
  1. 执行前请确保已配置好SSH密钥认证
  2. 建议先执行precheck检查系统状态
  3. IP替换按从大到小顺序执行，避免部分替换问题
  4. 所有操作都会自动备份原始配置文件
  5. 如有问题可使用rollback命令回滚

EOF
}

# 执行预检查
run_precheck() {
    log_info "开始执行CBP业务系统IP批量修改预检查..."
    
    local playbook="${SCRIPT_DIR}/cbp-ip-precheck.yml"
    if [[ ! -f "${playbook}" ]]; then
        log_error "预检查playbook不存在: ${playbook}"
        exit 1
    fi
    
    log_info "执行命令: ansible-playbook -i ${INVENTORY_FILE} ${playbook}"
    
    if ansible-playbook -i "${INVENTORY_FILE}" "${playbook}" 2>&1 | tee -a "${LOG_FILE}"; then
        log_success "预检查执行完成"
        log_info "请查看预检查报告，确认无高风险项目后再执行update命令"
    else
        log_error "预检查执行失败，请检查日志: ${LOG_FILE}"
        exit 1
    fi
}

# 执行IP批量修改
run_update() {
    local dry_run_flag=""
    local force_flag=""
    
    # 解析参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                dry_run_flag="--extra-vars dry_run=true"
                log_info "启用预演模式"
                shift
                ;;
            --force)
                force_flag="true"
                log_info "启用强制模式"
                shift
                ;;
            --verbose)
                set -x
                shift
                ;;
            *)
                log_warn "未知参数: $1"
                shift
                ;;
        esac
    done
    
    log_info "开始执行CBP业务系统IP批量修改..."
    
    local playbook="${SCRIPT_DIR}/cbp-ip-batch-update.yml"
    if [[ ! -f "${playbook}" ]]; then
        log_error "IP批量修改playbook不存在: ${playbook}"
        exit 1
    fi
    
    # 确认操作
    if [[ "${force_flag}" != "true" ]]; then
        echo
        log_warn "⚠️  即将执行IP批量修改操作，这将影响CBP业务系统的网络配置"
        log_warn "请确保已经执行过预检查并解决了所有高风险项目"
        echo
        read -p "确认继续执行吗？(yes/no): " confirm
        if [[ "${confirm}" != "yes" ]]; then
            log_info "用户取消操作"
            exit 0
        fi
    fi
    
    log_info "执行命令: ansible-playbook -i ${INVENTORY_FILE} ${playbook} ${dry_run_flag}"
    
    if ansible-playbook -i "${INVENTORY_FILE}" "${playbook}" ${dry_run_flag} 2>&1 | tee -a "${LOG_FILE}"; then
        if [[ -n "${dry_run_flag}" ]]; then
            log_success "IP批量修改预演完成"
            log_info "预演结果已保存到日志文件: ${LOG_FILE}"
        else
            log_success "IP批量修改执行完成"
            log_info "请及时验证业务功能是否正常"
            log_info "如有问题，可使用 $0 rollback 命令回滚"
        fi
    else
        log_error "IP批量修改执行失败，请检查日志: ${LOG_FILE}"
        log_info "如需回滚，请执行: $0 rollback"
        exit 1
    fi
}

# 执行回滚
run_rollback() {
    log_info "开始执行CBP业务系统IP配置回滚..."
    
    local playbook="${SCRIPT_DIR}/cbp-ip-rollback.yml"
    if [[ ! -f "${playbook}" ]]; then
        log_error "回滚playbook不存在: ${playbook}"
        exit 1
    fi
    
    # 确认操作
    echo
    log_warn "⚠️  即将执行IP配置回滚操作，这将恢复到修改前的配置"
    echo
    read -p "确认继续执行回滚吗？(yes/no): " confirm
    if [[ "${confirm}" != "yes" ]]; then
        log_info "用户取消回滚操作"
        exit 0
    fi
    
    log_info "执行命令: ansible-playbook -i ${INVENTORY_FILE} ${playbook}"
    
    if ansible-playbook -i "${INVENTORY_FILE}" "${playbook}" 2>&1 | tee -a "${LOG_FILE}"; then
        log_success "IP配置回滚执行完成"
        log_info "请验证业务功能是否恢复正常"
    else
        log_error "IP配置回滚执行失败，请检查日志: ${LOG_FILE}"
        exit 1
    fi
}

# 主函数
main() {
    # 检查参数
    if [[ $# -eq 0 ]]; then
        show_help
        exit 0
    fi
    
    # 检查依赖
    check_dependencies
    
    # 解析命令
    case "${1}" in
        precheck)
            shift
            run_precheck "$@"
            ;;
        update)
            shift
            run_update "$@"
            ;;
        rollback)
            shift
            run_rollback "$@"
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "未知命令: ${1}"
            echo
            show_help
            exit 1
            ;;
    esac
}

# 脚本开始
log_info "CBP业务系统IP批量修改工具启动"
log_info "日志文件: ${LOG_FILE}"
log_info "工作目录: ${SCRIPT_DIR}"

# 执行主函数
main "$@"
