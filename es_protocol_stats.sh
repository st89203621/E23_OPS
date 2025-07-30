#!/bin/bash

# ES协议每日数据量统计脚本 - 简化版本（不依赖jq）
# 使用方法: ./es_protocol_stats.sh [ES_HOST] [ES_PORT]

# 设置默认参数
ES_HOST=${1:-"192.168.14.1"}
ES_PORT=${2:-"9200"}
ES_URL="http://${ES_HOST}:${ES_PORT}"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${BLUE}ES协议每日数据量统计 - 简化版本${NC}"
echo "========================================"
echo "ES地址: $ES_URL"
echo "开始时间: $(date)"
echo ""

# 检查curl
if ! command -v curl &> /dev/null; then
    echo -e "${RED}错误: 需要安装curl${NC}"
    exit 1
fi

# 主要协议列表（可根据需要调整）
protocols=(
    "http" "mobilenetradius" "im" "location" "sms" "voip" 
    "email" "tool" "terminal" "ftp" "remotectrl" "vpn"
    "telnet" "fixednetradius" "call" "other" "sns"
)

# 生成月份模式
current_month=$(date +%Y%m)
last_month=$(date -d "1 month ago" +%Y%m)

echo "查询月份: $last_month, $current_month"
echo "主要协议数: ${#protocols[@]}"
echo ""

# 测试ES连接
echo -e "${YELLOW}测试ES连接...${NC}"
health_response=$(curl -s --connect-timeout 10 "$ES_URL/_cluster/health")
if [ $? -ne 0 ] || [ -z "$health_response" ]; then
    echo -e "${RED}ES连接失败，请检查ES服务状态${NC}"
    exit 1
fi
echo -e "${GREEN}ES连接成功${NC}"
echo ""

# 创建临时目录
temp_dir=$(mktemp -d)
trap "rm -rf $temp_dir" EXIT

# 简单的JSON解析函数（不依赖jq）
parse_buckets() {
    local response="$1"
    local protocol="$2"
    local month="$3"
    
    # 使用sed和awk解析JSON中的buckets数据
    echo "$response" | sed -n '/"buckets":/,/]/p' | \
    grep -o '"key":"[^"]*","doc_count":[0-9]*' | \
    sed 's/"key":"//g; s/","doc_count":/,/g' >> "$temp_dir/${protocol}_${month}.csv"
}

# 查询协议函数
query_protocol() {
    local protocol=$1
    local month_pattern=$2
    local index_pattern="deye_v64_${protocol}_*${month_pattern}*"
    
    # 构建查询JSON
    local query='{"size":0,"aggs":{"daily_count":{"terms":{"field":"capture_dayField","size":1000,"order":{"_key":"desc"}}}}}'
    
    # 执行查询
    local response=$(curl -s -X POST "$ES_URL/$index_pattern/_search" \
        -H "Content-Type: application/json" \
        -d "$query" \
        --connect-timeout 60 \
        --max-time 60)
    
    # 检查响应是否包含buckets
    if echo "$response" | grep -q '"buckets"'; then
        parse_buckets "$response" "$protocol" "$month_pattern"
        return 0
    else
        return 1
    fi
}

# 查询所有协议
declare -A protocol_data
protocol_count=0

for protocol in "${protocols[@]}"; do
    ((protocol_count++))
    echo -e "${CYAN}[$protocol_count/${#protocols[@]}] 查询协议: $protocol${NC}"
    
    # 初始化协议数据
    protocol_data["$protocol"]=0
    
    # 查询当前月份
    query_protocol "$protocol" "$current_month"
    current_success=$?
    
    # 查询上个月份
    query_protocol "$protocol" "$last_month"
    last_success=$?
    
    # 处理查询结果
    if [ $current_success -eq 0 ] || [ $last_success -eq 0 ]; then
        # 合并数据
        if ls "$temp_dir/${protocol}_"*.csv 1> /dev/null 2>&1; then
            # 使用awk合并同一天的数据
            total=$(cat "$temp_dir/${protocol}_"*.csv | \
            awk -F',' '{
                if ($1 in dates) {
                    dates[$1] += $2
                } else {
                    dates[$1] = $2
                }
            }
            END {
                total = 0
                for (date in dates) {
                    total += dates[date]
                    print date "," dates[date] > "'$temp_dir'/'$protocol'_final.csv"
                }
                print total
            }')
            
            protocol_data["$protocol"]=$total
            days=$(wc -l < "$temp_dir/${protocol}_final.csv")
            echo -e "  ${GREEN}找到 $days 天有数据，总计: $(printf "%'d" $total) 条记录${NC}"
        else
            echo -e "  ${YELLOW}未找到数据${NC}"
            touch "$temp_dir/${protocol}_final.csv"
        fi
    else
        echo -e "  ${YELLOW}查询失败或无数据${NC}"
        touch "$temp_dir/${protocol}_final.csv"
    fi
done

echo ""
echo -e "${YELLOW}生成CSV文件...${NC}"

# 生成最近15天的日期列表（简化版本）
dates=()
for i in $(seq 14 -1 0); do
    date=$(date -d "$i days ago" +%Y-%m-%d)
    dates+=("$date")
done

# 创建CSV文件
timestamp=$(date +%Y%m%d_%H%M%S)
csv_file="es_protocol_stats_simple_${timestamp}.csv"

# 获取有数据的协议
active_protocols=()
for protocol in "${protocols[@]}"; do
    if [ -s "$temp_dir/${protocol}_final.csv" ]; then
        active_protocols+=("$protocol")
    fi
done

# 写入表头
{
    echo -n "Date"
    for protocol in "${active_protocols[@]}"; do
        echo -n ",$protocol"
    done
    echo ",Daily_Total"
} > "$csv_file"

# 写入数据行
grand_total=0
for date in "${dates[@]}"; do
    echo -n "$date" >> "$csv_file"
    daily_total=0
    
    for protocol in "${active_protocols[@]}"; do
        count=0
        if [ -f "$temp_dir/${protocol}_final.csv" ]; then
            count=$(grep "^$date," "$temp_dir/${protocol}_final.csv" 2>/dev/null | cut -d',' -f2)
            count=${count:-0}
        fi
        daily_total=$((daily_total + count))
        echo -n ",$count" >> "$csv_file"
    done
    
    grand_total=$((grand_total + daily_total))
    echo ",$daily_total" >> "$csv_file"
done

# 写入汇总行
{
    echo -n "Protocol_Total"
    for protocol in "${active_protocols[@]}"; do
        total=${protocol_data["$protocol"]}
        echo -n ",$total"
    done
    echo ",$grand_total"
} >> "$csv_file"

echo -e "${GREEN}CSV文件已保存: $csv_file${NC}"

# 显示统计摘要
echo ""
echo -e "${BLUE}===============================================${NC}"
echo -e "${BLUE}统计摘要${NC}"
echo -e "${BLUE}===============================================${NC}"
echo "查询的协议数: ${#protocols[@]}"
echo "有数据的协议数: ${#active_protocols[@]}"
echo "日期范围: ${dates[0]} 到 ${dates[-1]}"
echo "总记录数: $(printf "%'d" $grand_total)"
echo ""

echo -e "${YELLOW}协议数据量排序:${NC}"
# 创建临时文件用于排序
sort_file="$temp_dir/protocol_sort.txt"
for protocol in "${active_protocols[@]}"; do
    total=${protocol_data["$protocol"]}
    echo "$total $protocol" >> "$sort_file"
done

# 排序并显示
sort -nr "$sort_file" | while read total protocol; do
    if [ $grand_total -gt 0 ]; then
        percentage=$(awk "BEGIN {printf \"%.1f\", $total * 100 / $grand_total}")
    else
        percentage="0.0"
    fi
    printf "%-20s: %15s 条记录 (%s%%)\n" "$protocol" "$(printf "%'d" $total)" "$percentage"
done

echo ""
echo -e "${GREEN}完成时间: $(date)${NC}"
echo -e "${GREEN}CSV文件: $csv_file${NC}"

# 显示使用说明
echo ""
echo -e "${BLUE}使用说明:${NC}"
echo "1. 给脚本添加执行权限: chmod +x es_protocol_stats_simple.sh"
echo "2. 运行脚本: ./es_protocol_stats_simple.sh [ES_HOST] [ES_PORT]"
echo "3. CSV文件保存在当前目录，可用Excel等工具打开"
echo "4. 如需查询更多协议，请修改脚本中的protocols数组"
