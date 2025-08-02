#!/bin/bash
# 批量后台执行NF和PR关联准确度统计脚本

echo "开始执行NF和PR关联准确度统计..."
echo "执行时间: $(date)"

# 进入脚本目录
cd "$(dirname "$0")"

# 创建日志和输出目录
mkdir -p logs
mkdir -p output_new

# 后台执行所有统计脚本
echo "启动NF昨天关联准确度统计..."
nohup bash nf_acc_yesterday.sh > logs/nf_acc_yesterday.log 2>&1 &
nf_yesterday_pid=$!

echo "启动NF前天关联准确度统计..."
nohup bash nf_acc_day_before.sh > logs/nf_acc_day_before.log 2>&1 &
nf_day_before_pid=$!

echo "启动PR昨天关联准确度统计..."
nohup bash pr_acc_yesterday.sh > logs/pr_acc_yesterday.log 2>&1 &
pr_yesterday_pid=$!

echo "启动PR前天关联准确度统计..."
nohup bash pr_acc_day_before.sh > logs/pr_acc_day_before.log 2>&1 &
pr_day_before_pid=$!

# 输出进程信息
echo "所有统计任务已启动，进程ID如下："
echo "NF昨天关联准确度: $nf_yesterday_pid"
echo "NF前天关联准确度: $nf_day_before_pid"
echo "PR昨天关联准确度: $pr_yesterday_pid"
echo "PR前天关联准确度: $pr_day_before_pid"

echo ""
echo "可以使用以下命令查看执行状态："
echo "ps -ef | grep -E '(nf_acc|pr_acc)'"
echo ""
echo "查看日志："
echo "tail -f logs/nf_acc_yesterday.log"
echo "tail -f logs/nf_acc_day_before.log"
echo "tail -f logs/pr_acc_yesterday.log"
echo "tail -f logs/pr_acc_day_before.log"
echo ""
echo "结果输出到新目录: ./output_new/"

# 可选：等待所有任务完成
# wait $nf_yesterday_pid $nf_day_before_pid $pr_yesterday_pid $pr_day_before_pid
# echo "所有统计任务已完成"
