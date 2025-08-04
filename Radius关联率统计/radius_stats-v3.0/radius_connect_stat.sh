#!/bin/bash

#sparksql提交查询任务
submit_task() {
    task_name=$3
    spark_param="--master yarn --name ${task_name} \
    --conf spark.driver.memory=8g \
    --conf spark.driver.cores=4 \
    --conf spark.executor.memory=48g \
    --conf spark.executor.cores=4 \
    --conf spark.executor.instances=24 \
    --conf spark.default.parallelism=96 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.minPartitionNum=1 \
    --conf spark.sql.files.maxPartitionBytes=268435456 \
    --conf spark.sql.files.openCostInBytes=8388608 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.localShuffleReader.enabled=true"
    spark-sql -e "$1" $spark_param >> $2
    # 将csv分隔符改为逗号
    sed -i "s/\t/,/g" $2
}

# 读取配置文件 query.conf 中的 start_day 和 end_day 参数
read_config() {
    # 读取配置文件并存储参数
    while IFS='=' read -r key value; do
        case $key in
            start_day)
                start_day=$value
                ;;
            end_day)
                end_day=$value
                ;;
            start_hour)
                start_hour=$value
                ;;
            end_hour)
                end_hour=$value
                ;;
            pr_offline_time)
                pr_offline_time=$value
                ;;
            nf_offline_time)
                nf_offline_time=$value
                ;;
        esac
    done < $1
}

read_config "query.conf"

#获取环境变量
. /etc/profile

# 将日期转换为秒数，以便计算日期差
start_seconds=$(date -d "$start_day" +%s)
end_seconds=$(date -d "$end_day" +%s)

current_date=$(date +"%Y-%m-%d")

if [ "$start_hour" -ge 0 ] && [ "$start_hour" -lt 10 ]; then
    start_hour="0$start_hour"
else
    start_hour="$start_hour"
fi

if [ "$end_hour" -ge 0 ] && [ "$end_hour" -lt 10 ]; then
    end_hour="0$end_hour"
else
    end_hour="$end_hour"
fi

# 计算nf固网关联率
nf_fix_con_rate() {
    echo "==========start nf_fix_con_rate============"
    for uparea_id in 210213 220214 230215; do
        nf_fix_con_rate=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{uparea_id}/${uparea_id}/g" ./template/nf_fix_con_rate.sql`
        echo "$nf_fix_con_rate" > sql/nf_fix_con_rate.sql
        submit_task "$nf_fix_con_rate" "./output/${1}/radius_connect_$1.csv" "nf_fix_con_rate"
    done
    echo "==========end nf_fix_con_rate============"
}

# 计算nf固网关联率准确率
nf_fix_acc_rate() {
    echo "==========start nf_fix_acc_rate============"
    for uparea_id in 210213 220214 230215; do
        nf_fix_acc_rate=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{nf_offline_time}/${nf_offline_time}/g;s/{uparea_id}/${uparea_id}/g" ./template/nf_fix_acc_rate.sql`
        echo "$nf_fix_acc_rate" > sql/nf_fix_acc_rate.sql
        submit_task "$nf_fix_acc_rate" "./output/${1}/radius_connect_$1.csv" "nf_fix_acc_rate"
    done
    echo "==========end nf_fix_acc_rate============"
}

# 计算nf移网关联率
nf_mob_con_rate() {
    echo "==========start nf_mob_con_rate============"
    for uparea_id in 210213 220214 230215; do
        nf_mob_con_rate=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{uparea_id}/${uparea_id}/g" ./template/nf_mob_con_rate.sql`
        echo "$nf_mob_con_rate" > sql/nf_mob_con_rate.sql
        submit_task "$nf_mob_con_rate" "./output/${1}/radius_connect_$1.csv" "nf_mob_con_rate"
    done
    echo "==========end nf_mob_con_rate============"
}

# 计算nf移网关联率准确率
nf_mob_acc_rate() {
    echo "==========start nf_mob_acc_rate============"
    # 循环210213、220214、230215这3个局点
    for uparea_id in 210213 220214 230215; do
        # 获取移动用户关联率
        nf_mob_acc_rate=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{nf_offline_time}/${nf_offline_time}/g;s/{uparea_id}/${uparea_id}/g" ./template/nf_mob_acc_rate.sql`
        echo "$nf_mob_acc_rate" > sql/nf_mob_acc_rate.sql
        submit_task "$nf_mob_acc_rate" "./output/${1}/radius_connect_$1.csv" "nf_mob_acc_rate"
    done
    echo "==========end nf_mob_acc_rate============"
}

# 计算nf固网关联率-当日
nf_fix_con_rate_today() {
    echo "==========start nf_fix_con_rate_today============"
    for uparea_id in 210213 220214 230215; do
        nf_fix_con_rate_today=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{start_time}/${2}/g;s/{end_time}/${3}/g;s/{uparea_id}/${uparea_id}/g" ./template/nf_fix_con_rate_today.sql`
        echo "$nf_fix_con_rate_today" > sql/nf_fix_con_rate_today.sql
        submit_task "$nf_fix_con_rate_today" "./output/${1}/radius_connect_$1.csv" "nf_fix_con_rate_today"
    done
    echo "==========end nf_fix_con_rate_today============"
}

# 计算nf固网关联率准确率
nf_fix_acc_rate_today() {
    echo "==========start nf_fix_acc_rate_today============"
    for uparea_id in 210213 220214 230215; do
        nf_fix_acc_rate_today=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{nf_offline_time}/${nf_offline_time}/g;s/{end_hour}/${end_hour}/g;s/{start_time}/${2}/g;s/{end_time}/${3}/g;s/{uparea_id}/${uparea_id}/g" ./template/nf_fix_acc_rate_today.sql`
        echo "$nf_fix_acc_rate_today" > sql/nf_fix_acc_rate_today.sql
        submit_task "$nf_fix_acc_rate_today" "./output/${1}/radius_connect_$1.csv" "nf_fix_acc_rate_today"
    done
    echo "==========end nf_fix_acc_rate_today============"
}

# 计算nf移网关联率-当日
nf_mob_con_rate_today() {
    echo "==========start nf_mob_con_rate_today============"
    for uparea_id in 210213 220214 230215; do
        nf_mob_con_rate_today=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{end_hour}/${end_hour}/g;s/{start_time}/${2}/g;s/{end_time}/${3}/g;s/{uparea_id}/${uparea_id}/g" ./template/nf_mob_con_rate_today.sql`
        echo "$nf_mob_con_rate_today" > sql/nf_mob_con_rate_today.sql
        submit_task "$nf_mob_con_rate_today" "./output/${1}/radius_connect_$1.csv" "nf_mob_con_rate_today"
    done
    echo "==========end nf_mob_con_rate_today============"
}

# 计算nf移网关联率准确率
nf_mob_acc_rate_today() {
    echo "==========start nf_mob_acc_rate_today============"
    for uparea_id in 210213 220214 230215; do
        nf_mob_acc_rate_today=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{nf_offline_time}/${nf_offline_time}/g;s/{end_hour}/${end_hour}/g;s/{start_time}/${2}/g;s/{end_time}/${3}/g;s/{uparea_id}/${uparea_id}/g" ./template/nf_mob_acc_rate_today.sql`
        echo "$nf_mob_acc_rate_today" > sql/nf_mob_acc_rate_today.sql
        submit_task "$nf_mob_acc_rate_today" "./output/${1}/radius_connect_$1.csv" "nf_mob_acc_rate_today"
    done
    echo "==========end nf_mob_acc_rate_today============"
}

# 计算pr固网关联率
pr_fix_con_rate() {
    echo "==========start pr_fix_con_rate============"
    for uparea_id in 210213 220214 230215; do
        pr_fix_con_rate=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{uparea_id}/${uparea_id}/g" ./template/pr_fix_con_rate.sql`
        echo "$pr_fix_con_rate" > sql/pr_fix_con_rate.sql
        submit_task "$pr_fix_con_rate" "./output/${1}/radius_connect_$1.csv" "pr_fix_con_rate"
        echo "==========end pr_fix_con_rate============"
    done
}

# 计算pr固网关联率准确率
pr_fix_acc_rate() {
    for uparea_id in 210213 220214 230215; do
        echo "==========start pr_fix_acc_rate============"
        pr_fix_acc_rate=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{pr_offline_time}/${pr_offline_time}/g;s/{uparea_id}/${uparea_id}/g" ./template/pr_fix_acc_rate.sql`
        echo "$pr_fix_acc_rate" > sql/pr_fix_acc_rate.sql
        submit_task "$pr_fix_acc_rate" "./output/${1}/radius_connect_$1.csv" "pr_fix_acc_rate"
        echo "==========end pr_fix_acc_rate============"
    done
}

# 计算pr移网关联率
pr_mob_con_rate() {
    for uparea_id in 210213 220214 230215; do
        echo "==========start pr_mob_con_rate============"
        pr_mob_con_rate=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{uparea_id}/${uparea_id}/g" ./template/pr_mob_con_rate.sql`
        echo "$pr_mob_con_rate" > sql/pr_mob_con_rate.sql
        submit_task "$pr_mob_con_rate" "./output/${1}/radius_connect_$1.csv" "pr_mob_con_rate"
        echo "==========end pr_mob_con_rate============"
    done
}

# 计算pr移网关联率准确率
pr_mob_acc_rate() {
    for uparea_id in 210213 220214 230215; do
        echo "==========start pr_mob_acc_rate============"
        pr_mob_acc_rate=`sed "s/{cal_day}/${1}/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{pr_offline_time}/${pr_offline_time}/g;s/{uparea_id}/${uparea_id}/g" ./template/pr_mob_acc_rate.sql`
        echo "$pr_mob_acc_rate" > sql/pr_mob_acc_rate.sql
        submit_task "$pr_mob_acc_rate" "./output/${1}/radius_connect_$1.csv" "pr_mob_acc_rate"
        echo "==========end pr_mob_acc_rate============"
    done
}

# 计算所有关联率
start_all(){
    day_seconds=$start_seconds
    while [ $day_seconds -le $end_seconds ]; do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        end_day_time=$(($(date -d "$cal_day 00:00:00" +"%s")*1000+86400000))
        end_hour_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi

        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        nf_fix_con_rate $cal_day
        nf_mob_con_rate $cal_day
        
        pr_fix_con_rate $cal_day
        pr_mob_con_rate $cal_day

        nf_fix_acc_rate $cal_day
        nf_mob_acc_rate $cal_day

        pr_fix_acc_rate $cal_day
        pr_mob_acc_rate $cal_day

        day_seconds=$((day_seconds + 86400))
    done
}

# 计算所有关联率
start_all_today(){
    day_seconds=$start_seconds
    while [ $day_seconds -le $end_seconds ]; do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        end_day_time=$(($(date -d "$cal_day 00:00:00" +"%s")*1000+86400000))
        end_hour_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi

        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        nf_fix_con_rate_today $cal_day
        nf_mob_con_rate_today $cal_day

        pr_fix_con_rate $cal_day
        pr_mob_con_rate $cal_day
        
        nf_mob_acc_rate $cal_day
        pr_mob_acc_rate $cal_day
        nf_fix_acc_rate $cal_day
        pr_fix_acc_rate $cal_day

        day_seconds=$((day_seconds + 86400))
    done
}

# 计算nf关联率
start_nf() {
    day_seconds=$start_seconds
    while [ $day_seconds -le $end_seconds ]; do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        end_day_time=$(($(date -d "$cal_day 00:00:00" +"%s")*1000+86400000))
        end_hour_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi
        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        nf_fix_con_rate $cal_day
        nf_fix_acc_rate $cal_day

        nf_mob_con_rate $cal_day
        nf_mob_acc_rate $cal_day
    done
}

# 计算nf固网关联率
start_nf_fix() {
    for (( day_seconds = start_seconds; day_seconds <= end_seconds; day_seconds += 86400 )); do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        end_day_time=$(($(date -d "$cal_day 00:00:00" +"%s")*1000+86400000))
        end_hour_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi

        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        nf_fix_con_rate $cal_day
        nf_fix_acc_rate $cal_day
    done
}

# 计算nf移动网关联率
start_nf_mob() {
    for (( day_seconds = start_seconds; day_seconds <= end_seconds; day_seconds += 86400 )); do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        end_day_time=$(($(date -d "$cal_day 00:00:00" +"%s")*1000+86400000))
        end_hour_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi

        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        nf_mob_con_rate $cal_day
        nf_mob_acc_rate $cal_day
    done
}

# 计算nf关联率
start_nf_today() {
    for (( day_seconds = start_seconds; day_seconds <= end_seconds; day_seconds += 86400 )); do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        end_day_time=$(($(date -d "$cal_day 00:00:00" +"%s")*1000+86400000))
        start_time=$(($(date -d "$cal_day $start_hour:00:00" +"%s")*1000))
        end_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi
        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        nf_fix_con_rate_today $cal_day $start_time $end_time
        nf_fix_acc_rate_today $cal_day $start_time $end_time

        nf_mob_con_rate_today $cal_day $start_time $end_time
        nf_mob_acc_rate_today $cal_day $start_time $end_time
    done
}

# 计算nf固网关联率
start_nf_fix_today() {
    for (( day_seconds = start_seconds; day_seconds <= end_seconds; day_seconds += 86400 )); do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        start_time=$(($(date -d "$cal_day $start_hour:00:00" +"%s")*1000))
        end_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi

        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        nf_fix_con_rate_today $cal_day $start_time $end_time
        nf_fix_acc_rate_today $cal_day $start_time $end_time
    done
}

# 计算nf移动网关联率
start_nf_mob_today() {
    for (( day_seconds = start_seconds; day_seconds <= end_seconds; day_seconds += 86400 )); do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        start_time=$(($(date -d "$cal_day $start_hour:00:00" +"%s")*1000))
        end_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi

        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        nf_mob_con_rate_today $cal_day $start_time $end_time
        nf_mob_acc_rate_today $cal_day $start_time $end_time
    done
}

# 计算pr关联率
start_pr() {
    for (( day_seconds = start_seconds; day_seconds <= end_seconds; day_seconds += 86400 )); do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        end_day_time=$(($(date -d "$cal_day 00:00:00" +"%s")*1000+86400000))
        end_hour_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi
        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        pr_fix_con_rate $cal_day
        pr_fix_acc_rate $cal_day

        pr_mob_con_rate $cal_day
        pr_mob_acc_rate $cal_day
    done
}

# 计算pr固网关联率
start_pr_fix() {
    for (( day_seconds = start_seconds; day_seconds <= end_seconds; day_seconds += 86400 )); do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        end_day_time=$(($(date -d "$cal_day 00:00:00" +"%s")*1000+86400000))
        end_hour_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi

        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        pr_fix_con_rate $cal_day
        pr_fix_acc_rate $cal_day
    done
}

# 计算pr移动网关联率
start_pr_mob() {
    for (( day_seconds = start_seconds; day_seconds <= end_seconds; day_seconds += 86400 )); do
        cal_day=$(date -d "@$day_seconds" +"%Y-%m-%d")
        end_day_time=$(($(date -d "$cal_day 00:00:00" +"%s")*1000+86400000))
        end_hour_time=$(($(date -d "$cal_day $end_hour:00:00" +"%s")*1000+3600000))
        if [ ! -d "$cal_day" ]; then
            # 若目录不存在则创建
            mkdir -p "output/$cal_day"
        fi

        echo "统计类型,统计日期,统计小时,关联成功数,关联总数,关联率" >> ./output/$cal_day/radius_connect_$cal_day.csv
        pr_mob_con_rate $cal_day
        pr_mob_acc_rate $cal_day
    done
}


# 使用 case 语句根据参数执行不同的方法
case "$1" in
    "start_all")
        start_all
        ;;
    "start_all_today")
        start_all_today
        ;;
    "start_nf")
        start_nf
        ;;
    "start_pr")
        start_pr
        ;;
    "start_nf_fix")
        start_nf_fix
        ;;
    "start_nf_mob")
        start_nf_mob
        ;;
    "start_nf_fix_today")
        start_nf_fix_today
        ;;
    "start_nf_mob_today")
        start_nf_mob_today
        ;;
    "start_nf_today")
        start_nf_today
        ;;
    "start_pr_mob")
        start_pr_mob
        ;;
    "start_pr_fix")
        start_pr_fix
        ;;
    *)
        echo "Usage: $0 [start_nf|start_pr|start_nf_fix|start_nf_mob|start_nf_fix_today|start_nf_mob_today|start_nf_today|start_pr_mob|start_pr_fix]"
        ;;
esac
