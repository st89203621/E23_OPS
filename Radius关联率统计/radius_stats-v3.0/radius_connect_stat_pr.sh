#!/bin/bash

#sparksql提交查询任务
submit_task() {
    task_name=$3
    spark_param="--master yarn --name ${task_name} --conf spark.driver.memory=2g --conf spark.driver.cores=2 --conf spark.executor.memory=12g --conf spark.executor.cores=4 --conf spark.executor.instances=16 --conf spark.default.parallelism=16"
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
source /etc/profile

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

# pr固网关联率准确率-异常数据明细
pr_fix_acc_rate_detail() {
    for uparea_id in 210213 220214 230215; do
        echo "==========start pr_fix_acc_rate_detail============"
        pr_fix_acc_rate_detail=`sed "s/{cal_day}/$1/g;s/{start_hour}/${start_hour}/g;s/{end_hour}/${end_hour}/g;s/{pr_offline_time}/${pr_offline_time}/g;s/{uparea_id}/${uparea_id}/g" ./template/pr_fix_acc_detail.sql`
        echo "$pr_fix_acc_rate_detail" > sql/pr_fix_acc_rate_detail.sql
        submit_task "$pr_fix_acc_rate_detail" "./output/$1/pr_fix_acc_rate_detail_$1.csv" "pr_fix_acc_rate_detail"
        echo "==========end pr_fix_acc_rate_detail============"
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
    "start_pr_mob")
        start_pr_mob
        ;;
    "start_pr_fix")
        start_pr_fix
        ;;
    *)
        start_pr
        ;;
esac
