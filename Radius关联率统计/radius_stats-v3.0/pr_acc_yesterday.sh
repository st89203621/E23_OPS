#!/bin/bash
# PR昨天关联准确度统计
source /etc/profile
yesterday=$(date -d "yesterday" +"%Y-%m-%d")
mkdir -p ./output_new/$yesterday

# PR移网关联准确度
for uparea_id in 210213 220214 230215; do
    spark-sql --master yarn --name "pr_mob_acc_${yesterday}_${uparea_id}" \
    --conf spark.driver.memory=8g \
    --conf spark.driver.cores=4 \
    --conf spark.executor.memory=24g \
    --conf spark.executor.cores=4 \
    --conf spark.executor.instances=32 \
    --conf spark.default.parallelism=64 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=268435456 \
    --conf spark.sql.files.openCostInBytes=8388608 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer -e "
    SELECT 'pr移网关联准确率' AS cal_type, '$yesterday' AS cal_day, '00-23' AS cal_hour, '$uparea_id' AS uparea_id,
           SUM(if(calling_station_id = auth_account,1,0)) AS connect_account, SUM(1) AS total_account,
           SUM(if(calling_station_id = auth_account,1,0)) / SUM(1) AS acc_rate
    FROM (
        SELECT pr.strsrc_ip, pr.auth_account, pr.capture_time, radius.calling_station_id,
               DENSE_RANK() OVER (PARTITION BY pr.strsrc_ip,pr.auth_account,pr.capture_time ORDER BY pr.capture_time - radius.capture_time) AS rn
        FROM (
            SELECT strsrc_ip, auth_account, src_port, capture_time FROM v64_deye_dw_ods.ods_pr_http_store
            WHERE capture_day = '$yesterday' AND capture_hour BETWEEN '00' AND '23' AND auth_account LIKE '213%' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, src_port, capture_time FROM v64_deye_dw_ods.ods_pr_im_store
            WHERE capture_day = '$yesterday' AND capture_hour BETWEEN '00' AND '23' AND auth_account LIKE '213%' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, src_port, capture_time FROM v64_deye_dw_ods.ods_pr_mail_store
            WHERE capture_day = '$yesterday' AND capture_hour BETWEEN '00' AND '23' AND auth_account LIKE '213%' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, src_port, capture_time FROM v64_deye_dw_ods.ods_pr_ftp_store
            WHERE capture_day = '$yesterday' AND capture_hour BETWEEN '00' AND '23' AND auth_account LIKE '213%' AND uparea_id = '$uparea_id'
        ) pr
        JOIN (
            SELECT internet_ip, calling_station_id, cast(split(port_range,'-')[0] AS int) AS start_port,
                   cast(split(port_range,'-')[1] AS int) AS end_port, capture_time
            FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
            WHERE capture_day = '$yesterday' AND action = 'Start'
            GROUP BY internet_ip,calling_station_id,port_range,capture_time
        ) radius ON pr.strsrc_ip = radius.internet_ip AND pr.src_port >= start_port AND pr.src_port <= end_port
        WHERE pr.capture_time >= radius.capture_time
    ) tmp WHERE rn = 1;" >> ./output_new/$yesterday/pr_acc_${yesterday}_${uparea_id}.csv &
done

# PR固网关联准确度
for uparea_id in 210213 220214 230215; do
    spark-sql --master yarn --name "pr_fix_acc_${yesterday}_${uparea_id}" \
    --conf spark.driver.memory=8g \
    --conf spark.driver.cores=4 \
    --conf spark.executor.memory=24g \
    --conf spark.executor.cores=4 \
    --conf spark.executor.instances=32 \
    --conf spark.default.parallelism=64 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=268435456 \
    --conf spark.sql.files.openCostInBytes=8388608 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer -e "
    SELECT 'pr固网关联准确率' AS cal_type, '$yesterday' AS cal_day, '00-23' AS cal_hour, '$uparea_id' AS uparea_id,
           SUM(IF(auth_account = account,1,0)) AS connect_account, SUM(1) AS total_account,
           SUM(IF(auth_account = account,1,0)) / SUM(1) AS acc_rate
    FROM (
        SELECT pr.strsrc_ip, pr.auth_account, pr.capture_time, radius.account,
               DENSE_RANK() OVER (PARTITION BY pr.strsrc_ip,pr.auth_account,pr.capture_time ORDER BY pr.capture_time - radius.capture_time) AS rn
        FROM (
            SELECT strsrc_ip, auth_account, capture_time FROM v64_deye_dw_ods.ods_pr_http_store
            WHERE capture_day = '$yesterday' AND capture_hour BETWEEN '00' AND '23' AND auth_account is not null AND auth_account <> '' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, capture_time FROM v64_deye_dw_ods.ods_pr_im_store
            WHERE capture_day = '$yesterday' AND capture_hour BETWEEN '00' AND '23' AND auth_account is not null AND auth_account <> '' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, capture_time FROM v64_deye_dw_ods.ods_pr_mail_store
            WHERE capture_day = '$yesterday' AND capture_hour BETWEEN '00' AND '23' AND auth_account is not null AND auth_account <> '' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, capture_time FROM v64_deye_dw_ods.ods_pr_ftp_store
            WHERE capture_day = '$yesterday' AND capture_hour BETWEEN '00' AND '23' AND auth_account is not null AND auth_account <> '' AND uparea_id = '$uparea_id'
        ) pr
        JOIN (
            SELECT ip, account, capture_time FROM v64_deye_dw_ods.ods_fixnet_radius_store
            WHERE capture_day = '$yesterday' AND ACTION IN ('login', 'logout')
        ) radius ON pr.strsrc_ip = radius.ip
        WHERE pr.capture_time >= radius.capture_time
    ) tmp WHERE rn = 1;" >> ./output_new/$yesterday/pr_fix_acc_${yesterday}_${uparea_id}.csv &
done

wait
echo "PR昨天关联准确度统计完成: $yesterday"
