#!/bin/bash

# 增强版批量导入脚本
# 1. 正确映射机房标识：210213→A1, 220214→A2, 230215→A3
# 2. 添加汇总值计算：ass × acc = 汇总值
# 3. 支持7字段和6字段CSV格式

DB_HOST="192.168.28.4"
DB_USER="root"
DB_PASS="123456"
DB_NAME="radius_stats"

echo "开始增强版批量导入所有CSV文件..."

# 首先清空现有数据
echo "清空现有数据..."
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" << SQL
DELETE FROM radius_statistics;
DELETE FROM import_log;
SQL

# 统计变量
total_files=0
success_files=0
failed_files=0
total_records=0

# 存储关联率和准确率数据用于计算汇总值
declare -A ass_data
declare -A acc_data

# 遍历所有CSV文件
find output -name "*.csv" -type f | sort | while read csv_file; do
    total_files=$((total_files + 1))
    echo "[$total_files] 正在处理: $csv_file"
    
    # 提取日期
    date_str=$(basename "$csv_file" | grep -o '[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}')
    
    if [ -z "$date_str" ]; then
        echo "  [错误] 无法从文件名提取日期: $csv_file"
        failed_files=$((failed_files + 1))
        continue
    fi
    
    file_records=0
    
    # 检测字段数量
    first_data_line=$(tail -n +2 "$csv_file" | head -n 1)
    field_count=$(echo "$first_data_line" | tr ',' '\n' | wc -l)
    
    echo "  [信息] 检测到字段数量: $field_count"
    
    if [ "$field_count" -eq 7 ]; then
        echo "  [信息] 使用7字段格式处理"
        # 7字段格式: 统计类型,统计日期,统计小时,标识字段,关联成功数,关联总数,关联率
        tail -n +2 "$csv_file" | while IFS=',' read -r stat_type stat_date stat_hour identifier success_num total_num rate; do
            # 清理数据
            stat_type=$(echo "$stat_type" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            stat_date=$(echo "$stat_date" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            stat_hour=$(echo "$stat_hour" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            identifier=$(echo "$identifier" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            success_num=$(echo "$success_num" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            total_num=$(echo "$total_num" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            rate=$(echo "$rate" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            
            # 跳过空行
            if [ -z "$stat_type" ] || [ -z "$success_num" ]; then
                continue
            fi
            
            # 映射机房标识
            case "$identifier" in
                "210213")
                    site="A1"
                    ;;
                "220214")
                    site="B1"
                    ;;
                "230215")
                    site="C1"
                    ;;
                *)
                    site="A1"  # 默认值
                    ;;
            esac
            
            # 根据统计类型映射字段
            case "$stat_type" in
                "nf固网关联率")
                    bus_type="NF"
                    radius_type="fix"
                    metric_type="ass"
                    ;;
                "nf移网关联率")
                    bus_type="NF"
                    radius_type="mobile"
                    metric_type="ass"
                    ;;
                "pr固网关联率")
                    bus_type="PR"
                    radius_type="fix"
                    metric_type="ass"
                    ;;
                "pr移网关联率")
                    bus_type="PR"
                    radius_type="mobile"
                    metric_type="ass"
                    ;;
                "nf固网关联准确率")
                    bus_type="NF"
                    radius_type="fix"
                    metric_type="acc"
                    ;;
                "nf移网关联准确率")
                    bus_type="NF"
                    radius_type="mobile"
                    metric_type="acc"
                    ;;
                "pr固网关联准确率")
                    bus_type="PR"
                    radius_type="fix"
                    metric_type="acc"
                    ;;
                "pr移网关联准确率")
                    bus_type="PR"
                    radius_type="mobile"
                    metric_type="acc"
                    ;;
                *)
                    echo "    [警告] 未知统计类型: $stat_type"
                    continue
                    ;;
            esac
            
            # 执行插入
            mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" << SQL
INSERT INTO radius_statistics
(site, bus_type, radius_type, metric_type, identifier, numerator, denominator, percentage, day, hour_range)
VALUES ('$site', '$bus_type', '$radius_type', '$metric_type', '$identifier', $success_num, $total_num, $rate, '$stat_date', '$stat_hour')
ON DUPLICATE KEY UPDATE
numerator = VALUES(numerator),
denominator = VALUES(denominator),
percentage = VALUES(percentage),
updated_time = CURRENT_TIMESTAMP;
SQL
            
            if [ $? -eq 0 ]; then
                echo "    [成功] 插入数据: $stat_type $stat_date $stat_hour (机房: $site, 标识: $identifier)"
                file_records=$((file_records + 1))
                
                # 存储数据用于汇总计算
                key="${site}_${bus_type}_${radius_type}_${stat_date}_${stat_hour}"
                if [ "$metric_type" = "ass" ]; then
                    ass_data["$key"]="$rate"
                elif [ "$metric_type" = "acc" ]; then
                    acc_data["$key"]="$rate"
                fi
            else
                echo "    [失败] 插入数据失败: $stat_type $stat_date $stat_hour (机房: $site, 标识: $identifier)"
            fi
        done
    elif [ "$field_count" -eq 6 ]; then
        echo "  [信息] 使用6字段格式处理"
        # 6字段格式: 统计类型,统计日期,统计小时,关联成功数,关联总数,关联率
        tail -n +2 "$csv_file" | while IFS=',' read -r stat_type stat_date stat_hour success_num total_num rate; do
            # 清理数据
            stat_type=$(echo "$stat_type" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            stat_date=$(echo "$stat_date" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            stat_hour=$(echo "$stat_hour" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            success_num=$(echo "$success_num" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            total_num=$(echo "$total_num" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            rate=$(echo "$rate" | tr -d '"' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            
            # 跳过空行
            if [ -z "$stat_type" ] || [ -z "$success_num" ]; then
                continue
            fi
            
            # 映射字段
            site="A1"  # 6字段格式默认使用A1
            identifier="default"  # 6字段格式使用默认标识
            
            # 根据统计类型映射字段
            case "$stat_type" in
                "nf固网关联率")
                    bus_type="NF"
                    radius_type="fix"
                    metric_type="ass"
                    ;;
                "nf移网关联率")
                    bus_type="NF"
                    radius_type="mobile"
                    metric_type="ass"
                    ;;
                "pr固网关联率")
                    bus_type="PR"
                    radius_type="fix"
                    metric_type="ass"
                    ;;
                "pr移网关联率")
                    bus_type="PR"
                    radius_type="mobile"
                    metric_type="ass"
                    ;;
                "nf固网关联准确率")
                    bus_type="NF"
                    radius_type="fix"
                    metric_type="acc"
                    ;;
                "nf移网关联准确率")
                    bus_type="NF"
                    radius_type="mobile"
                    metric_type="acc"
                    ;;
                "pr固网关联准确率")
                    bus_type="PR"
                    radius_type="fix"
                    metric_type="acc"
                    ;;
                "pr移网关联准确率")
                    bus_type="PR"
                    radius_type="mobile"
                    metric_type="acc"
                    ;;
                *)
                    echo "    [警告] 未知统计类型: $stat_type"
                    continue
                    ;;
            esac
            
            # 执行插入
            mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" << SQL
INSERT INTO radius_statistics
(site, bus_type, radius_type, metric_type, identifier, numerator, denominator, percentage, day, hour_range)
VALUES ('$site', '$bus_type', '$radius_type', '$metric_type', '$identifier', $success_num, $total_num, $rate, '$stat_date', '$stat_hour')
ON DUPLICATE KEY UPDATE
numerator = VALUES(numerator),
denominator = VALUES(denominator),
percentage = VALUES(percentage),
updated_time = CURRENT_TIMESTAMP;
SQL
            
            if [ $? -eq 0 ]; then
                echo "    [成功] 插入数据: $stat_type $stat_date $stat_hour (机房: $site)"
                file_records=$((file_records + 1))
            else
                echo "    [失败] 插入数据失败: $stat_type $stat_date $stat_hour (机房: $site)"
            fi
        done
    else
        echo "  [错误] 不支持的字段数量: $field_count"
        failed_files=$((failed_files + 1))
        continue
    fi
    
    # 记录导入日志
    mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" << SQL
INSERT INTO import_log
(file_name, file_path, import_date, total_records, success_records, failed_records, status, error_message)
VALUES ('$(basename "$csv_file")', '$csv_file', '$date_str', $file_records, $file_records, 0, 'SUCCESS', NULL)
ON DUPLICATE KEY UPDATE
total_records = VALUES(total_records),
success_records = VALUES(success_records),
import_time = CURRENT_TIMESTAMP;
SQL
    
    echo "  [完成] 文件处理完成: $csv_file (导入 $file_records 条记录)"
    success_files=$((success_files + 1))
    total_records=$((total_records + file_records))
done

echo ""
echo "开始计算汇总值..."

# 计算汇总值 (ass × acc)
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" << 'SQL'
INSERT INTO radius_statistics 
(site, bus_type, radius_type, metric_type, identifier, numerator, denominator, percentage, day, hour_range)
SELECT 
    a.site,
    a.bus_type,
    a.radius_type,
    'sum' as metric_type,
    a.identifier,
    ROUND(a.numerator * c.percentage) as numerator,
    a.denominator,
    (a.percentage * c.percentage) as percentage,
    a.day,
    a.hour_range
FROM radius_statistics a
JOIN radius_statistics c ON (
    a.site = c.site AND 
    a.bus_type = c.bus_type AND 
    a.radius_type = c.radius_type AND 
    a.identifier = c.identifier AND 
    a.day = c.day AND 
    a.hour_range = c.hour_range AND
    a.metric_type = 'ass' AND 
    c.metric_type = 'acc'
)
ON DUPLICATE KEY UPDATE
numerator = VALUES(numerator),
denominator = VALUES(denominator),
percentage = VALUES(percentage),
updated_time = CURRENT_TIMESTAMP;
SQL

summary_records=$(mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -se "SELECT COUNT(*) FROM radius_statistics WHERE metric_type = 'sum';")

echo "增强版批量导入完成!"
echo "总文件数: $total_files"
echo "成功: $success_files"
echo "失败: $failed_files"
echo "总记录数: $total_records"
echo "汇总记录数: $summary_records"
echo ""
echo "数据统计："
mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "
SELECT 
    metric_type as '指标类型',
    COUNT(*) as '记录数'
FROM radius_statistics 
GROUP BY metric_type 
ORDER BY 
    CASE metric_type 
        WHEN 'ass' THEN 1 
        WHEN 'acc' THEN 2 
        WHEN 'sum' THEN 3 
    END;
"
