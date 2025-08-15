#!/bin/bash

# 批量数据盘切换脚本
# 目标服务器列表（排除192.168.54.4和192.168.54.12）
servers=(
    "192.168.94.1" "192.168.94.2" "192.168.94.3" "192.168.94.4" "192.168.94.5" "192.168.94.6"
    "192.168.94.7" "192.168.94.8" "192.168.94.9" "192.168.94.10" "192.168.94.11"
    "192.168.94.12"
)

# 对每台服务器执行数据盘切换
for server in "${servers[@]}"; do
    echo "=== 开始处理 $server ==="
    
    # SSH连接并执行完整切换流程
    ssh root@$server << 'EOF'
        echo "=== 磁盘格式化和挂载 ==="
        mkfs.xfs -f /dev/mapper/mpatha-part1
        mkdir -p /data2
        mount /dev/mapper/mpatha-part1 /data2
        
        echo "=== 配置fstab ==="
        UUID=$(blkid /dev/mapper/mpatha-part1 | awk -F'"' '{print $2}')
        echo "UUID=\"$UUID\" /data2/ xfs defaults,nofail,_netdev,x-systemd.device-timeout=5 0 2" >> /etc/fstab
        
        echo "=== 停止服务 ==="
        /data1/daq/deye-pr-collect-task-1.4.3-SNAPSHOT/bin/semptian-task.sh stop
        /data1/daq/deye-pr-collect-master-1.4.3-SNAPSHOT/bin/semptian-task.sh stop
        
        echo "=== 迁移MQ数据 ==="
        cp -r /data1/apache-activemq-5.7.0 /data2/
        
        echo "=== 创建目录结构 ==="
        mkdir -p /data2/pr-collect/upload
        
        echo "=== 更新配置文件 ==="
        sed -i 's/\/data1\/pr-collect/\/data2\/pr-collect/g' /data1/daq/deye-pr-collect-task-1.4.3-SNAPSHOT/conf/task.properties
        sed -i 's/\/data1\/pr-collect/\/data2\/pr-collect/g' /data1/daq/deye-pr-collect-task-1.4.3-SNAPSHOT/conf/semptian/semptian-application.properties
        
        echo "=== 启动服务 ==="
        /data1/daq/deye-pr-collect-task-1.4.3-SNAPSHOT/bin/semptian-task.sh start
        /data1/daq/deye-dataprotocol-file-compress-v6.2.3.6/bin/start.sh
        /data1/daq/deye-dataprotocol-file-ferry-v6.2.3.4/bin/start.sh
        /data1/daq/deye-pr-collect-master-1.4.3-SNAPSHOT/bin/semptian-task.sh start
        
        echo "=== $HOSTNAME 切换完成 ==="
EOF
    
    # 更新MySQL路径
    echo "=== 更新 $server 的MySQL路径 ==="
    ssh root@192.168.54.1 "mysql -u root -p123456 daq -e \"update t_scan_info set history_zip_path = replace(history_zip_path,'/data1/pr-collect','/data2/pr-collect'),mq_status_error_zip_path = replace(mq_status_error_zip_path,'/data1/pr-collect','/data2/pr-collect'),fs_prop = replace(fs_prop,'/data1/pr-collect','/data2/pr-collect') where exec_ip = '$server';\""
    
    echo "=== $server 完整切换完成 ==="
    echo ""
done

echo "=== 所有服务器数据盘切换完成 ==="
