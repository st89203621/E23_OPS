1.修改配置query.conf;
start_day=2025-04-29
end_day=2025-04-29

start_hour=0
end_hour=8

start_day和end_day可以跨多天，按天统计start_hour-end_hour（包含end_hour）这个时间段；
2.输出文件在output目录下，按数据统计日期分割目录；
3.sql目录下记录实际执行sql，方便排查问题；
4.常用nf关联率（包含准确率）命令：
统计今天的固网和移动网关联率：sh radius_connect_stat_nf.sh start_nf_today
统计今天的固网关联率：sh radius_connect_stat_nf.sh start_nf_fix_today
统计今天的移动网关联率：sh radius_connect_stat_nf.sh start_nf_mob_today

统计非今天的固网和移动网关联率：sh radius_connect_stat_nf.sh start_nf
统计非今天的固网关联率：sh radius_connect_stat_nf.sh start_nf_fix
统计非今天的移动网关联率：sh radius_connect_stat_nf.sh start_nf_mob