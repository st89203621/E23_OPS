# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import time
import os
import pymysql
from pymysql import MySQLError
import xlrd
import xlwt
import allure
import pandas as pd
import json
import datetime
from collections import defaultdict

def date_handler(obj):
    if isinstance(obj, datetime.date):
        return obj.isoformat()  # 将日期对象转换为 ISO 格式字符串
    raise TypeError("Type not serializable")
def mysql_connect(mysql_host,mysql_port, mysql_user, mysql_password, mysql_database):
    try:
        connection = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            db=mysql_database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        return connection
    except MySQLError as e:
        print(e)
        return None

def make_mysql_connet():
    connection = mysql_connect('192.168.15.51',9030,'root','123456','dwd')
    return connection
def mysql_query(connection,sql):
    allure.attach(sql, name="执行的SQL", attachment_type=allure.attachment_type.TEXT)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor: # 使用字典游标
            cursor.execute(sql)
            result = cursor.fetchall()
            # 关闭游标
            cursor.close()
            json_data = result or []  # 确保始终是列表类型
            json_str = json.dumps(json_data, indent=2, ensure_ascii=False,default=date_handler)
            allure.attach(json_str, name="数据库查询结果", attachment_type=allure.attachment_type.JSON)
            return result # 转换为列表字典格式
    except MySQLError as e:
        print(e)
        return None

account = """
    SELECT account FROM ads_ops.ops_api_access_result 
WHERE stat_date = '2025-07-30'
and metric_id = '5'
and system_name = '全息档案'
AND account IN (
    SELECT t1.account
    FROM ads_ops.ops_api_access_result t1
    JOIN ads_ops.ops_api_access_result t2
    ON t1.account = t2.account 
    AND t1.stat_date = t2.stat_date
    WHERE t1.stat_date = '2025-07-30'
    AND t1.system_name = '全息档案' 
    AND t2.system_name = '综合搜索'
    AND t1.metric_id = '5'
    AND t2.metric_id = '5'
    AND t1.stat_count <> t2.stat_count
) order by account,metric_id;
"""

archive_16 = """
SELECT stat_date,metric_id ,account,system_name ,stat_count,kv_content FROM ads_ops.ops_api_access_result 
WHERE stat_date = '2025-07-30'
and metric_id = '5'
and system_name = '全息档案'
AND account IN (
    SELECT t1.account
    FROM ads_ops.ops_api_access_result t1
    JOIN ads_ops.ops_api_access_result t2
    ON t1.account = t2.account 
    AND t1.stat_date = t2.stat_date
    WHERE t1.stat_date = '2025-07-30'
    AND t1.system_name = '全息档案' 
    AND t2.system_name = '综合搜索'
    AND t1.metric_id = '5'
    AND t2.metric_id = '5'
    AND t1.stat_count <> t2.stat_count
) order by account,metric_id;
"""

search_16 = """
    SELECT stat_date,metric_id ,account,system_name ,stat_count,kv_content FROM ads_ops.ops_api_access_result 
WHERE stat_date = '2025-07-30'
and metric_id = '5'
and system_name = '综合搜索'
AND account IN (
    SELECT t1.account
    FROM ads_ops.ops_api_access_result t1
    JOIN ads_ops.ops_api_access_result t2
    ON t1.account = t2.account 
    AND t1.stat_date = t2.stat_date
    WHERE t1.stat_date = '2025-07-30'
    AND t1.system_name = '全息档案' 
    AND t2.system_name = '综合搜索'
    AND t1.metric_id = '5'
    AND t2.metric_id = '5'
    AND t1.stat_count <> t2.stat_count
) order by account,metric_id;
"""

mysql_cnnet = make_mysql_connet()

account_list = mysql_query(mysql_cnnet,account)
archive_value = mysql_query(mysql_cnnet,archive_16)
search_value = mysql_query(mysql_cnnet,search_16)

mysql_cnnet.close()

# print(f"account_list：{account_list}")
# print(f"archive_value:{archive_value}")
# print(f"search_value:{search_value}")

comparison_data = defaultdict(list)

datatype_mapping = {'HTTP': 100,
            'IM': 103,
            'Email': 101,
            'VPN': 121,
            'SNS': 119,
            'Travel': 2001,
            'Tool': 2004,
            'Terminal': 142,
            'RemoteCTRL': 113,
            'AppsLocation': 146,
            'FTP': 105,
            'Telnet': 108,
            'Engine': 125,
            'Multimedia': 2002,
            'News': 138,
            'Entertainment': 2003,
            'Shopping': 122,
            'Finance': 963,
            'Other':999
            }

# 处理archive数据
for item in archive_value:
    try:
        account = item['account']
        kv_data = json.loads(item['kv_content'].replace("'", '"'))
        for kv in kv_data:
            # 获取映射表中的值，如果没有找到，则使用原始值
            data_type_mapped = datatype_mapping.get(kv['dataTypeName'], kv['dataTypeName'])
            comparison_data[account].append({
                'type': 'archive',
                'dataType': data_type_mapped,  #使用映射后的值
                'value': kv['behaviorNum']
            })
    except Exception as e:
        print(f"解析archive数据错误: {e}")

# 处理search数据
for item in search_value:
    try:
        account = item['account']
        kv_data = json.loads(item['kv_content'].replace("'", '"'))
        for kv in kv_data:
            comparison_data[account].append({
                'type': 'search',
                'key': str(kv['key']),  # 统一为字符串类型
                'value': kv['doc_count']
            })
    except Exception as e:
        print(f"解析search数据错误: {e}")

# 执行比对分析
results = {}
for account, entries in comparison_data.items():
    archive_map = {str(item['dataType']): item['value'] 
                   for item in entries if item['type'] == 'archive'}
    search_map = {item['key']: item['value'] 
                 for item in entries if item['type'] == 'search'}

    all_keys = set(archive_map.keys()).union(set(search_map.keys()))

    diffs = []
    for data_type in all_keys:
        a_value = archive_map.get(data_type, 0)  # 如果不存在则为 None
        s_value = search_map.get(data_type, 0)   # 如果不存在则为 None
        
        # 检查两个值是否为数字并计算差异
        if isinstance(a_value, (int, float)) and isinstance(s_value, (int, float)):
            diff = a_value - s_value
        else:
            diff = None  # 或者其他适当的处理非数字的情况
        
        diffs.append({
            'dataType': data_type,
            'archive_value': a_value,
            'search_value': s_value,
            'difference': diff
        })

    if diffs:
        results[account] = {
            'total_differences': len(diffs),
            'details': diffs,
            'total_diff_sum': sum(d['difference'] for d in diffs)
        }


# 输出结果
with open('metric5_output.txt', 'w') as f:
    for account, result in results.items():
        f.write(f"\n\n account {account} comparison result: \n")
        # f.write(f"差异总数: {result['total_differences']}\n")
        # f.write(f"累计差值: {result['total_diff_sum']}")
        for detail in result['details']:
            f.write(f"\ndatatype {detail['dataType']}: "
                f"Archive({detail['archive_value']}) vs "
                f"Search({detail['search_value']}) | "
                f"diff: {detail['difference']}")