#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置文件
用于配置输入文件路径、输出目录等参数
"""

import os

# 输入文件配置
INPUT_FILE_PATH = os.path.join(os.path.dirname(__file__), "input", "IAM配置.xlsx")

# 输出文件配置
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
OUTPUT_FILENAME_TEMPLATE = "NF系统流速Top{top_n}用户统计{timestamp}.xlsx"  # 新的文件命名格式

# 日志文件配置
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")

# API配置
API_PORT = 9999

# 用户级别流速API配置
USER_API_ENDPOINT = "/v1/status/user-rank"  # 获取用户流速排行
USER_API_METHOD = "POST"
USER_API_HEADERS = {
    "Content-Type": "application/json",
    "Accept-Language": "zh-CN"  # 支持中文响应
}
USER_API_PAYLOAD = {
    "filter": {
        "top": 50,
        "line": "0"
    }
}

# 设备级别流速API配置
DEVICE_API_ENDPOINT = "/v1/status/throughput"  # 获取设备吞吐量
DEVICE_API_METHOD = "POST"
DEVICE_API_HEADERS = {
    "Content-Type": "application/json",
    "Accept-Language": "zh-CN"  # 支持中文响应
}
DEVICE_API_PAYLOAD = {
    "unit": "bytes",  # 流速单位
    "interface": ""   # 空表示所有WAN口
}

# API认证配置
# 注意：请根据实际环境修改共享密钥
SHARED_SECRET = "1"  # 共享密钥，请修改为实际值
RANDOM_LENGTH = 16  # random字符串长度

# 请求配置
REQUEST_TIMEOUT = 30  # 请求超时时间（秒）
MAX_RETRIES = 3      # 最大重试次数

# 数据处理配置
TOP_N_USERS_PER_DEVICE = 50  # 每台设备输出的用户数量（可配置）
INCLUDE_BANDWIDTH_INFO = True  # 是否包含带宽信息
GROUP_BY_STATION = True       # 是否按局点分组到不同sheet

# 流速单位配置
FLOW_RATE_UNIT = "B/s"   # API返回的流速单位，默认为bytes/s
OUTPUT_UNIT = "Mb/s"     # 输出显示单位：Mb/s（兆比特每秒）
CONVERT_TO_MBPS = True   # 是否转换为Mb/s显示

# 日志配置
LOG_LEVEL = "INFO"   # 日志级别：DEBUG, INFO, WARNING, ERROR
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# 输出配置
OUTPUT_TO_EXCEL = True   # 是否输出到Excel文件
OUTPUT_TO_DATABASE = True  # 是否输出到数据库

# Excel配置
EXCEL_SHEET_NAME = "用户流速统计"

# 数据库配置
DB_HOST = "192.168.13.4"
DB_USER = "root"
DB_PASSWORD = "123456"
DB_NAME = "packets_statistics"
DB_USER_TABLE = "nf_user_flow_statistics_v2"  # 用户级别数据表（新表）
DB_DEVICE_TABLE = "nf_device_flow_statistics"  # 设备级别数据表

# 机房名称到代号的映射
MACHINE_ROOM_MAPPING = {
    'Benaknoun': 'A2',
    'Djamila': 'A3',
    'Annaba': 'B1',
    'Oran': 'C1'
}

# 确保所有必要目录存在
for directory in [OUTPUT_DIR, LOGS_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)
