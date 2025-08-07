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
OUTPUT_FILENAME = "用户流量统计结果.xlsx"

# 日志文件配置
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")

# API配置
API_PORT = 9999
API_ENDPOINT = "/v1/status/user-rank"  # 获取用户流速排行
API_METHOD = "POST"
API_HEADERS = {
    "Content-Type": "application/json",
    "Accept-Language": "zh-CN"  # 支持中文响应
}
API_PAYLOAD = {
    "filter": {
        "top": 50,
        "line": "0"
    }
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
DB_TABLE = "nf_top_ip_statistics_base"

# 确保所有必要目录存在
for directory in [OUTPUT_DIR, LOGS_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)
