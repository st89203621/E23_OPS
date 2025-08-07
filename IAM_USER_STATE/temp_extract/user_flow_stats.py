#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户流速统计脚本
从IAM配置文件中读取IP地址，调用API获取用户流速数据，
按设备分组处理后输出到Excel文件
注意：API返回的是流速数据(B/s)，不是流量数据
"""

import os
import sys
import json
import logging
import hashlib
import random
import string
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

# 尝试导入依赖包，如果失败则给出提示
try:
    import requests
except ImportError:
    print("❌ 缺少 requests 包，请运行: pip install requests")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("❌ 缺少 pandas 包，请运行: pip install pandas")
    sys.exit(1)

try:
    import openpyxl
except ImportError:
    print("❌ 缺少 openpyxl 包，请运行: pip install openpyxl")
    sys.exit(1)

# 数据库相关导入
try:
    import pymysql
    DB_AVAILABLE = True
except ImportError:
    print("⚠️  警告: pymysql未安装，数据库功能不可用。安装命令: pip install pymysql")
    DB_AVAILABLE = False

import config


class UserFlowStatsProcessor:
    """用户流速统计处理器"""
    
    def __init__(self):
        """初始化处理器"""
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        self.all_user_data = []
        self.station_names = {}  # 存储局点名称映射
        self.used_randoms = set()  # 存储已使用的random值，防止重复
        
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=getattr(logging, config.LOG_LEVEL),
            format=config.LOG_FORMAT,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(
                    os.path.join(config.LOGS_DIR, f"user_flow_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
                    encoding='utf-8'
                )
            ]
        )

    def generate_random_string(self) -> str:
        """
        生成唯一的随机字符串用于API认证

        Returns:
            随机字符串
        """
        max_attempts = 100  # 最大尝试次数，防止无限循环

        for _ in range(max_attempts):
            # 生成随机字符串：时间戳 + 随机字符
            timestamp = str(int(time.time() * 1000))  # 毫秒时间戳
            random_chars = ''.join(random.choices(
                string.ascii_letters + string.digits,
                k=config.RANDOM_LENGTH - len(timestamp)
            ))
            random_str = timestamp + random_chars

            # 确保不重复
            if random_str not in self.used_randoms:
                self.used_randoms.add(random_str)
                self.logger.debug(f"生成random字符串: {random_str}")
                return random_str

        # 如果尝试多次仍然重复，使用UUID确保唯一性
        import uuid
        random_str = str(uuid.uuid4()).replace('-', '')[:config.RANDOM_LENGTH]
        self.used_randoms.add(random_str)
        self.logger.debug(f"使用UUID生成random字符串: {random_str}")
        return random_str

    def calculate_md5(self, shared_secret: str, random_str: str) -> str:
        """
        计算MD5认证值

        Args:
            shared_secret: 共享密钥
            random_str: 随机字符串

        Returns:
            MD5值
        """
        # 按照规则拼接：共享密钥 + random
        combined_str = shared_secret + random_str

        # 计算MD5
        md5_hash = hashlib.md5(combined_str.encode('utf-8')).hexdigest()

        self.logger.debug(f"MD5计算: '{shared_secret}' + '{random_str}' = '{combined_str}' -> {md5_hash}")
        return md5_hash

    def get_auth_params(self) -> Dict[str, str]:
        """
        获取API认证参数

        Returns:
            包含random和md5的认证参数字典
        """
        random_str = self.generate_random_string()
        md5_value = self.calculate_md5(config.SHARED_SECRET, random_str)

        auth_params = {
            "random": random_str,
            "md5": md5_value
        }

        self.logger.debug(f"生成认证参数: {auth_params}")
        return auth_params

    def convert_bytes_to_mbps(self, bytes_per_second: float) -> float:
        """
        将B/s转换为Mb/s（兆比特每秒）

        Args:
            bytes_per_second: 字节每秒的流速值

        Returns:
            Mb/s流速值
        """
        try:
            # B/s -> Mb/s 转换公式：
            # 1 Byte = 8 bits
            # 1 Mb = 1,000,000 bits (使用十进制，网络带宽标准)
            mbps = (bytes_per_second * 8) / 1000000
            return round(mbps, 3)
        except Exception as e:
            self.logger.warning(f"流速单位转换失败: {str(e)}")
            return 0.0

    def save_to_database(self, data: List[Dict[str, Any]]) -> bool:
        """
        将数据保存到MySQL数据库

        Args:
            data: 要保存的数据列表

        Returns:
            是否保存成功
        """
        if not DB_AVAILABLE:
            self.logger.warning("数据库功能不可用，跳过数据库保存")
            return False

        try:
            self.logger.info("开始连接数据库...")

            # 连接数据库
            connection = pymysql.connect(
                host=config.DB_HOST,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                database=config.DB_NAME,
                charset='utf8mb4',
                autocommit=True
            )

            cursor = connection.cursor()

            # 创建表（如果不存在）
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {config.DB_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                device_type VARCHAR(10) NOT NULL COMMENT '设备类型',
                station_name VARCHAR(100) NOT NULL COMMENT '机房',
                device_ip VARCHAR(45) NOT NULL COMMENT '设备IP',
                user_name VARCHAR(100) NOT NULL COMMENT '用户名',
                user_ip VARCHAR(45) NOT NULL COMMENT 'IP',
                up_flow_rate DECIMAL(12,3) NOT NULL DEFAULT 0 COMMENT '上行流速Mb每s',
                down_flow_rate DECIMAL(12,3) NOT NULL DEFAULT 0 COMMENT '下行流速Mb每s',
                total_flow_rate DECIMAL(12,3) NOT NULL DEFAULT 0 COMMENT '总流速Mb每s',
                session_count INT NOT NULL DEFAULT 0 COMMENT '会话数',
                record_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
                INDEX idx_device_ip (device_ip),
                INDEX idx_user_ip (user_ip),
                INDEX idx_station_name (station_name),
                INDEX idx_record_time (record_time),
                INDEX idx_total_flow_rate (total_flow_rate)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='NF设备Top用户流速统计'
            """

            cursor.execute(create_table_sql)
            self.logger.info(f"数据库表 {config.DB_TABLE} 准备完成")

            # 清空当前数据（可选，避免重复数据）
            cursor.execute(f"DELETE FROM {config.DB_TABLE} WHERE DATE(record_time) = CURDATE()")
            self.logger.info("已清空今日数据，准备插入新数据")

            # 插入数据
            insert_sql = f"""
            INSERT INTO {config.DB_TABLE}
            (device_type, station_name, device_ip, user_name, user_ip,
             up_flow_rate, down_flow_rate, total_flow_rate, session_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            insert_count = 0
            for record in data:
                try:
                    values = (
                        record.get('device_type', 'Unknown'),
                        record.get('machine_room', 'Unknown'),  # 使用机房而不是局点
                        record.get('source_ip', ''),
                        record.get('name', 'Unknown'),
                        record.get('ip', ''),
                        record.get('up_mbps', 0),
                        record.get('down_mbps', 0),
                        record.get('total_mbps', 0),
                        record.get('session', 0)
                    )

                    cursor.execute(insert_sql, values)
                    insert_count += 1

                except Exception as e:
                    self.logger.warning(f"插入记录失败: {str(e)}")
                    continue

            connection.close()

            self.logger.info(f"成功保存 {insert_count} 条记录到数据库")
            return True

        except Exception as e:
            self.logger.error(f"数据库保存失败: {str(e)}")
            return False

    def convert_flow_rate_unit(self, bytes_per_second: float) -> Dict[str, Any]:
        """
        转换流速单位从B/s到更易读的单位

        Args:
            bytes_per_second: 字节每秒的流速值

        Returns:
            包含原始值和转换后值的字典
        """
        try:
            # 转换为Mbps (兆比特每秒)
            mbps = (bytes_per_second * 8) / (1024 * 1024)  # bytes -> bits -> Mbps

            # 转换为MB/s (兆字节每秒)
            mbps_bytes = bytes_per_second / (1024 * 1024)  # bytes -> MB/s

            return {
                'bytes_per_second': bytes_per_second,
                'mbps': round(mbps, 3),           # Mbps (兆比特每秒)
                'mb_per_second': round(mbps_bytes, 3)  # MB/s (兆字节每秒)
            }
        except Exception as e:
            self.logger.warning(f"流速单位转换失败: {str(e)}")
            return {
                'bytes_per_second': bytes_per_second,
                'mbps': 0,
                'mb_per_second': 0
            }
    
    def read_excel_config(self, file_path: str) -> List[Dict[str, str]]:
        """
        读取Excel配置文件，提取局点名称、IP地址和带宽信息

        Args:
            file_path: Excel文件路径

        Returns:
            包含局点名称、IP地址和带宽信息的字典列表
        """
        try:
            self.logger.info(f"开始读取配置文件: {file_path}")

            # 读取Excel文件
            df = pd.read_excel(file_path, engine='openpyxl')

            # 检查是否有足够的列
            if df.shape[1] < 2:
                raise ValueError("Excel文件至少需要包含2列数据（局点名称和IP地址）")

            # 提取列数据（按新的列顺序）
            device_type_column = df.iloc[:, 0] if df.shape[1] > 0 else None    # 第一列：设备类型
            station_column = df.iloc[:, 1] if df.shape[1] > 1 else None        # 第二列：局点名称
            ip_column = df.iloc[:, 2] if df.shape[1] > 2 else None             # 第三列：设备IP
            machine_room_column = df.iloc[:, 3] if df.shape[1] > 3 else None   # 第四列：机房

            config_data = []

            # 确定数据行数
            max_rows = max(len(device_type_column) if device_type_column is not None else 0,
                          len(station_column) if station_column is not None else 0,
                          len(ip_column) if ip_column is not None else 0,
                          len(machine_room_column) if machine_room_column is not None else 0)

            for i in range(max_rows):
                # 获取各列数据
                device_type = device_type_column.iloc[i] if device_type_column is not None and i < len(device_type_column) else None
                station = station_column.iloc[i] if station_column is not None and i < len(station_column) else None
                ip = ip_column.iloc[i] if ip_column is not None and i < len(ip_column) else None
                machine_room = machine_room_column.iloc[i] if machine_room_column is not None and i < len(machine_room_column) else None

                # 跳过空值
                if pd.isna(station) or pd.isna(ip):
                    continue

                station_str = str(station).strip()
                ip_str = str(ip).strip()

                if station_str and ip_str:
                    device_config = {
                        'station_name': station_str,
                        'ip_address': ip_str
                    }

                    # 添加设备类型信息
                    if device_type is not None and not pd.isna(device_type):
                        device_config['device_type'] = str(device_type).strip()
                    else:
                        device_config['device_type'] = 'Unknown'

                    # 添加机房信息
                    if machine_room is not None and not pd.isna(machine_room):
                        device_config['machine_room'] = str(machine_room).strip()
                    else:
                        device_config['machine_room'] = 'Unknown'

                    config_data.append(device_config)

                    # 存储IP到设备信息的映射
                    self.station_names[ip_str] = station_str

            self.logger.info(f"成功读取 {len(config_data)} 条配置记录")
            return config_data
            
        except Exception as e:
            self.logger.error(f"读取Excel配置文件失败: {str(e)}")
            raise
    
    def call_api(self, ip_address: str) -> Optional[List[Dict[str, Any]]]:
        """
        调用API获取用户流量数据

        Args:
            ip_address: 设备IP地址

        Returns:
            用户数据列表，失败时返回None
        """
        for attempt in range(config.MAX_RETRIES):
            try:
                self.logger.debug(f"向 {ip_address} 发送API请求 (尝试 {attempt + 1}/{config.MAX_RETRIES})")

                # 获取认证参数
                auth_params = self.get_auth_params()

                # 构建URL，将认证参数放在查询参数中
                url = f"http://{ip_address}:{config.API_PORT}{config.API_ENDPOINT}?_method=GET&random={auth_params['random']}&md5={auth_params['md5']}"

                # 构建请求体，只包含业务参数
                request_payload = config.API_PAYLOAD

                self.logger.debug(f"请求URL: {url}")
                self.logger.debug(f"请求体: {json.dumps(request_payload, ensure_ascii=False)}")

                response = requests.post(
                    url,
                    headers=config.API_HEADERS,
                    json=request_payload,
                    timeout=config.REQUEST_TIMEOUT
                )
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if 'data' in data and isinstance(data['data'], list):
                            user_data = data['data']
                            self.logger.info(f"从 {ip_address} 获取到 {len(user_data)} 条用户数据")

                            # 为每条数据添加来源信息并转换流速单位
                            for user in user_data:
                                user['source_ip'] = ip_address
                                user['station_name'] = self.station_names.get(ip_address, ip_address)

                                # 转换流速单位从B/s到Mb/s
                                if config.CONVERT_TO_MBPS:
                                    if 'up' in user:
                                        user['up_mbps'] = self.convert_bytes_to_mbps(float(user.get('up', 0)))
                                    if 'down' in user:
                                        user['down_mbps'] = self.convert_bytes_to_mbps(float(user.get('down', 0)))
                                    if 'total' in user:
                                        user['total_mbps'] = self.convert_bytes_to_mbps(float(user.get('total', 0)))

                            return user_data
                        else:
                            self.logger.warning(f"API响应格式异常，IP: {ip_address}, 响应: {data}")
                            return None
                    except json.JSONDecodeError:
                        self.logger.warning(f"API响应JSON解析失败，IP: {ip_address}")
                        return None
                elif response.status_code == 401:
                    self.logger.error(f"API认证失败，IP: {ip_address}, 请检查共享密钥配置")
                    return None
                elif response.status_code == 403:
                    self.logger.error(f"API访问被拒绝，IP: {ip_address}, 可能是认证参数错误")
                    return None
                else:
                    self.logger.warning(f"API请求失败，IP: {ip_address}, 状态码: {response.status_code}")
                    try:
                        error_data = response.json()
                        self.logger.warning(f"错误详情: {error_data}")
                    except:
                        self.logger.warning(f"响应内容: {response.text[:200]}")
                    
            except requests.exceptions.Timeout:
                self.logger.warning(f"API请求超时，IP: {ip_address} (尝试 {attempt + 1}/{config.MAX_RETRIES})")
            except requests.exceptions.ConnectionError:
                self.logger.warning(f"连接失败，IP: {ip_address} (尝试 {attempt + 1}/{config.MAX_RETRIES})")
            except Exception as e:
                self.logger.error(f"API请求异常，IP: {ip_address}, 错误: {str(e)}")
                break
        
        self.logger.error(f"API请求最终失败，IP: {ip_address}")
        return None

    def process_user_data_by_device(self, all_data: List[Dict[str, Any]], config_data: List[Dict[str, str]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        按设备处理用户数据：每台设备取前N名用户

        Args:
            all_data: 所有用户数据列表
            config_data: 设备配置信息列表

        Returns:
            按局点分组的用户数据字典，键为局点名称，值为该局点的用户数据列表
        """
        try:
            self.logger.info(f"开始按设备处理用户数据，总计 {len(all_data)} 条记录")

            if not all_data:
                self.logger.warning("没有用户数据需要处理")
                return []

            # 创建DataFrame进行数据处理
            df = pd.DataFrame(all_data)

            # 确定用于排序的字段（优先使用Mb/s，否则使用原始B/s）
            sort_field = 'total_mbps' if 'total_mbps' in df.columns else 'total'

            if sort_field not in df.columns:
                self.logger.error(f"用户数据中缺少'{sort_field}'字段")
                return []

            # 转换排序字段为数值类型
            df[sort_field] = pd.to_numeric(df[sort_field], errors='coerce')

            # 移除排序字段为NaN的记录
            df = df.dropna(subset=[sort_field])

            # 创建设备信息映射（设备类型和机房）
            device_info_map = {}
            for device in config_data:
                device_info_map[device['ip_address']] = {
                    'device_type': device.get('device_type', 'Unknown'),
                    'machine_room': device.get('machine_room', 'Unknown')
                }

            # 按设备分组处理，然后按机房重新组织
            machine_room_data = {}  # 按机房分组的结果

            # 按source_ip分组
            grouped = df.groupby('source_ip')

            for device_ip, device_df in grouped:
                device_name = self.station_names.get(device_ip, device_ip)
                device_info = device_info_map.get(device_ip, {
                    'device_type': 'Unknown',
                    'machine_room': 'Unknown'
                })

                # 确定机房名称
                machine_room_name = device_info['machine_room']

                self.logger.info(f"处理设备: {device_name} ({device_ip})，机房: {machine_room_name}，用户数: {len(device_df)}")

                # 按用户ID去重，保留流速最大的记录
                if 'id' in device_df.columns:
                    device_df = device_df.sort_values(sort_field, ascending=False).drop_duplicates(subset=['id'], keep='first')

                # 按流速字段降序排序
                device_df = device_df.sort_values(sort_field, ascending=False)

                # 取前N名用户
                top_users = device_df.head(config.TOP_N_USERS_PER_DEVICE)

                # 为每个用户添加设备信息
                device_users = []
                for _, user in top_users.iterrows():
                    user_dict = user.to_dict()
                    user_dict['device_type'] = device_info['device_type']
                    user_dict['machine_room'] = device_info['machine_room']
                    device_users.append(user_dict)

                # 按机房分组
                if machine_room_name not in machine_room_data:
                    machine_room_data[machine_room_name] = []
                machine_room_data[machine_room_name].extend(device_users)

                self.logger.info(f"设备 {device_name} 获取前 {len(top_users)} 名用户")

            # 统计总用户数
            total_users = sum(len(users) for users in machine_room_data.values())
            self.logger.info(f"成功处理用户数据，{len(machine_room_data)}个机房，总计获取 {total_users} 名用户")

            return machine_room_data

        except Exception as e:
            self.logger.error(f"处理用户数据失败: {str(e)}")
            raise

    def create_output_excel(self, machine_room_data: Dict[str, List[Dict[str, Any]]], output_path: str):
        """
        创建输出Excel文件，按机房分组到不同工作表

        Args:
            machine_room_data: 按机房分组的用户数据字典
            output_path: 输出文件路径
        """
        try:
            self.logger.info(f"开始创建Excel文件: {output_path}")

            if not machine_room_data:
                self.logger.warning("没有数据可写入Excel文件")
                return

            # 定义输出列和中文表头（只保留需要的9个字段）
            column_mapping = {
                'device_type': '设备类型',      # 1. 设备类型
                'machine_room': '机房',        # 2. 机房
                'source_ip': '设备IP',         # 3. 设备IP
                'name': '用户名',              # 4. 用户名
                'ip': 'IP',                   # 5. IP
                'up_mbps': '上行Mb每s',        # 6. 上行Mb每s
                'down_mbps': '下行Mb每s',      # 7. 下行Mb每s
                'total_mbps': '总流速Mb每s',   # 8. 总流速Mb每s
                'session': '会话数'            # 9. 会话数
            }

            # 按您要求的列顺序（只要这9个字段）
            desired_order = [
                'device_type',      # 1. 设备类型
                'machine_room',     # 2. 机房
                'source_ip',        # 3. 设备IP
                'name',             # 4. 用户名
                'ip',               # 5. IP
                'up_mbps',          # 6. 上行Mb每s
                'down_mbps',        # 7. 下行Mb每s
                'total_mbps',       # 8. 总流速Mb每s
                'session'           # 9. 会话数
            ]

            # 写入Excel文件，每个局点一个工作表
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:

                for machine_room_name, user_data in machine_room_data.items():
                    if not user_data:
                        continue

                    # 创建DataFrame
                    df = pd.DataFrame(user_data)

                    # 只选择需要的9个字段，按指定顺序
                    output_columns = []
                    output_headers = []

                    # 严格按照desired_order添加列，不添加其他列
                    for col in desired_order:
                        if col in df.columns:
                            output_columns.append(col)
                            output_headers.append(column_mapping[col])

                    # 选择输出列
                    output_df = df[output_columns].copy()
                    output_df.columns = output_headers

                    # 格式化数值列（不要括号和斜杠）
                    numeric_columns = ['上行Mb每s', '下行Mb每s', '总流速Mb每s']
                    for col in numeric_columns:
                        if col in output_df.columns:
                            output_df[col] = pd.to_numeric(output_df[col], errors='coerce')
                            output_df[col] = output_df[col].round(3)  # Mb/s保留3位小数

                    # 清理工作表名称（Excel工作表名称限制）
                    sheet_name = machine_room_name.replace('/', '_').replace('\\', '_')[:31]

                    # 写入工作表
                    output_df.to_excel(
                        writer,
                        sheet_name=sheet_name,
                        index=False,
                        startrow=0
                    )

                    # 获取工作表对象进行格式化
                    worksheet = writer.sheets[sheet_name]

                    # 调整列宽
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 30)
                        worksheet.column_dimensions[column_letter].width = adjusted_width

                    self.logger.info(f"工作表 '{sheet_name}' 创建成功，包含 {len(output_df)} 条记录")

            self.logger.info(f"Excel文件创建成功: {output_path}，包含 {len(machine_room_data)} 个机房工作表")

        except Exception as e:
            self.logger.error(f"创建Excel文件失败: {str(e)}")
            raise

    def run(self):
        """运行主流程"""
        try:
            self.logger.info("=" * 50)
            self.logger.info("用户流量统计脚本开始运行")
            self.logger.info("=" * 50)

            # 1. 读取Excel配置文件
            if not os.path.exists(config.INPUT_FILE_PATH):
                raise FileNotFoundError(f"输入文件不存在: {config.INPUT_FILE_PATH}")

            config_data = self.read_excel_config(config.INPUT_FILE_PATH)

            if not config_data:
                raise ValueError("没有读取到有效的配置数据")

            # 2. 调用API获取数据
            self.logger.info("开始调用API获取用户流量数据...")
            all_user_data = []

            for i, config_item in enumerate(config_data, 1):
                station_name = config_item['station_name']
                ip_address = config_item['ip_address']

                self.logger.info(f"处理第 {i}/{len(config_data)} 个设备: {station_name} ({ip_address})")

                user_data = self.call_api(ip_address)
                if user_data:
                    all_user_data.extend(user_data)
                    self.logger.info(f"从 {station_name} 获取到 {len(user_data)} 条用户数据")
                else:
                    self.logger.warning(f"从 {station_name} 未获取到数据")

            self.logger.info(f"API调用完成，总计获取 {len(all_user_data)} 条用户数据")

            # 3. 处理数据
            if not all_user_data:
                self.logger.warning("没有获取到任何用户数据，程序结束")
                return

            machine_room_grouped_data = self.process_user_data_by_device(all_user_data, config_data)

            # 4. 生成输出
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # 4.1 输出到Excel（如果启用）
            if config.OUTPUT_TO_EXCEL:
                output_filename = f"各机房Top{config.TOP_N_USERS_PER_DEVICE}用户流速统计_{timestamp}.xlsx"
                output_path = os.path.join(config.OUTPUT_DIR, output_filename)
                self.create_output_excel(machine_room_grouped_data, output_path)
                self.logger.info(f"Excel文件保存完成: {output_path}")

            # 4.2 输出到数据库（如果启用）
            if config.OUTPUT_TO_DATABASE:
                # 将分组数据展平为列表
                all_users = []
                for machine_room_users in machine_room_grouped_data.values():
                    all_users.extend(machine_room_users)

                if self.save_to_database(all_users):
                    self.logger.info("数据库保存完成")
                else:
                    self.logger.warning("数据库保存失败")

            # 5. 输出统计信息
            total_users = sum(len(users) for users in machine_room_grouped_data.values())

            self.logger.info("=" * 50)
            self.logger.info("处理完成统计信息:")
            self.logger.info(f"处理设备数量: {len(config_data)}")
            self.logger.info(f"获取用户总数: {len(all_user_data)}")
            self.logger.info(f"输出机房数量: {len(machine_room_grouped_data)}")
            self.logger.info(f"输出用户数量: {total_users}")
            self.logger.info(f"每设备Top用户数: {config.TOP_N_USERS_PER_DEVICE}")
            if config.OUTPUT_TO_EXCEL:
                self.logger.info(f"Excel文件路径: {output_path}")
            self.logger.info("=" * 50)

            print(f"\n✅ 处理完成！")
            print(f"📊 处理了 {len(config_data)} 个设备")
            print(f"👥 获取了 {len(all_user_data)} 条用户流速数据")
            print(f"🏢 输出 {len(machine_room_grouped_data)} 个机房")
            print(f"🏆 每设备输出前 {config.TOP_N_USERS_PER_DEVICE} 名用户，总计 {total_users} 名用户")
            if config.OUTPUT_TO_EXCEL:
                print(f"📁 Excel文件: {output_path}")
            if config.OUTPUT_TO_DATABASE:
                print(f"💾 数据库: {config.DB_HOST}/{config.DB_NAME}.{config.DB_TABLE}")
            print(f"📏 流速单位: {config.OUTPUT_UNIT}")

            # 按机房统计输出信息
            print(f"\n📋 各机房统计:")
            for machine_room_name, users in machine_room_grouped_data.items():
                device_count = len(set(user.get('source_ip') for user in users))
                user_count = len(users)
                print(f"   {machine_room_name}: {device_count}个设备, {user_count}个用户")

        except Exception as e:
            self.logger.error(f"程序运行失败: {str(e)}")
            print(f"\n❌ 程序运行失败: {str(e)}")
            raise


def main():
    """主函数"""
    try:
        processor = UserFlowStatsProcessor()
        processor.run()
    except KeyboardInterrupt:
        print("\n⚠️  程序被用户中断")
    except Exception as e:
        print(f"\n❌ 程序异常退出: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
