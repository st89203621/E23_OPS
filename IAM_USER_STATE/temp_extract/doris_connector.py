#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Doris数据库连接器
支持Doris数据库的连接和操作
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
import time
from datetime import datetime

# 数据库相关导入
try:
    import pymysql
    DORIS_AVAILABLE = True
except ImportError:
    print("⚠️  警告: pymysql未安装，Doris数据库功能不可用。安装命令: pip install pymysql")
    DORIS_AVAILABLE = False

import config

class DorisConnector:
    """Doris数据库连接器"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化Doris连接器
        
        Args:
            logger: 日志记录器
        """
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None
        
    def connect(self) -> bool:
        """
        连接到Doris数据库
        
        Returns:
            是否连接成功
        """
        if not DORIS_AVAILABLE:
            self.logger.warning("Doris数据库功能不可用，pymysql未安装")
            return False
            
        try:
            self.logger.info(f"正在连接Doris数据库: {config.DB_HOST}:{config.DB_PORT}")
            
            # 连接Doris数据库（使用MySQL协议）
            self.connection = pymysql.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                database=config.DB_NAME,
                charset='utf8mb4',
                autocommit=True,
                connect_timeout=30,
                read_timeout=60,
                write_timeout=60
            )
            
            self.logger.info("Doris数据库连接成功")
            return True
            
        except Exception as e:
            self.logger.error(f"Doris数据库连接失败: {str(e)}")
            return False
    
    def disconnect(self):
        """断开数据库连接"""
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Doris数据库连接已关闭")
            except Exception as e:
                self.logger.warning(f"关闭Doris数据库连接时出错: {str(e)}")
            finally:
                self.connection = None
    
    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> Optional[List[Dict]]:
        """
        执行查询SQL
        
        Args:
            sql: SQL语句
            params: 参数
            
        Returns:
            查询结果列表
        """
        if not self.connection:
            if not self.connect():
                return None
                
        try:
            cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sql, params)
            result = cursor.fetchall()
            cursor.close()
            return result
            
        except Exception as e:
            self.logger.error(f"执行查询失败: {str(e)}")
            return None
    
    def execute_non_query(self, sql: str, params: Optional[Tuple] = None) -> bool:
        """
        执行非查询SQL（INSERT, UPDATE, DELETE等）
        
        Args:
            sql: SQL语句
            params: 参数
            
        Returns:
            是否执行成功
        """
        if not self.connection:
            if not self.connect():
                return False
                
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, params)
            cursor.close()
            return True
            
        except Exception as e:
            self.logger.error(f"执行SQL失败: {str(e)}")
            return False
    
    def batch_insert(self, table_name: str, columns: List[str], data: List[Tuple]) -> Tuple[bool, int]:
        """
        批量插入数据
        
        Args:
            table_name: 表名
            columns: 列名列表
            data: 数据列表
            
        Returns:
            (是否成功, 插入行数)
        """
        if not self.connection:
            if not self.connect():
                return False, 0
                
        if not data:
            return True, 0
            
        try:
            cursor = self.connection.cursor()
            
            # 构建SQL语句
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join([f'`{col}`' for col in columns])
            sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
            
            # 批量插入
            cursor.executemany(sql, data)
            affected_rows = cursor.rowcount
            cursor.close()
            
            self.logger.info(f"批量插入成功: {affected_rows} 行数据插入到表 {table_name}")
            return True, affected_rows
            
        except Exception as e:
            self.logger.error(f"批量插入失败: {str(e)}")
            return False, 0
    
    def create_table_if_not_exists(self, table_name: str, create_sql: str) -> bool:
        """
        创建表（如果不存在）
        
        Args:
            table_name: 表名
            create_sql: 建表SQL
            
        Returns:
            是否成功
        """
        try:
            if self.execute_non_query(create_sql):
                self.logger.info(f"数据库表 {table_name} 准备完成")
                return True
            else:
                self.logger.error(f"创建表 {table_name} 失败")
                return False
                
        except Exception as e:
            self.logger.error(f"创建表 {table_name} 时出错: {str(e)}")
            return False
    
    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """
        获取表信息
        
        Args:
            table_name: 表名
            
        Returns:
            表信息字典
        """
        sql = f"SHOW CREATE TABLE `{table_name}`"
        result = self.execute_query(sql)
        
        if result and len(result) > 0:
            return result[0]
        return None
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        获取表行数
        
        Args:
            table_name: 表名
            
        Returns:
            行数
        """
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.execute_query(sql)
        
        if result and len(result) > 0:
            return result[0].get('count', 0)
        return 0
    
    def test_connection(self) -> bool:
        """
        测试数据库连接
        
        Returns:
            连接是否正常
        """
        try:
            if not self.connection:
                if not self.connect():
                    return False
            
            # 执行简单查询测试连接
            result = self.execute_query("SELECT 1 as test")
            return result is not None and len(result) > 0
            
        except Exception as e:
            self.logger.error(f"测试连接失败: {str(e)}")
            return False
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()


def test_doris_connection():
    """测试Doris数据库连接"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("开始测试Doris数据库连接...")
    
    with DorisConnector(logger) as connector:
        if connector.test_connection():
            logger.info("✅ Doris数据库连接测试成功")
            
            # 测试查询数据库信息
            result = connector.execute_query("SELECT DATABASE() as current_db")
            if result:
                logger.info(f"当前数据库: {result[0]['current_db']}")
            
            # 测试查询表信息
            result = connector.execute_query("SHOW TABLES")
            if result:
                tables = [row[f'Tables_in_{config.DB_NAME}'] for row in result]
                logger.info(f"数据库中的表: {tables}")
            
            return True
        else:
            logger.error("❌ Doris数据库连接测试失败")
            return False


if __name__ == "__main__":
    test_doris_connection()
