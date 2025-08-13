"""
ODS数据库连接和查询模块
基于HiveClient，专门用于查询ODS数据源
"""
from pyhive import hive
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class ODSClient:
    """ODS数据客户端类"""
    
    def __init__(self, host: str, port: int, username: str = None, 
                 password: str = None, database: str = "v64_deye_dw_ods", auth: str = "PLAIN"):
        """
        初始化ODS客户端
        
        Args:
            host: ODS Hive服务器地址
            port: ODS Hive服务器端口
            username: 用户名
            password: 密码
            database: ODS数据库名
            auth: 认证方式
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.auth = auth
        self.connection = None
    
    def connect(self) -> bool:
        """
        连接到ODS Hive数据库
        
        Returns:
            连接是否成功
        """
        try:
            self.connection = hive.Connection(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                auth=self.auth
            )
            logger.info(f"成功连接到ODS数据库: {self.host}:{self.port}/{self.database}")
            return True
        
        except Exception as e:
            logger.error(f"连接ODS数据库失败: {e}")
            return False
    
    def disconnect(self) -> None:
        """断开ODS连接"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("已断开ODS数据库连接")
            except Exception as e:
                logger.error(f"断开ODS连接时出错: {e}")
            finally:
                self.connection = None
    
    def execute_query(self, sql: str) -> Optional[list]:
        """
        执行SQL查询
        
        Args:
            sql: SQL查询语句
            
        Returns:
            查询结果列表，失败时返回None
        """
        if not self.connection:
            logger.error("ODS连接未建立，请先调用connect()方法")
            return None
        
        try:
            cursor = self.connection.cursor()
            logger.info(f"执行ODS SQL查询: {sql}")
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            logger.info(f"ODS查询成功，返回 {len(results)} 行结果")
            return results
        
        except Exception as e:
            logger.error(f"执行ODS SQL查询失败: {e}")
            return None
    
    def get_total_count(self, table_name: str, date_field: str, query_date: str) -> Optional[int]:
        """
        获取ODS表指定日期的总记录数
        
        Args:
            table_name: ODS表名
            date_field: 日期字段名 (通常是capture_day)
            query_date: 查询日期
            
        Returns:
            总记录数，失败时返回None
        """
        # ODS表的查询逻辑：直接按capture_day查询
        sql = f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE {date_field} = '{query_date}'
        """
        results = self.execute_query(sql)
        
        if results and len(results) > 0:
            return results[0][0]
        return None
    
    def get_distinct_count(self, table_name: str, date_field: str, 
                          query_date: str, distinct_field: str = "data_id") -> Optional[int]:
        """
        获取ODS表指定日期的去重记录数
        
        Args:
            table_name: ODS表名
            date_field: 日期字段名
            query_date: 查询日期
            distinct_field: 去重字段名
            
        Returns:
            去重记录数，失败时返回None
        """
        sql = f"""
        SELECT COUNT(DISTINCT {distinct_field}) FROM {table_name}
        WHERE {date_field} = '{query_date}'
        """
        results = self.execute_query(sql)
        
        if results and len(results) > 0:
            return results[0][0]
        return None
    
    def get_ods_metrics(self, table_name: str, query_date: str,
                       date_field: str = "capture_day") -> Optional[int]:
        """
        获取ODS指标数据（支持单表或多表合并查询）

        Args:
            table_name: ODS表名，支持逗号分隔的多表名
            query_date: 查询日期
            date_field: 日期字段名

        Returns:
            总记录数
        """
        logger.info(f"开始查询ODS指标数据 - 表: {table_name}, 日期: {query_date}")

        # 检查是否为多表查询
        if ',' in table_name:
            tables = [t.strip() for t in table_name.split(',')]
            total_count = self.get_multi_table_count(tables, date_field, query_date)
        else:
            total_count = self.get_total_count(table_name, date_field, query_date)

        if total_count is None:
            logger.error(f"获取ODS总记录数失败")
            return None

        logger.info(f"ODS指标查询完成 - 总记录数: {total_count}")
        return total_count

    def get_multi_table_count(self, tables: list, date_field: str, query_date: str) -> Optional[int]:
        """
        获取多个ODS表的合并记录数

        Args:
            tables: ODS表名列表
            date_field: 日期字段名
            query_date: 查询日期

        Returns:
            合并总记录数，失败时返回None
        """
        total_count = 0

        for table in tables:
            logger.info(f"查询ODS子表: {table}")
            count = self.get_total_count(table, date_field, query_date)
            if count is not None:
                total_count += count
                logger.info(f"子表 {table} 记录数: {count}")
            else:
                logger.warning(f"子表 {table} 查询失败，跳过")

        logger.info(f"多表合并总记录数: {total_count}")
        return total_count if total_count > 0 else None
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
