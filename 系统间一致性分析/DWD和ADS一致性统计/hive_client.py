"""
Hive数据库连接和查询模块
"""
from pyhive import hive
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class HiveClient:
    """Hive客户端类"""
    
    def __init__(self, host: str, port: int, username: str = None, 
                 password: str = None, database: str = "default", auth: str = "PLAIN"):
        """
        初始化Hive客户端
        
        Args:
            host: Hive服务器地址
            port: Hive服务器端口
            username: 用户名
            password: 密码
            database: 数据库名
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
        连接到Hive数据库
        
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
            logger.info(f"成功连接到Hive数据库: {self.host}:{self.port}/{self.database}")
            return True
        
        except Exception as e:
            logger.error(f"连接Hive数据库失败: {e}")
            return False
    
    def disconnect(self) -> None:
        """断开Hive连接"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("已断开Hive数据库连接")
            except Exception as e:
                logger.error(f"断开Hive连接时出错: {e}")
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
            logger.error("Hive连接未建立，请先调用connect()方法")
            return None
        
        try:
            cursor = self.connection.cursor()
            logger.info(f"执行SQL查询: {sql}")
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            logger.info(f"查询成功，返回 {len(results)} 行结果")
            return results
        
        except Exception as e:
            logger.error(f"执行SQL查询失败: {e}")
            return None
    
    def get_total_count(self, table_name: str, date_field: str, query_date: str) -> Optional[int]:
        """
        获取指定日期的总记录数

        Args:
            table_name: 表名
            date_field: 日期字段名insert_day
            query_date: 查询日期

        Returns:
            总记录数，失败时返回None
        """
        # 构建扩展时间范围的SQL查询
        # 包含：前一天19:00-23:59 + 当天00:00-05:00 + 当天数据
        sql = f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE capture_day = '{query_date}'
        AND (
            ({date_field} = DATE_SUB('{query_date}', 1) AND insert_hour >= 19)
            OR
            ({date_field} = DATE_ADD('{query_date}', 1) AND insert_hour <= 5)
            OR
            ({date_field} = '{query_date}')
        )
        """
        results = self.execute_query(sql)

        if results and len(results) > 0:
            return results[0][0]
        return None
    
    def get_distinct_count(self, table_name: str, distinct_field: str,
                          date_field: str, query_date: str) -> Optional[int]:
        """
        获取指定日期按指定字段去重后的记录数

        Args:
            table_name: 表名
            distinct_field: 去重字段名
            date_field: 日期字段名
            query_date: 查询日期

        Returns:
            去重后记录数，失败时返回None
        """
        # 构建扩展时间范围的SQL查询
        # 包含：前一天19:00-23:59 + 当天00:00-05:00 + 当天数据
        sql = f"""
        SELECT COUNT(DISTINCT {distinct_field}) FROM {table_name}
        WHERE capture_day = '{query_date}'
        AND (
            ({date_field} = DATE_SUB('{query_date}', 1) AND insert_hour >= 19)
            OR
            ({date_field} = DATE_ADD('{query_date}', 1) AND insert_hour <= 5)
            OR
            ({date_field} = '{query_date}')
        )
        """
        results = self.execute_query(sql)

        if results and len(results) > 0:
            return results[0][0]
        return None
    
    def get_hive_metrics(self, table_name: str, query_date: str, 
                        date_field: str = "insert_day", 
                        distinct_field: str = "data_id") -> Tuple[Optional[int], Optional[int]]:
        """
        获取Hive指标数据
        
        Args:
            table_name: 表名
            query_date: 查询日期
            date_field: 日期字段名
            distinct_field: 去重字段名
            
        Returns:
            (总记录数, 去重后记录数) 的元组
        """
        logger.info(f"开始查询Hive指标数据 - 表: {table_name}, 日期: {query_date}")
        
        # 获取总记录数
        total_count = self.get_total_count(table_name, date_field, query_date)
        if total_count is None:
            logger.error(f"获取总记录数失败")
            return None, None
        
        # 获取去重后记录数
        distinct_count = self.get_distinct_count(table_name, distinct_field, date_field, query_date)
        if distinct_count is None:
            logger.error(f"获取去重后记录数失败")
            return total_count, None
        
        logger.info(f"Hive指标查询完成 - 总记录数: {total_count}, 去重后记录数: {distinct_count}")
        return total_count, distinct_count
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
