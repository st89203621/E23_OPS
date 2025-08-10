"""
Elasticsearch数据库连接和查询模块
"""
from elasticsearch import Elasticsearch
from typing import Optional, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ESClient:
    """Elasticsearch客户端类"""
    
    def __init__(self, host: str, port: int, timeout: int = 30):
        """
        初始化ES客户端
        
        Args:
            host: ES服务器地址
            port: ES服务器端口
            timeout: 超时时间（秒）
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.es_url = f"http://{host}:{port}"
        self.client = None
    
    def connect(self) -> bool:
        """
        连接到Elasticsearch
        
        Returns:
            连接是否成功
        """
        try:
            self.client = Elasticsearch(
                [{'host': self.host, 'port': self.port}],
                timeout=self.timeout,
                max_retries=3,
                retry_on_timeout=True
            )
            
            # 测试连接
            health = self.client.cluster.health()
            logger.info(f"成功连接到ES集群: {self.es_url}, 状态: {health['status']}")
            return True
        
        except Exception as e:
            logger.error(f"连接ES集群失败: {e}")
            return False
    
    def disconnect(self) -> None:
        """断开ES连接"""
        if self.client:
            try:
                self.client.close()
                logger.info("已断开ES集群连接")
            except Exception as e:
                logger.error(f"断开ES连接时出错: {e}")
            finally:
                self.client = None
    
    def get_total_count(self, index_pattern: str, date_field: str, query_date: str) -> Optional[int]:
        """
        获取指定日期的总文档数
        
        Args:
            index_pattern: 索引模式
            date_field: 日期字段名
            query_date: 查询日期
            
        Returns:
            总文档数，失败时返回None
        """
        if not self.client:
            logger.error("ES连接未建立，请先调用connect()方法")
            return None
        
        try:
            # 构建查询条件
            query = {
                "query": {
                    "term": {
                        date_field: query_date
                    }
                }
            }
            
            logger.info(f"执行ES查询 - 索引: {index_pattern}, 日期字段: {date_field}, 查询日期: {query_date}")
            
            # 执行查询
            response = self.client.count(
                index=index_pattern,
                body=query
            )
            
            count = response['count']
            logger.info(f"ES查询成功，返回文档数: {count}")
            return count
        
        except Exception as e:
            logger.error(f"执行ES查询失败: {e}")
            return None
    
    def get_distinct_count(self, index_pattern: str, distinct_field: str, 
                          date_field: str, query_date: str) -> Optional[int]:
        """
        获取指定日期按指定字段去重后的文档数
        
        Args:
            index_pattern: 索引模式
            distinct_field: 去重字段名
            date_field: 日期字段名
            query_date: 查询日期
            
        Returns:
            去重后文档数，失败时返回None
        """
        if not self.client:
            logger.error("ES连接未建立，请先调用connect()方法")
            return None
        
        try:
            # 构建聚合查询
            query = {
                "size": 0,
                "query": {
                    "term": {
                        date_field: query_date
                    }
                },
                "aggs": {
                    "distinct_count": {
                        "cardinality": {
                            "field": distinct_field
                        }
                    }
                }
            }
            
            logger.info(f"执行ES去重查询 - 索引: {index_pattern}, 去重字段: {distinct_field}")
            
            # 执行查询
            response = self.client.search(
                index=index_pattern,
                body=query
            )
            
            distinct_count = response['aggregations']['distinct_count']['value']
            logger.info(f"ES去重查询成功，返回去重文档数: {distinct_count}")
            return distinct_count
        
        except Exception as e:
            logger.error(f"执行ES去重查询失败: {e}")
            return None
    
    def get_es_metrics(self, index_pattern: str, query_date: str,
                      date_field: str = "capture_dayField", 
                      distinct_field: str = "data_id") -> tuple[Optional[int], Optional[int]]:
        """
        获取ES指标数据
        
        Args:
            index_pattern: 索引模式
            query_date: 查询日期
            date_field: 日期字段名
            distinct_field: 去重字段名
            
        Returns:
            (总文档数, 去重后文档数) 的元组
        """
        logger.info(f"开始查询ES指标数据 - 索引: {index_pattern}, 日期: {query_date}")
        
        # 获取总文档数
        total_count = self.get_total_count(index_pattern, date_field, query_date)
        if total_count is None:
            logger.error(f"获取总文档数失败")
            return None, None
        
        # 获取去重后文档数
        distinct_count = self.get_distinct_count(index_pattern, distinct_field, date_field, query_date)
        if distinct_count is None:
            logger.error(f"获取去重后文档数失败")
            return total_count, None
        
        logger.info(f"ES指标查询完成 - 总文档数: {total_count}, 去重后文档数: {distinct_count}")
        return total_count, distinct_count
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
