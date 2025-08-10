"""
DWD和ADS一致性统计主程序
"""
import yaml
import logging
import os
import csv
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
import sys

from hive_client import HiveClient
from es_client import ESClient


class DataQualityMonitor:
    """数据质量监控器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化监控器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.results = []
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            sys.exit(1)
    
    def _setup_logging(self) -> None:
        """设置日志"""
        log_dir = self.config['output']['log_directory']
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"data_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("数据质量监控器初始化完成")
    
    def _get_query_date(self) -> str:
        """获取查询日期"""
        default_date = self.config['date']['default_query_date']
        date_format = self.config['date']['date_format']
        
        if default_date == "yesterday":
            # 使用昨天的日期
            yesterday = datetime.now() - timedelta(days=1)
            return yesterday.strftime(date_format)
        elif default_date:
            # 使用配置中指定的日期
            return default_date
        else:
            # 使用当前日期
            return datetime.now().strftime(date_format)
    
    def _format_index_pattern(self, pattern: str, query_date: str) -> str:
        """格式化ES索引模式"""
        date_obj = datetime.strptime(query_date, self.config['date']['date_format'])
        return pattern.format(
            year=date_obj.year,
            month=date_obj.month
        )
    
    def compare_protocol_data(self, protocol_name: str, protocol_config: Dict[str, str], 
                            query_date: str) -> Dict[str, Any]:
        """
        比较单个协议的DWD和ADS数据
        
        Args:
            protocol_name: 协议名称
            protocol_config: 协议配置
            query_date: 查询日期
            
        Returns:
            比较结果字典
        """
        self.logger.info(f"开始比较协议 {protocol_name} 的数据一致性")
        
        result = {
            'protocol': protocol_name,
            'query_date': query_date,
            'hive_total': None,
            'hive_distinct': None,
            'es_total': None,
            'es_distinct': None,
            'total_diff': None,
            'distinct_diff': None,
            'total_consistency_rate': None,
            'distinct_consistency_rate': None,
            'status': 'FAILED',
            'error': None
        }
        
        try:
            # 连接Hive并查询数据
            hive_config = self.config['hive']
            with HiveClient(
                host=hive_config['host'],
                port=hive_config['port'],
                username=hive_config.get('username'),
                password=hive_config.get('password'),
                database=hive_config['database'],
                auth=hive_config.get('auth', 'PLAIN')
            ) as hive_client:
                
                hive_total, hive_distinct = hive_client.get_hive_metrics(
                    table_name=protocol_config['hive_table'],
                    query_date=query_date
                )
                
                result['hive_total'] = hive_total
                result['hive_distinct'] = hive_distinct
            
            # 连接ES并查询数据
            es_config = self.config['elasticsearch']
            with ESClient(
                host=es_config['host'],
                port=es_config['port'],
                timeout=es_config.get('timeout', 30)
            ) as es_client:
                
                index_pattern = self._format_index_pattern(
                    protocol_config['es_index_pattern'], 
                    query_date
                )
                
                es_total, es_distinct = es_client.get_es_metrics(
                    index_pattern=index_pattern,
                    query_date=query_date,
                    date_field=protocol_config['es_date_field']
                )
                
                result['es_total'] = es_total
                result['es_distinct'] = es_distinct
            
            # 计算差异和一致性率
            if hive_total is not None and es_total is not None:
                result['total_diff'] = hive_total - es_total
                if max(hive_total, es_total) > 0:
                    result['total_consistency_rate'] = min(hive_total, es_total) / max(hive_total, es_total) * 100
                else:
                    result['total_consistency_rate'] = 100.0
            
            if hive_distinct is not None and es_distinct is not None:
                result['distinct_diff'] = hive_distinct - es_distinct
                if max(hive_distinct, es_distinct) > 0:
                    result['distinct_consistency_rate'] = min(hive_distinct, es_distinct) / max(hive_distinct, es_distinct) * 100
                else:
                    result['distinct_consistency_rate'] = 100.0
            
            # 判断状态
            if (result['total_consistency_rate'] is not None and 
                result['distinct_consistency_rate'] is not None):
                if (result['total_consistency_rate'] >= 95 and 
                    result['distinct_consistency_rate'] >= 95):
                    result['status'] = 'GOOD'
                elif (result['total_consistency_rate'] >= 90 and 
                      result['distinct_consistency_rate'] >= 90):
                    result['status'] = 'WARNING'
                else:
                    result['status'] = 'ERROR'
            
            self.logger.info(f"协议 {protocol_name} 比较完成 - 状态: {result['status']}")
            
        except Exception as e:
            self.logger.error(f"比较协议 {protocol_name} 时发生错误: {e}")
            result['error'] = str(e)
        
        return result
    
    def run_comparison(self) -> None:
        """运行完整的数据一致性比较"""
        self.logger.info("开始执行DWD和ADS数据一致性比较")
        
        query_date = self._get_query_date()
        self.logger.info(f"查询日期: {query_date}")
        
        protocols = self.config['protocols']
        total_protocols = len(protocols)
        
        for i, (protocol_name, protocol_config) in enumerate(protocols.items(), 1):
            self.logger.info(f"[{i}/{total_protocols}] 处理协议: {protocol_name}")
            
            result = self.compare_protocol_data(protocol_name, protocol_config, query_date)
            self.results.append(result)

            # 每完成一个协议就输出结果
            self._print_protocol_result(result, i, total_protocols)
        
        self.logger.info("数据一致性比较完成")
    
    def save_results(self) -> None:
        """保存比较结果到CSV文件"""
        if not self.results:
            self.logger.warning("没有结果需要保存")
            return
        
        # 创建输出目录
        output_dir = self.config['output']['csv_directory']
        os.makedirs(output_dir, exist_ok=True)
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = os.path.join(output_dir, f"dwd_ads_consistency_{timestamp}.csv")
        
        # 写入CSV文件
        fieldnames = [
            'protocol', 'query_date', 'hive_total', 'hive_distinct', 
            'es_total', 'es_distinct', 'total_diff', 'distinct_diff',
            'total_consistency_rate', 'distinct_consistency_rate', 'status', 'error'
        ]
        
        try:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.results)
            
            self.logger.info(f"结果已保存到: {csv_file}")
            
            # 打印摘要
            self._print_summary()
            
        except Exception as e:
            self.logger.error(f"保存结果文件失败: {e}")
    
    def _print_protocol_result(self, result: Dict[str, Any], current: int, total: int) -> None:
        """打印单个协议的结果"""
        status_color = {
            'GOOD': '\033[92m',      # 绿色
            'WARNING': '\033[93m',   # 黄色
            'ERROR': '\033[91m',     # 红色
            'FAILED': '\033[91m'     # 红色
        }
        reset_color = '\033[0m'

        color = status_color.get(result['status'], '')

        print(f"\n[{current}/{total}] {color}{result['protocol'].upper()}{reset_color} - {color}{result['status']}{reset_color}")
        print(f"  Hive: 总数={result['hive_total']:,}, 去重={result['hive_distinct']:,}")
        print(f"  ES:   总数={result['es_total']:,}, 去重={result['es_distinct']:,}")

        if result['status'] != 'FAILED':
            print(f"  一致性: 总数={result['total_consistency_rate']:.1f}%, 去重={result['distinct_consistency_rate']:.1f}%")

        if result['error']:
            print(f"  错误: {result['error']}")

    def _print_summary(self) -> None:
        """打印统计摘要"""
        if not self.results:
            return

        total_count = len(self.results)
        good_count = sum(1 for r in self.results if r['status'] == 'GOOD')
        warning_count = sum(1 for r in self.results if r['status'] == 'WARNING')
        error_count = sum(1 for r in self.results if r['status'] == 'ERROR')
        failed_count = sum(1 for r in self.results if r['status'] == 'FAILED')

        print("\n" + "="*60)
        print("数据一致性统计摘要")
        print("="*60)
        print(f"总协议数: {total_count}")
        print(f"良好 (GOOD): {good_count} ({good_count/total_count*100:.1f}%)")
        print(f"警告 (WARNING): {warning_count} ({warning_count/total_count*100:.1f}%)")
        print(f"错误 (ERROR): {error_count} ({error_count/total_count*100:.1f}%)")
        print(f"失败 (FAILED): {failed_count} ({failed_count/total_count*100:.1f}%)")
        print("="*60)


def main():
    """主函数"""
    monitor = DataQualityMonitor()
    monitor.run_comparison()
    monitor.save_results()


if __name__ == "__main__":
    main()
