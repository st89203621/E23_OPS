"""
DWD和ADS一致性统计主程序 - 并发版本（仅总数统计）
"""
import yaml
import logging
import os
import csv
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import sys
import concurrent.futures
import threading
from dataclasses import dataclass

from hive_client import HiveClient
from es_client import ESClient


@dataclass
class ProtocolResult:
    """协议统计结果"""
    protocol: str
    query_date: str
    hive_total: Optional[int] = None
    es_total: Optional[int] = None
    total_diff: Optional[int] = None
    consistency_rate: Optional[float] = None
    status: str = 'PENDING'
    error: Optional[str] = None
    duration: Optional[float] = None


class ConcurrentDataQualityMonitor:
    """并发数据质量监控器"""
    
    def __init__(self, config_path: str = "config.yaml", max_workers: int = 8):
        """
        初始化监控器
        
        Args:
            config_path: 配置文件路径
            max_workers: 最大并发数
        """
        self.config = self._load_config(config_path)
        self.max_workers = max_workers
        self._setup_logging()
        self.results: List[ProtocolResult] = []
        self.lock = threading.Lock()
        
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
        
        log_file = os.path.join(log_dir, f"data_quality_concurrent_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("并发数据质量监控器初始化完成")
    
    def _get_query_date(self) -> str:
        """获取查询日期"""
        date_config = self.config.get('date', {})
        default_date = date_config.get('default_query_date', 'yesterday')
        
        if default_date == 'yesterday':
            return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            return default_date
    
    def _format_index_pattern(self, pattern: str, query_date: str) -> str:
        """格式化ES索引模式"""
        date_obj = datetime.strptime(query_date, '%Y-%m-%d')
        return pattern.format(
            year=date_obj.year,
            month=date_obj.month
        )
    
    def _compare_single_protocol(self, protocol_name: str, protocol_config: Dict[str, Any], query_date: str) -> ProtocolResult:
        """比较单个协议的数据（仅总数）"""
        start_time = datetime.now()
        result = ProtocolResult(
            protocol=protocol_name,
            query_date=query_date
        )
        
        try:
            self.logger.info(f"开始处理协议: {protocol_name}")
            
            # 查询Hive数据
            hive_config = self.config['hive']
            with HiveClient(
                host=hive_config['host'],
                port=hive_config['port'],
                username=hive_config.get('username'),
                password=hive_config.get('password'),
                database=hive_config['database'],
                auth=hive_config.get('auth', 'PLAIN')
            ) as hive_client:
                
                hive_total = hive_client.get_hive_metrics(
                    table_name=protocol_config['hive_table'],
                    query_date=query_date
                )
                result.hive_total = hive_total
            
            # 查询ES数据
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
                
                es_total = es_client.get_es_metrics(
                    index_pattern=index_pattern,
                    query_date=query_date,
                    date_field=protocol_config['es_date_field']
                )
                result.es_total = es_total
            
            # 计算差异和一致性率
            if hive_total is not None and es_total is not None:
                result.total_diff = hive_total - es_total
                if max(hive_total, es_total) > 0:
                    result.consistency_rate = min(hive_total, es_total) / max(hive_total, es_total) * 100
                else:
                    result.consistency_rate = 100.0
                
                # 判断状态
                if result.consistency_rate >= 99.0:
                    result.status = 'GOOD'
                elif result.consistency_rate >= 95.0:
                    result.status = 'WARNING'
                else:
                    result.status = 'ERROR'
            else:
                result.status = 'FAILED'
            
            # 计算耗时
            result.duration = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(f"协议 {protocol_name} 处理完成 - 状态: {result.status}, 耗时: {result.duration:.1f}s")
            
        except Exception as e:
            result.status = 'FAILED'
            result.error = str(e)
            result.duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"处理协议 {protocol_name} 时发生错误: {e}")
        
        # 线程安全地添加结果并打印
        with self.lock:
            self.results.append(result)
            self._print_protocol_result(result, len(self.results))
        
        return result
    
    def _print_protocol_result(self, result: ProtocolResult, current: int) -> None:
        """打印单个协议的结果"""
        status_color = {
            'GOOD': '\033[92m',      # 绿色
            'WARNING': '\033[93m',   # 黄色
            'ERROR': '\033[91m',     # 红色
            'FAILED': '\033[91m'     # 红色
        }
        reset_color = '\033[0m'
        
        color = status_color.get(result.status, '')
        
        print(f"\n[{current}/29] {color}{result.protocol.upper()}{reset_color} - {color}{result.status}{reset_color} ({result.duration:.1f}s)")
        
        if result.hive_total is not None and result.es_total is not None:
            print(f"  Hive: {result.hive_total:,}")
            print(f"  ES:   {result.es_total:,}")
            print(f"  差异: {result.total_diff:,}")
            print(f"  一致性: {result.consistency_rate:.1f}%")
        else:
            print(f"  Hive: {result.hive_total}")
            print(f"  ES:   {result.es_total}")
        
        if result.error:
            print(f"  错误: {result.error}")
    
    def run_comparison(self) -> None:
        """运行并发数据一致性比较"""
        self.logger.info("开始执行并发DWD和ADS数据一致性比较")
        
        query_date = self._get_query_date()
        self.logger.info(f"查询日期: {query_date}")
        self.logger.info(f"并发数: {self.max_workers}")
        
        protocols = self.config['protocols']
        total_protocols = len(protocols)
        
        print(f"\n🚀 开始并发统计 {total_protocols} 个协议 (并发数: {self.max_workers})")
        print("="*80)
        
        start_time = datetime.now()
        
        # 使用线程池并发执行
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_protocol = {
                executor.submit(self._compare_single_protocol, protocol_name, protocol_config, query_date): protocol_name
                for protocol_name, protocol_config in protocols.items()
            }
            
            # 等待所有任务完成
            concurrent.futures.wait(future_to_protocol.keys())
        
        total_duration = (datetime.now() - start_time).total_seconds()
        
        print(f"\n🎉 所有协议统计完成！总耗时: {total_duration:.1f}秒")
        self.logger.info(f"并发数据一致性比较完成，总耗时: {total_duration:.1f}秒")
    
    def save_results(self) -> None:
        """保存比较结果到CSV文件"""
        if not self.results:
            self.logger.warning("没有结果需要保存")
            return
        
        # 按协议名排序
        self.results.sort(key=lambda x: x.protocol)
        
        # 创建输出目录
        output_dir = self.config['output']['csv_directory']
        os.makedirs(output_dir, exist_ok=True)
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = os.path.join(output_dir, f"dwd_ads_consistency_concurrent_{timestamp}.csv")
        
        # 写入CSV文件
        fieldnames = [
            'protocol', 'query_date', 'hive_total', 'es_total', 
            'total_diff', 'consistency_rate', 'status', 'duration', 'error'
        ]
        
        try:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for result in self.results:
                    writer.writerow({
                        'protocol': result.protocol,
                        'query_date': result.query_date,
                        'hive_total': result.hive_total,
                        'es_total': result.es_total,
                        'total_diff': result.total_diff,
                        'consistency_rate': result.consistency_rate,
                        'status': result.status,
                        'duration': result.duration,
                        'error': result.error
                    })
            
            self.logger.info(f"结果已保存到: {csv_file}")
            
            # 打印摘要
            self._print_summary()
            
        except Exception as e:
            self.logger.error(f"保存结果文件失败: {e}")
    
    def _print_summary(self) -> None:
        """打印统计摘要"""
        if not self.results:
            return
        
        total_count = len(self.results)
        good_count = sum(1 for r in self.results if r.status == 'GOOD')
        warning_count = sum(1 for r in self.results if r.status == 'WARNING')
        error_count = sum(1 for r in self.results if r.status == 'ERROR')
        failed_count = sum(1 for r in self.results if r.status == 'FAILED')
        
        avg_duration = sum(r.duration for r in self.results if r.duration) / total_count
        
        print("\n" + "="*80)
        print("📊 数据一致性统计摘要")
        print("="*80)
        print(f"总协议数: {total_count}")
        print(f"🟢 良好 (GOOD): {good_count} ({good_count/total_count*100:.1f}%)")
        print(f"🟡 警告 (WARNING): {warning_count} ({warning_count/total_count*100:.1f}%)")
        print(f"🔴 错误 (ERROR): {error_count} ({error_count/total_count*100:.1f}%)")
        print(f"❌ 失败 (FAILED): {failed_count} ({failed_count/total_count*100:.1f}%)")
        print(f"⏱️  平均耗时: {avg_duration:.1f}秒/协议")
        print("="*80)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='DWD和ADS数据一致性并发统计')
    parser.add_argument('--workers', type=int, default=8, help='并发线程数 (默认: 8)')
    args = parser.parse_args()
    
    monitor = ConcurrentDataQualityMonitor(max_workers=args.workers)
    monitor.run_comparison()
    monitor.save_results()


if __name__ == "__main__":
    main()
