"""
ODS、DWD和ADS三方一致性统计并发版本
支持多线程并发执行，提高统计效率
"""
import yaml
import logging
import os
import csv
import concurrent.futures
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import sys

from hive_client import HiveClient
from ods_client import ODSClient
from es_client import ESClient


class ComparisonResult:
    """比较结果数据类"""
    def __init__(self, protocol: str, query_date: str):
        self.protocol = protocol
        self.query_date = query_date
        self.ods_total = None
        self.dwd_total = None
        self.ads_total = None
        self.ods_dwd_diff = None
        self.ods_ads_diff = None
        self.dwd_ads_diff = None
        self.ods_dwd_consistency_rate = None
        self.ods_ads_consistency_rate = None
        self.dwd_ads_consistency_rate = None
        self.status = 'FAILED'
        self.error = None
        self.duration = 0.0


class DataQualityMonitorODSConcurrent:
    """ODS、DWD、ADS三方数据质量并发监控器"""
    
    def __init__(self, config_path: str = "config.yaml", max_workers: int = 5):
        """
        初始化并发监控器
        
        Args:
            config_path: 配置文件路径
            max_workers: 最大并发数
        """
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.max_workers = max_workers
        self.results = []
        self.results_lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
    
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
        
        log_file = os.path.join(log_dir, f"ods_dwd_ads_concurrent_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def _get_query_date(self) -> str:
        """获取查询日期"""
        date_config = self.config.get('date', {})
        default_date = date_config.get('default_query_date', 'yesterday')
        
        if default_date == 'yesterday':
            query_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            query_date = default_date
        
        return query_date
    
    def _format_index_pattern(self, pattern: str, query_date: str) -> str:
        """格式化ES索引模式"""
        date_obj = datetime.strptime(query_date, '%Y-%m-%d')
        return pattern.format(
            year=date_obj.year,
            month=date_obj.month
        )
    
    def _compare_single_protocol(self, protocol_name: str, protocol_config: Dict[str, str], 
                               query_date: str) -> ComparisonResult:
        """
        比较单个协议的三方数据（线程安全版本）
        
        Args:
            protocol_name: 协议名称
            protocol_config: 协议配置
            query_date: 查询日期
            
        Returns:
            比较结果对象
        """
        start_time = datetime.now()
        result = ComparisonResult(protocol_name, query_date)
        
        try:
            self.logger.info(f"[线程] 开始处理协议: {protocol_name}")
            
            # 1. 查询ODS数据
            if 'ods_table' in protocol_config:
                ods_config = self.config['hive_ods']
                with ODSClient(
                    host=ods_config['host'],
                    port=ods_config['port'],
                    username=ods_config.get('username'),
                    password=ods_config.get('password'),
                    database=ods_config['database'],
                    auth=ods_config.get('auth', 'PLAIN')
                ) as ods_client:
                    result.ods_total = ods_client.get_ods_metrics(
                        table_name=protocol_config['ods_table'],
                        query_date=query_date,
                        date_field=protocol_config.get('date_field', 'capture_day')
                    )
            
            # 2. 查询DWD数据
            if 'dwd_table' in protocol_config:
                dwd_config = self.config['hive_dwd']
                with HiveClient(
                    host=dwd_config['host'],
                    port=dwd_config['port'],
                    username=dwd_config.get('username'),
                    password=dwd_config.get('password'),
                    database=dwd_config['database'],
                    auth=dwd_config.get('auth', 'PLAIN')
                ) as dwd_client:
                    result.dwd_total = dwd_client.get_hive_metrics(
                        table_name=protocol_config['dwd_table'],
                        query_date=query_date,
                        date_field=protocol_config.get('date_field', 'capture_day')
                    )
            
            # 3. 查询ADS数据 (ES)
            if 'es_index_pattern' in protocol_config:
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
                    result.ads_total = es_client.get_es_metrics(
                        index_pattern=index_pattern,
                        query_date=query_date,
                        date_field=protocol_config['es_date_field']
                    )
            
            # 4. 计算一致性指标
            self._calculate_consistency_metrics(result)
            
            result.status = 'SUCCESS'
            
        except Exception as e:
            self.logger.error(f"[线程] 处理协议 {protocol_name} 失败: {e}")
            result.error = str(e)
        
        finally:
            result.duration = (datetime.now() - start_time).total_seconds()
            
            # 线程安全地添加结果和打印
            with self.results_lock:
                self.results.append(result)
                self._print_protocol_result(result)
        
        return result
    
    def _calculate_consistency_metrics(self, result: ComparisonResult) -> None:
        """计算一致性指标"""
        # ODS vs DWD
        if result.ods_total is not None and result.dwd_total is not None:
            result.ods_dwd_diff = result.ods_total - result.dwd_total
            if max(result.ods_total, result.dwd_total) > 0:
                result.ods_dwd_consistency_rate = min(result.ods_total, result.dwd_total) / max(result.ods_total, result.dwd_total) * 100
            else:
                result.ods_dwd_consistency_rate = 100.0
        
        # ODS vs ADS
        if result.ods_total is not None and result.ads_total is not None:
            result.ods_ads_diff = result.ods_total - result.ads_total
            if max(result.ods_total, result.ads_total) > 0:
                result.ods_ads_consistency_rate = min(result.ods_total, result.ads_total) / max(result.ods_total, result.ads_total) * 100
            else:
                result.ods_ads_consistency_rate = 100.0
        
        # DWD vs ADS
        if result.dwd_total is not None and result.ads_total is not None:
            result.dwd_ads_diff = result.dwd_total - result.ads_total
            if max(result.dwd_total, result.ads_total) > 0:
                result.dwd_ads_consistency_rate = min(result.dwd_total, result.ads_total) / max(result.dwd_total, result.ads_total) * 100
            else:
                result.dwd_ads_consistency_rate = 100.0
    
    def _print_protocol_result(self, result: ComparisonResult) -> None:
        """打印单个协议的比较结果（线程安全）"""
        protocol = result.protocol
        status = result.status
        
        print(f"\n✅ 协议: {protocol} | 状态: {status} | 耗时: {result.duration:.1f}s")
        
        if status == 'SUCCESS':
            print(f"   📊 ODS: {result.ods_total or 'N/A':,} | DWD: {result.dwd_total or 'N/A':,} | ADS: {result.ads_total or 'N/A':,}")
            
            if result.ods_dwd_consistency_rate is not None:
                print(f"   🔄 ODS-DWD一致性: {result.ods_dwd_consistency_rate:.2f}%")
            if result.ods_ads_consistency_rate is not None:
                print(f"   🔄 ODS-ADS一致性: {result.ods_ads_consistency_rate:.2f}%")
            if result.dwd_ads_consistency_rate is not None:
                print(f"   🔄 DWD-ADS一致性: {result.dwd_ads_consistency_rate:.2f}%")
        else:
            print(f"   ❌ 错误: {result.error}")

    def run_comparison(self) -> None:
        """运行并发三方数据一致性比较"""
        self.logger.info("开始执行并发ODS、DWD和ADS三方数据一致性比较")

        query_date = self._get_query_date()
        self.logger.info(f"查询日期: {query_date}")
        self.logger.info(f"并发数: {self.max_workers}")

        protocols = self.config['protocols']
        total_protocols = len(protocols)

        print(f"\n🚀 开始并发三方数据一致性统计 {total_protocols} 个协议 (并发数: {self.max_workers})")
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
        self.logger.info(f"并发三方数据一致性比较完成，总耗时: {total_duration:.1f}秒")

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
        csv_file = os.path.join(output_dir, f"ods_dwd_ads_consistency_concurrent_{timestamp}.csv")

        # 写入CSV文件
        fieldnames = [
            'protocol', 'query_date',
            'ods_total', 'dwd_total', 'ads_total',
            'ods_dwd_diff', 'ods_ads_diff', 'dwd_ads_diff',
            'ods_dwd_consistency_rate', 'ods_ads_consistency_rate', 'dwd_ads_consistency_rate',
            'status', 'duration', 'error'
        ]

        try:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                for result in self.results:
                    # 转换结果对象为字典
                    row = {
                        'protocol': result.protocol,
                        'query_date': result.query_date,
                        'ods_total': result.ods_total if result.ods_total is not None else '',
                        'dwd_total': result.dwd_total if result.dwd_total is not None else '',
                        'ads_total': result.ads_total if result.ads_total is not None else '',
                        'ods_dwd_diff': result.ods_dwd_diff if result.ods_dwd_diff is not None else '',
                        'ods_ads_diff': result.ods_ads_diff if result.ods_ads_diff is not None else '',
                        'dwd_ads_diff': result.dwd_ads_diff if result.dwd_ads_diff is not None else '',
                        'ods_dwd_consistency_rate': result.ods_dwd_consistency_rate if result.ods_dwd_consistency_rate is not None else '',
                        'ods_ads_consistency_rate': result.ods_ads_consistency_rate if result.ods_ads_consistency_rate is not None else '',
                        'dwd_ads_consistency_rate': result.dwd_ads_consistency_rate if result.dwd_ads_consistency_rate is not None else '',
                        'status': result.status,
                        'duration': f"{result.duration:.2f}",
                        'error': result.error if result.error else ''
                    }
                    writer.writerow(row)

            self.logger.info(f"结果已保存到: {csv_file}")
            print(f"\n📄 结果已保存到: {csv_file}")

        except Exception as e:
            self.logger.error(f"保存结果失败: {e}")
            print(f"❌ 保存结果失败: {e}")

    def print_summary(self) -> None:
        """打印统计摘要"""
        if not self.results:
            return

        total_protocols = len(self.results)
        success_count = sum(1 for r in self.results if r.status == 'SUCCESS')
        failed_count = total_protocols - success_count

        # 计算平均耗时
        avg_duration = sum(r.duration for r in self.results) / total_protocols if total_protocols > 0 else 0

        print(f"\n📊 统计摘要")
        print("="*50)
        print(f"总协议数: {total_protocols}")
        print(f"成功: {success_count}")
        print(f"失败: {failed_count}")
        print(f"成功率: {success_count/total_protocols*100:.1f}%")
        print(f"平均耗时: {avg_duration:.2f}秒")

        if failed_count > 0:
            print(f"\n❌ 失败的协议:")
            for result in self.results:
                if result.status != 'SUCCESS':
                    print(f"  - {result.protocol}: {result.error or '未知错误'}")


def main():
    """主函数"""
    try:
        # 可以通过命令行参数调整并发数
        max_workers = 5
        if len(sys.argv) > 1:
            try:
                max_workers = int(sys.argv[1])
                max_workers = max(1, min(max_workers, 20))  # 限制在1-20之间
            except ValueError:
                print("⚠️  并发数参数无效，使用默认值5")

        monitor = DataQualityMonitorODSConcurrent(max_workers=max_workers)
        monitor.run_comparison()
        monitor.save_results()
        monitor.print_summary()

    except KeyboardInterrupt:
        print("\n⚠️  用户中断执行")
    except Exception as e:
        print(f"❌ 程序执行失败: {e}")
        logging.error(f"程序执行失败: {e}")


if __name__ == "__main__":
    main()
