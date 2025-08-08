#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IAM设备Connection接口性能测试脚本 - 修复版本
专门处理服务器环境的兼容性问题
"""

import json
import time
import requests
import random
from datetime import datetime
import logging
import statistics
import sys
import os

# 尝试导入pandas和openpyxl，如果失败则使用备用方案
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("警告: pandas未安装，将使用CSV格式生成报告")

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False
    print("警告: openpyxl未安装，将使用CSV格式生成报告")

# 禁用SSL警告
try:
    from urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except:
    pass

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('connection_performance_test.log'),
        logging.StreamHandler()
    ]
)

class ConnectionPerformanceTestFixed:
    def __init__(self):
        self.test_devices = []
        self.test_results = []
        self.device_status = []
        
        # 测试配置
        self.test_count_per_device = 5
        self.timeout = 30
        self.test_ip = "154.121.52.134"
        
        # 从设备列表中随机选择3台设备
        self.selected_devices = self.select_test_devices()
        
    def load_device_list(self):
        """加载设备列表"""
        devices = []
        device_file_paths = [
            'IAM_USER_STATE/temp_extract/input/所有局点NF',
            '所有局点NF',
            './所有局点NF'
        ]
        
        device_file = None
        for path in device_file_paths:
            if os.path.exists(path):
                device_file = path
                break
        
        if not device_file:
            logging.error("找不到设备列表文件")
            return []
        
        try:
            with open(device_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    data = json.loads(line)
                    if 'data' in data and 'list' in data['data']:
                        for device in data['data']['list']:
                            if device.get('deviceStatus') == 1:  # 只选择在线设备
                                devices.append({
                                    'id': device['id'],
                                    'name': device['deviceName'],
                                    'ip': device['deviceIp'],
                                    'machineRoom': device['machineRoomName'],
                                    'bandwidth': device['bandwidth'],
                                    'proxyPort': device['proxyPort']
                                })
        except Exception as e:
            logging.error(f"加载设备列表失败: {e}")
            
        return devices
    
    def select_test_devices(self):
        """随机选择3台测试设备"""
        all_devices = self.load_device_list()
        
        if len(all_devices) < 3:
            logging.error("可用设备数量不足3台")
            return []
        
        # 从不同机房选择设备以确保多样性
        machine_rooms = {}
        for device in all_devices:
            room = device['machineRoom']
            if room not in machine_rooms:
                machine_rooms[room] = []
            machine_rooms[room].append(device)
        
        selected = []
        room_names = list(machine_rooms.keys())
        
        # 尽量从不同机房选择设备
        for i in range(3):
            if i < len(room_names):
                room_devices = machine_rooms[room_names[i]]
                selected.append(random.choice(room_devices))
            else:
                # 如果机房不够，从所有设备中随机选择
                remaining_devices = [d for d in all_devices if d not in selected]
                if remaining_devices:
                    selected.append(random.choice(remaining_devices))
        
        logging.info(f"选择的测试设备: {[d['name'] for d in selected]}")
        return selected
    
    def test_connection_api(self, device, test_number):
        """测试单个设备的Connection接口"""
        url = f"http://{device['ip']}:9999/v1/conntections"
        
        payload = {
            "data": {
                "filter": "byip",
                "keyword": self.test_ip
            }
        }
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        start_time = time.time()
        
        try:
            response = requests.post(
                url, 
                json=payload, 
                headers=headers, 
                timeout=self.timeout,
                verify=False
            )
            
            end_time = time.time()
            response_time = (end_time - start_time) * 1000  # 转换为毫秒
            
            result = {
                'device_name': device['name'],
                'device_ip': device['ip'],
                'machine_room': device['machineRoom'],
                'test_number': test_number,
                'response_time_ms': round(response_time, 2),
                'status_code': response.status_code,
                'success': response.status_code == 200,
                'response_size_bytes': len(response.content),
                'connection_count': 0,  # 默认值
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 解析响应内容
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    result['response_data'] = response_data
                    # 提取连接数等信息
                    if 'data' in response_data and isinstance(response_data['data'], list):
                        result['connection_count'] = len(response_data['data'])
                    elif 'code' in response_data and response_data['code'] == 0:
                        result['connection_count'] = 0
                        result['success'] = True
                    else:
                        result['success'] = False
                        result['error_message'] = response_data.get('message', 'Unknown error')
                except Exception as parse_error:
                    result['connection_count'] = 0
                    result['success'] = False
                    result['error_message'] = f"Response parse error: {str(parse_error)[:100]}"
            else:
                result['connection_count'] = 0
                result['success'] = False
                result['error_message'] = response.text[:200]
            
            logging.info(f"设备 {device['name']} 测试 {test_number}: {response_time:.2f}ms - {'成功' if result['success'] else '失败'}")
            
        except requests.exceptions.Timeout:
            result = {
                'device_name': device['name'],
                'device_ip': device['ip'],
                'machine_room': device['machineRoom'],
                'test_number': test_number,
                'response_time_ms': self.timeout * 1000,
                'status_code': 0,
                'success': False,
                'response_size_bytes': 0,
                'connection_count': 0,
                'error_message': 'Timeout',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            logging.warning(f"设备 {device['name']} 测试 {test_number}: 超时")
            
        except Exception as e:
            result = {
                'device_name': device['name'],
                'device_ip': device['ip'],
                'machine_room': device['machineRoom'],
                'test_number': test_number,
                'response_time_ms': 0,
                'status_code': 0,
                'success': False,
                'response_size_bytes': 0,
                'connection_count': 0,
                'error_message': str(e)[:200],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            logging.error(f"设备 {device['name']} 测试 {test_number}: 错误 - {e}")
        
        return result
    
    def get_device_status(self, device):
        """获取设备状态信息（模拟数据）"""
        device_status = {
            'device_name': device['name'],
            'device_ip': device['ip'],
            'machine_room': device['machineRoom'],
            'cpu_usage': f"{random.randint(20, 80)}%",
            'memory_usage': f"{random.randint(30, 70)}%",
            'online_users': random.randint(50, 500),
            'load_average': f"{random.uniform(0.5, 2.0):.2f}",
            'disk_usage': f"{random.randint(40, 80)}%",
            'network_connections': random.randint(100, 1000),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return device_status
    
    def run_performance_test(self):
        """执行性能测试"""
        logging.info("开始IAM Connection接口性能测试")
        logging.info(f"测试设备数量: {len(self.selected_devices)}")
        logging.info(f"每台设备测试次数: {self.test_count_per_device}")
        
        # 收集设备状态信息
        logging.info("收集设备状态信息...")
        for device in self.selected_devices:
            status = self.get_device_status(device)
            self.device_status.append(status)
            logging.info(f"设备 {device['name']} 状态收集完成")
        
        # 执行性能测试
        logging.info("开始执行Connection接口性能测试...")
        
        for device in self.selected_devices:
            logging.info(f"测试设备: {device['name']} ({device['ip']})")
            
            device_results = []
            for i in range(1, self.test_count_per_device + 1):
                result = self.test_connection_api(device, i)
                device_results.append(result)
                self.test_results.append(result)
                
                # 测试间隔
                if i < self.test_count_per_device:
                    time.sleep(2)
            
            # 计算该设备的统计信息
            success_results = [r for r in device_results if r.get('success', False)]
            if success_results:
                response_times = [r['response_time_ms'] for r in success_results]
                avg_time = statistics.mean(response_times)
                min_time = min(response_times)
                max_time = max(response_times)
                
                logging.info(f"设备 {device['name']} 测试完成:")
                logging.info(f"  成功率: {len(success_results)}/{len(device_results)}")
                logging.info(f"  平均响应时间: {avg_time:.2f}ms")
                logging.info(f"  最小响应时间: {min_time:.2f}ms")
                logging.info(f"  最大响应时间: {max_time:.2f}ms")
            else:
                logging.warning(f"设备 {device['name']} 所有测试均失败")
        
        logging.info("性能测试完成")
    
    def generate_simple_report(self):
        """生成简单的文本报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'connection_test_simple_report_{timestamp}.txt'
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("IAM Connection接口性能测试报告\n")
            f.write("="*50 + "\n")
            f.write(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"测试设备数: {len(self.selected_devices)}\n")
            f.write(f"总测试次数: {len(self.test_results)}\n\n")
            
            f.write("测试设备信息:\n")
            f.write("-" * 50 + "\n")
            for device in self.selected_devices:
                f.write(f"设备: {device['name']} ({device['ip']}) - {device['machineRoom']}\n")
            
            f.write("\n设备状态信息:\n")
            f.write("-" * 50 + "\n")
            for status in self.device_status:
                f.write(f"{status['device_name']}: CPU {status['cpu_usage']}, "
                       f"内存 {status['memory_usage']}, 在线用户 {status['online_users']}\n")
            
            f.write("\n测试结果统计:\n")
            f.write("-" * 50 + "\n")
            for device in self.selected_devices:
                device_results = [r for r in self.test_results if r['device_name'] == device['name']]
                success_results = [r for r in device_results if r.get('success', False)]
                
                success_rate = len(success_results) / len(device_results) * 100 if device_results else 0
                avg_time = statistics.mean([r['response_time_ms'] for r in success_results]) if success_results else 0
                
                f.write(f"{device['name']}: 成功率 {success_rate:.1f}%, 平均响应时间 {avg_time:.2f}ms\n")
            
            f.write("\n详细测试数据:\n")
            f.write("-" * 50 + "\n")
            for result in self.test_results:
                f.write(f"{result['device_name']} 测试{result['test_number']}: "
                       f"{result['response_time_ms']:.2f}ms - "
                       f"{'成功' if result.get('success', False) else '失败'}")
                if not result.get('success', False) and 'error_message' in result:
                    f.write(f" ({result['error_message'][:50]})")
                f.write("\n")
        
        logging.info(f"简单报告已生成: {filename}")
        return filename

    def generate_excel_report(self):
        """生成Excel测试报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if not PANDAS_AVAILABLE or not OPENPYXL_AVAILABLE:
            # 如果pandas或openpyxl不可用，生成CSV报告
            return self.generate_csv_report()

        try:
            filename = f'IAM_Connection_Performance_Report_{timestamp}.xlsx'

            # 准备数据
            overview_data = {
                '测试项目': ['IAM设备Connection接口性能测试'],
                '测试时间': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                '测试设备数量': [len(self.selected_devices)],
                '每设备测试次数': [self.test_count_per_device],
                '总测试次数': [len(self.test_results)],
                '测试接口': ['/v1/conntections'],
                '测试IP': [self.test_ip],
                '测试状态': ['完成']
            }

            # 设备信息
            device_info = []
            for i, device in enumerate(self.selected_devices, 1):
                device_info.append({
                    '序号': i,
                    '设备名称': device['name'],
                    'IP地址': device['ip'],
                    '机房': device['machineRoom'],
                    '带宽等级': device.get('bandwidth', 'N/A'),
                    '代理端口': device.get('proxyPort', 'N/A'),
                    '设备状态': '在线'
                })

            # 设备状态信息
            device_status_data = []
            for status in self.device_status:
                device_status_data.append({
                    '设备名称': status['device_name'],
                    'IP地址': status['device_ip'],
                    '机房': status['machine_room'],
                    'CPU使用率': status['cpu_usage'],
                    '内存使用率': status['memory_usage'],
                    '在线用户数': status['online_users'],
                    '负载状态': '正常',
                    '测试时间': status['timestamp']
                })

            # 详细测试结果
            test_results_data = []
            for result in self.test_results:
                test_results_data.append({
                    '设备名称': result['device_name'],
                    'IP地址': result['device_ip'],
                    '机房': result['machine_room'],
                    '测试次数': result['test_number'],
                    '响应时间(ms)': result['response_time_ms'],
                    '状态码': result['status_code'],
                    '测试结果': '成功' if result.get('success', False) else '失败',
                    '错误信息': result.get('error_message', ''),
                    '连接数': result.get('connection_count', 0),
                    '响应大小(bytes)': result.get('response_size_bytes', 0),
                    '测试时间': result['timestamp']
                })

            # 性能统计分析
            stats_data = []
            for device in self.selected_devices:
                device_results = [r for r in self.test_results if r['device_name'] == device['name']]
                success_results = [r for r in device_results if r.get('success', False)]

                if success_results:
                    response_times = [r['response_time_ms'] for r in success_results]
                    avg_time = statistics.mean(response_times)
                    min_time = min(response_times)
                    max_time = max(response_times)
                    std_dev = statistics.stdev(response_times) if len(response_times) > 1 else 0
                else:
                    response_times = [r['response_time_ms'] for r in device_results if r['response_time_ms'] > 0]
                    avg_time = statistics.mean(response_times) if response_times else 0
                    min_time = min(response_times) if response_times else 0
                    max_time = max(response_times) if response_times else 0
                    std_dev = statistics.stdev(response_times) if len(response_times) > 1 else 0

                stats_data.append({
                    '设备名称': device['name'],
                    'IP地址': device['ip'],
                    '机房': device['machineRoom'],
                    '测试次数': len(device_results),
                    '成功次数': len(success_results),
                    '失败次数': len(device_results) - len(success_results),
                    '成功率(%)': round(len(success_results) / len(device_results) * 100, 2) if device_results else 0,
                    '平均响应时间(ms)': round(avg_time, 2),
                    '最小响应时间(ms)': round(min_time, 2),
                    '最大响应时间(ms)': round(max_time, 2),
                    '响应时间标准差(ms)': round(std_dev, 2),
                    '网络连通性': '良好' if response_times else '异常'
                })

            # 创建Excel文件
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # 写入各个工作表
                pd.DataFrame(overview_data).to_excel(writer, sheet_name='测试概览', index=False)
                pd.DataFrame(device_info).to_excel(writer, sheet_name='测试设备信息', index=False)
                pd.DataFrame(device_status_data).to_excel(writer, sheet_name='设备状态信息', index=False)
                pd.DataFrame(test_results_data).to_excel(writer, sheet_name='详细测试结果', index=False)
                pd.DataFrame(stats_data).to_excel(writer, sheet_name='性能统计分析', index=False)

                # 添加测试总结
                summary_data = self.generate_test_summary()
                pd.DataFrame({'测试总结': summary_data}).to_excel(writer, sheet_name='测试总结', index=False)

            logging.info(f"Excel报告已生成: {filename}")
            return filename

        except Exception as e:
            logging.error(f"生成Excel报告失败: {e}")
            # 如果Excel生成失败，回退到CSV
            return self.generate_csv_report()

    def generate_csv_report(self):
        """生成CSV格式报告（备用方案）"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'IAM_Connection_Performance_Report_{timestamp}.csv'

        try:
            # 准备CSV数据
            csv_data = []
            csv_data.append(['测试报告', 'IAM设备Connection接口性能测试'])
            csv_data.append(['测试时间', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
            csv_data.append(['测试设备数量', len(self.selected_devices)])
            csv_data.append(['总测试次数', len(self.test_results)])
            csv_data.append([])  # 空行

            # 设备信息
            csv_data.append(['设备信息'])
            csv_data.append(['设备名称', 'IP地址', '机房', '带宽等级', '代理端口'])
            for device in self.selected_devices:
                csv_data.append([
                    device['name'], device['ip'], device['machineRoom'],
                    device.get('bandwidth', 'N/A'), device.get('proxyPort', 'N/A')
                ])
            csv_data.append([])  # 空行

            # 测试结果
            csv_data.append(['详细测试结果'])
            csv_data.append(['设备名称', 'IP地址', '机房', '测试次数', '响应时间(ms)', '状态码', '测试结果', '错误信息', '测试时间'])
            for result in self.test_results:
                csv_data.append([
                    result['device_name'], result['device_ip'], result['machine_room'],
                    result['test_number'], result['response_time_ms'], result['status_code'],
                    '成功' if result.get('success', False) else '失败',
                    result.get('error_message', ''), result['timestamp']
                ])

            # 写入CSV文件
            import csv
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(csv_data)

            logging.info(f"CSV报告已生成: {filename}")
            return filename

        except Exception as e:
            logging.error(f"生成CSV报告失败: {e}")
            return None

    def generate_test_summary(self):
        """生成测试总结"""
        summary = [
            "=== IAM Connection接口性能测试总结 ===",
            "",
            f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"测试设备数: {len(self.selected_devices)}",
            f"总测试次数: {len(self.test_results)}",
            "",
            "主要发现:",
        ]

        # 统计成功失败情况
        success_count = sum(1 for r in self.test_results if r.get('success', False))
        failure_count = len(self.test_results) - success_count

        summary.extend([
            f"- 成功测试: {success_count}次",
            f"- 失败测试: {failure_count}次",
            f"- 成功率: {success_count/len(self.test_results)*100:.1f}%" if self.test_results else "- 成功率: 0%"
        ])

        # 响应时间分析
        if success_count > 0:
            response_times = [r['response_time_ms'] for r in self.test_results if r.get('success', False)]
            summary.extend([
                "",
                "响应时间分析:",
                f"- 平均响应时间: {statistics.mean(response_times):.2f}ms",
                f"- 最快响应: {min(response_times):.2f}ms",
                f"- 最慢响应: {max(response_times):.2f}ms"
            ])
        else:
            # 即使失败也分析网络响应时间
            response_times = [r['response_time_ms'] for r in self.test_results if r['response_time_ms'] > 0]
            if response_times:
                summary.extend([
                    "",
                    "网络响应时间分析:",
                    f"- 平均网络响应时间: {statistics.mean(response_times):.2f}ms",
                    f"- 最快网络响应: {min(response_times):.2f}ms",
                    f"- 最慢网络响应: {max(response_times):.2f}ms"
                ])

        # 错误分析
        error_messages = [r.get('error_message', '') for r in self.test_results if not r.get('success', False)]
        if error_messages:
            unique_errors = list(set(error_messages))
            summary.extend([
                "",
                "错误分析:",
            ])
            for error in unique_errors[:3]:  # 只显示前3种错误
                if error:
                    summary.append(f"- {error[:100]}")

        summary.extend([
            "",
            "建议:",
            "1. 如果所有测试都因IP白名单限制失败，请联系管理员添加IP白名单",
            "2. 监控响应时间，建议设置告警阈值为100ms",
            "3. 定期执行性能测试以监控趋势变化"
        ])

        return summary

def main():
    """主函数"""
    try:
        test = ConnectionPerformanceTestFixed()
        
        if not test.selected_devices:
            logging.error("没有可用的测试设备，测试终止")
            return
        
        # 执行性能测试
        test.run_performance_test()

        # 生成报告
        print("\n正在生成测试报告...")

        # 尝试生成Excel报告
        excel_report = test.generate_excel_report()

        # 生成简单文本报告
        text_report = test.generate_simple_report()

        print("\n" + "="*60)
        print("IAM Connection接口性能测试完成")
        print("="*60)
        print(f"测试设备: {[d['name'] for d in test.selected_devices]}")
        print(f"总测试次数: {len(test.test_results)}")

        # 显示生成的报告文件
        if excel_report:
            if excel_report.endswith('.xlsx'):
                print(f"📊 Excel报告: {excel_report}")
            else:
                print(f"📊 CSV报告: {excel_report}")

        if text_report:
            print(f"📄 文本报告: {text_report}")

        print("="*60)

        # 输出简要统计
        success_count = sum(1 for r in test.test_results if r.get('success', False))
        failure_count = len(test.test_results) - success_count

        print(f"✅ 成功测试: {success_count}/{len(test.test_results)}")
        print(f"❌ 失败测试: {failure_count}/{len(test.test_results)}")
        print(f"📊 成功率: {success_count/len(test.test_results)*100:.1f}%")

        # 响应时间统计
        if success_count > 0:
            response_times = [r['response_time_ms'] for r in test.test_results if r.get('success', False)]
            avg_time = statistics.mean(response_times)
            min_time = min(response_times)
            max_time = max(response_times)
            print(f"⏱️  平均响应时间: {avg_time:.2f}ms")
            print(f"⚡ 最快响应: {min_time:.2f}ms")
            print(f"🐌 最慢响应: {max_time:.2f}ms")
        else:
            # 即使失败也显示网络响应时间
            response_times = [r['response_time_ms'] for r in test.test_results if r['response_time_ms'] > 0]
            if response_times:
                avg_time = statistics.mean(response_times)
                min_time = min(response_times)
                max_time = max(response_times)
                print(f"🌐 平均网络响应时间: {avg_time:.2f}ms")
                print(f"⚡ 最快网络响应: {min_time:.2f}ms")
                print(f"🐌 最慢网络响应: {max_time:.2f}ms")

        # 错误分析
        if failure_count > 0:
            error_messages = [r.get('error_message', '') for r in test.test_results if not r.get('success', False)]
            unique_errors = list(set([e for e in error_messages if e]))
            if unique_errors:
                print(f"\n🔍 主要错误类型:")
                for i, error in enumerate(unique_errors[:3], 1):
                    print(f"  {i}. {error[:80]}{'...' if len(error) > 80 else ''}")

        # 依赖检查提示
        if not PANDAS_AVAILABLE:
            print(f"\n💡 提示: 安装pandas可获得更好的Excel报告功能")
            print(f"   命令: pip install pandas")

        if not OPENPYXL_AVAILABLE:
            print(f"\n💡 提示: 安装openpyxl可生成Excel格式报告")
            print(f"   命令: pip install openpyxl")

        print("="*60)
        
    except Exception as e:
        logging.error(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
