#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查DWD数据库中的实际表
"""

import yaml
from hive_client import HiveClient

def main():
    print("🔍 检查DWD数据库中的实际表...")
    print("=" * 60)
    
    # 读取配置
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 连接DWD数据库
    dwd_config = config['hive_dwd']
    dwd_client = HiveClient(
        host=dwd_config['host'],
        port=dwd_config['port'],
        username=dwd_config.get('username'),
        password=dwd_config.get('password'),
        database=dwd_config['database'],
        auth=dwd_config.get('auth', 'PLAIN')
    )
    
    try:
        # 连接数据库
        if not dwd_client.connect():
            print("❌ 无法连接到DWD数据库")
            return

        print("📋 查询所有DWD表...")
        # 查询所有表
        results = dwd_client.execute_query("SHOW TABLES")
        if not results:
            print("❌ 无法获取表列表")
            return

        tables = [row[0] for row in results]
        print(f"✅ 找到 {len(tables)} 个表:")
        
        # 分类显示表
        dwd_tables = []
        other_tables = []
        
        for table in sorted(tables):
            if table.startswith('dwd_'):
                dwd_tables.append(table)
            else:
                other_tables.append(table)
        
        print(f"\n📊 DWD表 ({len(dwd_tables)}个):")
        for table in dwd_tables[:20]:  # 显示前20个
            print(f"  - {table}")
        if len(dwd_tables) > 20:
            print(f"  ... 还有 {len(dwd_tables) - 20} 个表")
        
        print(f"\n📊 其他表 ({len(other_tables)}个):")
        for table in other_tables[:10]:  # 显示前10个
            print(f"  - {table}")
        if len(other_tables) > 10:
            print(f"  ... 还有 {len(other_tables) - 10} 个表")
        
        # 检查配置文件中的DWD表是否存在
        print(f"\n🔍 检查配置文件中的DWD表:")
        print("=" * 60)
        
        missing_tables = []
        existing_tables = []
        
        for protocol, config_info in config['protocols'].items():
            dwd_table = config_info['dwd_table']
            if dwd_table in tables:
                existing_tables.append((protocol, dwd_table))
                print(f"✅ {protocol}: {dwd_table}")
            else:
                missing_tables.append((protocol, dwd_table))
                print(f"❌ {protocol}: {dwd_table} (不存在)")
        
        print(f"\n📊 统计结果:")
        print(f"  - 存在的DWD表: {len(existing_tables)} 个")
        print(f"  - 缺失的DWD表: {len(missing_tables)} 个")
        
        if missing_tables:
            print(f"\n⚠️  缺失的DWD表列表:")
            for protocol, table in missing_tables:
                print(f"  - {protocol}: {table}")
        
        # 查找协议相关的DWD表
        print(f"\n🔍 查找协议相关的DWD表:")
        protocol_keywords = ['pr_', 'call', 'sms', 'fax', 'radius', 'location', 'mobile']
        
        for keyword in protocol_keywords:
            matching_tables = [t for t in dwd_tables if keyword in t]
            if matching_tables:
                print(f"  {keyword}: {matching_tables[:5]}")  # 显示前5个匹配的表
        
    except Exception as e:
        print(f"❌ 错误: {e}")
    finally:
        dwd_client.disconnect()

if __name__ == "__main__":
    main()
