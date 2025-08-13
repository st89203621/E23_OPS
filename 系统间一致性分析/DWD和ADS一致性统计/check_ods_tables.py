"""
检查ODS数据库中实际存在的表
"""
import yaml
from ods_client import ODSClient

def main():
    """主函数"""
    # 加载配置
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    ods_config = config['hive_ods']
    
    print("🔍 检查ODS数据库中的实际表...")
    print("="*60)
    
    try:
        with ODSClient(
            host=ods_config['host'],
            port=ods_config['port'],
            username=ods_config.get('username'),
            password=ods_config.get('password'),
            database=ods_config['database'],
            auth=ods_config.get('auth', 'PLAIN')
        ) as ods_client:
            
            # 查看所有表
            print("📋 查询所有表...")
            tables = ods_client.execute_query("SHOW TABLES")
            
            if tables:
                print(f"✅ 找到 {len(tables)} 个表:")
                
                # 按类型分组显示
                ods_tables = []
                dwd_tables = []
                other_tables = []
                
                for table in tables:
                    table_name = table[0]
                    if table_name.startswith('ods_'):
                        ods_tables.append(table_name)
                    elif table_name.startswith('dwd_'):
                        dwd_tables.append(table_name)
                    else:
                        other_tables.append(table_name)
                
                print(f"\n📊 ODS表 ({len(ods_tables)}个):")
                for table in sorted(ods_tables)[:20]:  # 显示前20个
                    print(f"  - {table}")
                if len(ods_tables) > 20:
                    print(f"  ... 还有 {len(ods_tables) - 20} 个表")
                
                print(f"\n📊 DWD表 ({len(dwd_tables)}个):")
                for table in sorted(dwd_tables)[:10]:  # 显示前10个
                    print(f"  - {table}")
                if len(dwd_tables) > 10:
                    print(f"  ... 还有 {len(dwd_tables) - 10} 个表")
                
                print(f"\n📊 其他表 ({len(other_tables)}个):")
                for table in sorted(other_tables)[:10]:  # 显示前10个
                    print(f"  - {table}")
                if len(other_tables) > 10:
                    print(f"  ... 还有 {len(other_tables) - 10} 个表")
                
                # 查找与协议相关的ODS表
                print(f"\n🔍 查找协议相关的ODS表:")
                protocol_keywords = ['email', 'http', 'im', 'ftp', 'engine', 'entertainment', 'finance']
                
                for keyword in protocol_keywords:
                    matching_tables = [t for t in ods_tables if keyword in t.lower()]
                    if matching_tables:
                        print(f"  {keyword}: {matching_tables}")
                
            else:
                print("❌ 无法获取表列表")
                
    except Exception as e:
        print(f"❌ 连接ODS数据库失败: {e}")

if __name__ == "__main__":
    main()
