"""
创建完整的ODS表映射关系
基于ODS模型文件和实际数据库表结构
"""
import yaml
import os
from ods_client import ODSClient

def get_all_ods_tables():
    """获取所有ODS表"""
    config_path = "config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    ods_config = config['hive_ods']
    
    try:
        with ODSClient(
            host=ods_config['host'],
            port=ods_config['port'],
            username=ods_config.get('username'),
            password=ods_config.get('password'),
            database=ods_config['database'],
            auth=ods_config.get('auth', 'PLAIN')
        ) as ods_client:
            
            tables = ods_client.execute_query("SHOW TABLES")
            if tables:
                ods_tables = [table[0] for table in tables if table[0].startswith('ods_')]
                return sorted(ods_tables)
            return []
    except Exception as e:
        print(f"获取ODS表失败: {e}")
        return []

def create_protocol_mapping():
    """创建完整的协议映射"""
    
    # 获取实际的ODS表
    print("正在获取ODS数据库中的实际表...")
    ods_tables = get_all_ods_tables()
    print(f"找到 {len(ods_tables)} 个ODS表")
    
    # 29个协议的完整映射
    protocol_mappings = {
        # PR协议 (20个)
        "email": {
            "ods_candidates": ["ods_pr_email_store"],
            "dwd_table": "dwd_pr_email_store",
            "es_pattern": "deye_v64_email_{year}{month:02d}-*"
        },
        "engine": {
            "ods_candidates": ["ods_pr_engine_store"],
            "dwd_table": "dwd_pr_engine_store", 
            "es_pattern": "deye_v64_engine_{year}{month:02d}-*"
        },
        "entertainment": {
            "ods_candidates": ["ods_pr_entertainment_store", "ods_pr_game_store", "ods_pr_live_store"],
            "dwd_table": "dwd_pr_entertainment_store",
            "es_pattern": "deye_v64_entertainment_{year}{month:02d}-*"
        },
        "finance": {
            "ods_candidates": ["ods_pr_finance_store", "ods_pr_financial_store", "ods_pr_payment_store", "ods_pr_bank_store"],
            "dwd_table": "dwd_pr_finance_store",
            "es_pattern": "deye_v64_finance_{year}{month:02d}-*"
        },
        "ftp": {
            "ods_candidates": ["ods_pr_ftp_store"],
            "dwd_table": "dwd_pr_ftp_store",
            "es_pattern": "deye_v64_ftp_{year}{month:02d}-*"
        },
        "http": {
            "ods_candidates": ["ods_pr_http_store"],
            "dwd_table": "dwd_pr_http_store",
            "es_pattern": "deye_v64_http_{year}{month:02d}-*"
        },
        "im": {
            "ods_candidates": ["ods_pr_im_store"],
            "dwd_table": "dwd_pr_im_store",
            "es_pattern": "deye_v64_im_{year}{month:02d}-*"
        },
        "lbs": {
            "ods_candidates": ["ods_pr_lbs_store"],
            "dwd_table": "dwd_pr_lbs_store",
            "es_pattern": "deye_v64_lbs_{year}{month:02d}-*"
        },
        "multimedia": {
            "ods_candidates": ["ods_pr_multimedia_store", "ods_pr_video_store", "ods_pr_picture_store", "ods_pr_voice_store"],
            "dwd_table": "dwd_pr_multimedia_store",
            "es_pattern": "deye_v64_multimedia_{year}{month:02d}-*"
        },
        "news": {
            "ods_candidates": ["ods_pr_news_store"],
            "dwd_table": "dwd_pr_news_store",
            "es_pattern": "deye_v64_news_{year}{month:02d}-*"
        },
        "others": {
            "ods_candidates": ["ods_pr_others_store", "ods_pr_other_store", "ods_pr_weblogin_store"],
            "dwd_table": "dwd_pr_others_store",
            "es_pattern": "deye_v64_other_{year}{month:02d}-*"
        },
        "remotectrl": {
            "ods_candidates": ["ods_pr_remotectrl_store"],
            "dwd_table": "dwd_pr_remotectrl_store",
            "es_pattern": "deye_v64_remotectrl_{year}{month:02d}-*"
        },
        "shopping": {
            "ods_candidates": ["ods_pr_shopping_store", "ods_pr_shop_store"],
            "dwd_table": "dwd_pr_shopping_store",
            "es_pattern": "deye_v64_shopping_{year}{month:02d}-*"
        },
        "sns": {
            "ods_candidates": ["ods_pr_sns_store"],
            "dwd_table": "dwd_pr_sns_store",
            "es_pattern": "deye_v64_sns_{year}{month:02d}-*"
        },
        "telnet": {
            "ods_candidates": ["ods_pr_telnet_store"],
            "dwd_table": "dwd_pr_telnet_store",
            "es_pattern": "deye_v64_telnet_{year}{month:02d}-*"
        },
        "terminal": {
            "ods_candidates": ["ods_pr_terminal_store"],
            "dwd_table": "dwd_pr_terminal_store",
            "es_pattern": "deye_v64_terminal_{year}{month:02d}-*"
        },
        "tool": {
            "ods_candidates": ["ods_pr_tool_store", "ods_pr_p2p_store"],
            "dwd_table": "dwd_pr_tool_store",
            "es_pattern": "deye_v64_tool_{year}{month:02d}-*"
        },
        "travel": {
            "ods_candidates": ["ods_pr_travel_store", "ods_pr_map_store", "ods_pr_navigation_store"],
            "dwd_table": "dwd_pr_travel_store",
            "es_pattern": "deye_v64_travel_{year}{month:02d}-*"
        },
        "voip": {
            "ods_candidates": ["ods_pr_voip_store"],
            "dwd_table": "dwd_pr_voip_store",
            "es_pattern": "deye_v64_voip_{year}{month:02d}-*"
        },
        "vpn": {
            "ods_candidates": ["ods_pr_vpn_store"],
            "dwd_table": "dwd_pr_vpn_store",
            "es_pattern": "deye_v64_vpn_{year}{month:02d}-*"
        },
        
        # 基础通信协议 (3个)
        "call": {
            "ods_candidates": ["ods_call_store"],
            "dwd_table": "dwd_call_store",
            "es_pattern": "deye_v64_call_{year}{month:02d}-*"
        },
        "fax": {
            "ods_candidates": ["ods_fax_store", "ods_pr_voip_fax_store"],
            "dwd_table": "dwd_fax_store",
            "es_pattern": "deye_v64_fax_{year}{month:02d}-*"
        },
        "sms": {
            "ods_candidates": ["ods_sms_store"],
            "dwd_table": "dwd_sms_store",
            "es_pattern": "deye_v64_sms_{year}{month:02d}-*"
        },
        
        # Radius协议 (6个)
        "fixednetradius": {
            "ods_candidates": ["ods_fixnet_radius_store"],
            "dwd_table": "dwd_fixnet_radius_store",
            "es_pattern": "deye_v64_fixednetradius_{year}{month:02d}-*"
        },
        "location_mobilis": {
            "ods_candidates": ["ods_mobilenet_location_mobilis_store"],
            "dwd_table": "dwd_mobilenet_location_mobilis_store",
            "es_pattern": "deye_v64_location_mobilis_{year}{month:02d}-*"
        },
        "location_ooredoo": {
            "ods_candidates": ["ods_mobilenet_location_ooredoo_store"],
            "dwd_table": "dwd_mobilenet_location_ooredoo_store",
            "es_pattern": "deye_v64_location_ooredoo_{year}{month:02d}-*"
        },
        "mobilenetradius_djezzy": {
            "ods_candidates": ["ods_mobilenet_radius_djezzy_store"],
            "dwd_table": "dwd_mobile_radius_djezzy_store",
            "es_pattern": "deye_v64_mobilenetradius_djezzy_{year}{month:02d}-*"
        },
        "mobilenetradius_mobilis": {
            "ods_candidates": ["ods_mobilenet_radius_mobilis_store"],
            "dwd_table": "dwd_mobile_radius_mobilis_store",
            "es_pattern": "deye_v64_mobilenetradius_mobilis_{year}{month:02d}-*"
        },
        "mobilenetradius_ooredoo": {
            "ods_candidates": ["ods_mobilenet_radius_ooredoo_store"],
            "dwd_table": "dwd_mobile_radius_ooredoo_store",
            "es_pattern": "deye_v64_mobilenetradius_ooredoo_{year}{month:02d}-*"
        }
    }
    
    # 匹配实际存在的ODS表
    final_mapping = {}
    
    for protocol, config in protocol_mappings.items():
        # 查找存在的ODS表
        existing_ods_tables = []
        for candidate in config["ods_candidates"]:
            if candidate in ods_tables:
                existing_ods_tables.append(candidate)
        
        if existing_ods_tables:
            # 如果有多个表，用逗号分隔（支持多表合并）
            ods_table = ",".join(existing_ods_tables)
        else:
            # 如果没有找到，使用第一个候选表名（会在运行时报错，但保持配置完整）
            ods_table = config["ods_candidates"][0]
            print(f"⚠️  协议 {protocol} 的ODS表不存在: {config['ods_candidates']}")
        
        final_mapping[protocol] = {
            "ods_table": ods_table,
            "dwd_table": config["dwd_table"],
            "es_index_pattern": config["es_pattern"],
            "es_date_field": "capture_dayField",
            "date_field": "capture_day"
        }
    
    return final_mapping

def main():
    """主函数"""
    print("🔧 创建完整的协议映射关系...")
    
    mapping = create_protocol_mapping()
    
    print(f"\n📊 协议映射结果 (共 {len(mapping)} 个协议):")
    print("="*80)
    
    for protocol, config in mapping.items():
        print(f"\n{protocol}:")
        print(f"  ODS: {config['ods_table']}")
        print(f"  DWD: {config['dwd_table']}")
        print(f"  ES:  {config['es_index_pattern']}")
    
    # 生成新的配置文件内容
    print(f"\n📝 生成配置文件...")
    
    # 读取现有配置
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 更新协议配置
    config['protocols'] = mapping
    
    # 保存到新文件
    with open('config_complete.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True, indent=2)
    
    print(f"✅ 完整配置已保存到: config_complete.yaml")
    print(f"✅ 包含所有 {len(mapping)} 个协议的完整映射关系")

if __name__ == "__main__":
    main()
