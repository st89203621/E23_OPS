"""
快速提取DWD SQL文件中的ODS表映射关系
"""
import os
import re
from typing import Dict, List

def extract_ods_tables_from_dwd_sql(sql_file_path: str) -> List[str]:
    """从DWD SQL文件中提取ODS表名"""
    ods_tables = []
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 查找所有ods_pr_source表名
        patterns = [
            r'FROM\s+(ods_pr_source_\w+)',
            r'CREATE TABLE\s+(ods_pr_source_\w+)',
            r'topic.*=.*[\'\"](ods_pr_source_\w+)[\'\"]'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            ods_tables.extend(matches)
            
        # 去重并返回
        return list(set(ods_tables))
        
    except Exception as e:
        print(f"处理文件 {sql_file_path} 时出错: {e}")
        return []

def main():
    """主函数"""
    dwd_sql_dir = "../../ODS模型/DWD天基SQL/norm"
    
    # 协议映射
    protocol_mapping = {
        'dwd_pr_email.sql': 'email',
        'dwd_pr_http.sql': 'http', 
        'dwd_pr_im.sql': 'im',
        'dwd_pr_entertainment.sql': 'entertainment',
        'dwd_pr_engine.sql': 'engine',
        'dwd_pr_finance.sql': 'finance',
        'dwd_pr_ftp.sql': 'ftp',
        'dwd_pr_lbs.sql': 'lbs',
        'dwd_pr_multimedia.sql': 'multimedia',
        'dwd_pr_news.sql': 'news',
        'dwd_pr_others.sql': 'others',
        'dwd_pr_remotectrl.sql': 'remotectrl',
        'dwd_pr_shopping.sql': 'shopping',
        'dwd_pr_sns.sql': 'sns',
        'dwd_pr_telnet.sql': 'telnet',
        'dwd_pr_terminal.sql': 'terminal',
        'dwd_pr_tool.sql': 'tool',
        'dwd_pr_travel.sql': 'travel',
        'dwd_pr_vpn.sql': 'vpn'
    }
    
    print("ODS表映射关系分析结果:")
    print("="*60)
    
    for sql_file, protocol in protocol_mapping.items():
        sql_path = os.path.join(dwd_sql_dir, sql_file)
        if os.path.exists(sql_path):
            ods_tables = extract_ods_tables_from_dwd_sql(sql_path)
            if ods_tables:
                print(f"{protocol}:")
                for table in ods_tables:
                    print(f"  - {table}")
            else:
                print(f"{protocol}: 未找到ODS表")
        else:
            print(f"{protocol}: SQL文件不存在")
        print()

if __name__ == "__main__":
    main()
