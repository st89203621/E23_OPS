import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple

def determine_site_name(comment: str) -> str:
    """
    根据评论内容判断站点名称

    参数:
        comment (str): 包含站点信息的评论字符串

    返回:
        str: 对应的站点名称
    """
    comment = comment.lower()  # 转换为小写方便比较

    if 'djemila' in comment:
        return 'A3-1'
    elif 'ca2 ngis1' in comment:
        return 'A2-1'
    elif 'ca2 ngis2' in comment:
        return 'A2-2'
    elif 'annaba ngis1' in comment:
        return 'B1-1'
    elif 'annaba ngis2' in comment:
        return 'B1-2'
    elif 'oran' in comment:
        return 'C1-1'
    else:
        return comment.upper()  # 或者可以抛出异常 raise ValueError("无法识别的站点")


def parse_xml(xml_path):
    # 解析XML文件
    tree = ET.parse(xml_path)
    root = tree.getroot()
    port_configs = []

    # 遍历XML中的每个节点
    for node in root.findall('node'):
        # 获取comment作为node字段值
        site_comment = node.get('comment', '')
        site = determine_site_name(site_comment)

        port_pair = node.find('portPair')

        for port in port_pair.findall('port'):
            up = port.find('up').text if port.find('up') is not None else ''
            down = port.find('down').text if port.find('down') is not None else ''
            weight = port.find('weight').text if port.find('weight') is not None else ''
            server_ip = port.find('nfServerIP').text if port.find('nfServerIP') is not None else ''

            port_configs.append({
                'site': site,
                'port': up,
                'port_type': 'up',
                'weight': int(weight),
                'server_ip': server_ip
            })

            port_configs.append({
                'site': site,
                'port': down,
                'port_type': 'down',
                'weight': int(weight),
                'server_ip': server_ip
            })

    return port_configs

def save_to_csv(data: List[Dict], output_file: str):
    """将结果保存为CSV文件"""
    import csv
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['site', 'port', 'port_type', 'weight', 'server_ip']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def exist_config(port_config):
    print(f"site: {port_config['site']}, port: {port_config['port']}, port_type: {port_config['port_type']}, weight: {port_config['weight']}, server_ip: {port_config['nfServerIP']}")
    # write_to_mysql(port_config)


if __name__ == "__main__":
    xml_file = "nf.xml"
    csv_file = "nf.csv"

    try:
        port_configs = parse_xml(xml_file)

        current_site_weights = 0
        current_site = 'A2-1'
        count = 0
        index = 0

        for port_config in port_configs:
            site = port_config['site']
            port = port_config['port']
            port_type = port_config['port_type']
            weight = port_config['weight']

            if current_site != site:
                if count > 0:
                    print(f"1. site: {current_site}, count: {count}, current_site_weights: {current_site_weights}")
                current_site = site
                current_site_weights = 0
                count = 0
            # else:
            # print(f"1.2 site: {site}, count: {count}, current_site_weights: {current_site_weights}")

            current_site_weights += weight
            count += 1
            index += 1
            if index == len(port_configs):
                print(f"2. site: {current_site}, count: {count}, current_site_weights: {current_site_weights}")


        # 保存到CSV
        save_to_csv(port_configs, csv_file)
        print(f"\n总计解析出 {len(port_configs)} 组配置")
        print(f"结果已保存到 {csv_file}")

    except FileNotFoundError:
        print(f"错误: 文件 {xml_file} 未找到")
    except Exception as e:
        print(f"解析配置文件时出错: {str(e)}")