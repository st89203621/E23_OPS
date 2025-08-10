#!/usr/bin/env python3
"""
DWD和ADS一致性统计 - 离线Python包安装脚本（修复版）
自动检测实际存在的包文件并按依赖关系安装
"""

import os
import sys
import zipfile
import tempfile
import shutil
import site
import tarfile
import glob

def get_user_site_packages():
    """获取用户site-packages目录，如果不存在则创建。"""
    try:
        # 使用标准库函数获取路径
        user_site = site.getusersitepackages()
    except (AttributeError, TypeError):
        # 为旧版本Python或特殊环境提供备用方法
        python_version = f"python{sys.version_info.major}.{sys.version_info.minor}"
        user_site = os.path.expanduser(f"~/.local/lib/{python_version}/site-packages")

    if not os.path.exists(user_site):
        print(f"创建用户site-packages目录: {user_site}")
        os.makedirs(user_site, exist_ok=True)
    
    # 将用户目录添加到sys.path，以便验证步骤可以立即找到模块
    if user_site not in sys.path:
        sys.path.insert(0, user_site)
        
    return user_site

def install_wheel_package(wheel_path, install_dest):
    """手动解压并安装.whl包到指定目录。"""
    package_name = os.path.basename(wheel_path)
    print(f"\n⚙️  正在处理: {package_name}")

    if not os.path.exists(wheel_path):
        print(f"✗ 错误: 文件不存在 -> {wheel_path}")
        return False
        
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # 1. 解压wheel文件
            with zipfile.ZipFile(wheel_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # 2. 将解压后的内容复制到目标安装目录
            for item in os.listdir(temp_dir):
                source_item = os.path.join(temp_dir, item)
                dest_item = os.path.join(install_dest, item)
                
                # 如果目标已存在，先删除，防止旧文件残留
                if os.path.exists(dest_item):
                    if os.path.isdir(dest_item):
                        shutil.rmtree(dest_item)
                    else:
                        os.remove(dest_item)
                
                # 复制
                if os.path.isdir(source_item):
                    shutil.copytree(source_item, dest_item)
                else:
                    shutil.copy2(source_item, dest_item)
        
        print(f"✓  成功安装: {package_name}")
        return True
    except Exception as e:
        print(f"✗  安装失败: {package_name}")
        print(f"   原因: {e}")
        return False

def install_tar_package(tar_path, install_dest):
    """手动解压并安装.tar.gz包到指定目录。"""
    package_name = os.path.basename(tar_path)
    print(f"\n⚙️  正在处理: {package_name}")

    if not os.path.exists(tar_path):
        print(f"✗ 错误: 文件不存在 -> {tar_path}")
        return False
        
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # 1. 解压tar.gz文件
            with tarfile.open(tar_path, 'r:gz') as tar_ref:
                tar_ref.extractall(temp_dir)
            
            # 2. 找到解压后的目录（通常只有一个）
            extracted_dirs = [d for d in os.listdir(temp_dir) if os.path.isdir(os.path.join(temp_dir, d))]
            if not extracted_dirs:
                print(f"✗ 错误: 无法找到解压后的目录")
                return False
            
            source_dir = os.path.join(temp_dir, extracted_dirs[0])
            
            # 3. 查找Python包目录（通常与包名相同或类似）
            for item in os.listdir(source_dir):
                source_item = os.path.join(source_dir, item)
                if os.path.isdir(source_item) and not item.endswith('.egg-info') and not item.startswith('__pycache__'):
                    dest_item = os.path.join(install_dest, item)
                    
                    # 如果目标已存在，先删除
                    if os.path.exists(dest_item):
                        if os.path.isdir(dest_item):
                            shutil.rmtree(dest_item)
                        else:
                            os.remove(dest_item)
                    
                    # 复制
                    shutil.copytree(source_item, dest_item)
        
        print(f"✓  成功安装: {package_name}")
        return True
    except Exception as e:
        print(f"✗  安装失败: {package_name}")
        print(f"   原因: {e}")
        return False

def create_thrift_compatibility(install_target_dir):
    """创建thrift兼容性模块，使用系统的thriftpy"""
    try:
        # 创建thrift兼容性目录
        thrift_dir = os.path.join(install_target_dir, 'thrift')
        os.makedirs(thrift_dir, exist_ok=True)

        # 创建__init__.py文件
        init_file = os.path.join(thrift_dir, '__init__.py')
        with open(init_file, 'w') as f:
            f.write('''# Thrift兼容性模块
# 导入所有thrift模块内容
try:
    from thrift.Thrift import *
    from thrift.transport import *
    from thrift.protocol import *
    print("使用完整thrift库")
except ImportError:
    # 如果标准thrift不完整，尝试从我们安装的包中导入
    try:
        import sys
        import os

        # 查找thrift包的实际位置
        thrift_path = None
        for path in sys.path:
            potential_path = os.path.join(path, 'thrift')
            if os.path.exists(potential_path) and os.path.isdir(potential_path):
                thrift_path = potential_path
                break

        if thrift_path:
            # 导入thrift的核心模块
            sys.path.insert(0, os.path.dirname(thrift_path))
            from thrift.Thrift import *
            from thrift.transport.TSocket import TSocket
            from thrift.transport.TTransport import TBufferedTransport
            from thrift.protocol.TBinaryProtocol import TBinaryProtocol
            print("使用已安装的thrift库")
        else:
            raise ImportError("找不到thrift模块")

    except ImportError:
        print("警告: thrift模块不完整，PyHive可能无法工作")
        # 创建基本的兼容性类
        class TException(Exception):
            pass

        class TApplicationException(TException):
            pass
''')

        print(f"✓ 创建thrift兼容性模块: {thrift_dir}")
        return True
    except Exception as e:
        print(f"✗ 创建thrift兼容性模块失败: {e}")
        return False

def main():
    print("=" * 60)
    print(" DWD和ADS一致性统计 - Python离线包安装程序（修复版）")
    print("=" * 60)
    
    # 检查当前用户是否为root，并给出提示
    try:
        if os.geteuid() == 0:
            print("ℹ️  提示: 您当前是root用户。")
            print("   包将安装到root的用户目录: /root/.local/lib/...")
        else:
            print(f"ℹ️  提示: 您当前是普通用户。")
    except AttributeError:
        # Windows系统没有geteuid
        print("ℹ️  提示: 检测到Windows系统。")

    # 1. 确定安装目标目录
    install_target_dir = get_user_site_packages()
    print(f"\n🎯 安装目标目录: {install_target_dir}\n")
    
    # 2. 自动发现并按依赖关系排序安装包
    # 获取当前目录下的所有包文件
    all_packages = glob.glob("*.whl") + glob.glob("*.tar.gz")
    
    if not all_packages:
        print("❌ 错误: 当前目录下没有找到任何Python包文件")
        print("请确保包文件(.whl 或 .tar.gz)在当前目录中")
        return
    
    # 定义安装优先级（数字越小优先级越高）
    priority_map = {
        'six': 1,
        'setuptools': 1, 
        'wheel': 1,
        'future': 2,
        'python_dateutil': 2,
        'certifi': 3,
        'urllib3': 3,
        'PyYAML': 4,
        'thrift': 5,
        'pure-sasl': 6,
        'thrift_sasl': 7,
        'elasticsearch': 8,
        'PyHive': 9  # 最后安装
    }
    
    # 按优先级排序包
    def get_package_priority(filename):
        for key, priority in priority_map.items():
            if key.lower() in filename.lower():
                return priority
        return 999  # 未知包放在最后
    
    install_order = sorted(all_packages, key=get_package_priority)
    
    print(f"发现 {len(install_order)} 个包文件:")
    for i, pkg in enumerate(install_order, 1):
        print(f"  {i}. {pkg}")
    print()
    
    # 3. 执行安装
    success_count = 0
    failed_packages = []
    
    for package_file in install_order:
        package_path = os.path.join(os.path.dirname(__file__), package_file)
        
        if not os.path.exists(package_path):
            print(f"\n⚠️  跳过: {package_file} (文件不存在)")
            continue
            
        if package_file.endswith('.whl'):
            success = install_wheel_package(package_path, install_target_dir)
        elif package_file.endswith('.tar.gz'):
            success = install_tar_package(package_path, install_target_dir)
        else:
            print(f"\n⚠️  跳过: {package_file} (不支持的文件格式)")
            continue
            
        if success:
            success_count += 1
        else:
            failed_packages.append(package_file)
            
    # 4. 报告结果
    print("\n" + "=" * 60)
    print(" 安装完成统计")
    print("=" * 60)
    print(f"成功: {success_count} / {len(install_order)} 个包")
    if failed_packages:
        print(f"失败: {len(failed_packages)} 个包")
        for pkg in failed_packages:
            print(f"  - {pkg}")

    # 4.5. 创建thrift兼容性模块
    print("\n" + "=" * 60)
    print(" 创建thrift兼容性模块")
    print("=" * 60)
    create_thrift_compatibility(install_target_dir)

    # 5. 验证安装
    print("\n" + "=" * 60)
    print(" 验证已安装的包 (通过import)")
    print("=" * 60)
    
    packages_to_verify = ['yaml', 'pyhive', 'elasticsearch', 'thrift']
    all_verified = True
    for module_name in packages_to_verify:
        try:
            if module_name == 'pyhive':
                from pyhive import hive
                print(f"✓  {module_name:<15} - 导入成功")
            else:
                __import__(module_name)
                print(f"✓  {module_name:<15} - 导入成功")
        except ImportError as e:
            print(f"✗  {module_name:<15} - 导入失败! Reason: {e}")
            all_verified = False
            
    if all_verified:
        print("\n🎉 全部核心包验证成功！DWD和ADS一致性统计环境已准备就绪。")
    else:
        print("\n⚠️  部分包验证失败，请检查上面的错误信息。")
        print("   您可以尝试手动安装失败的包：")
        print("   pip3 install --user package_name")


if __name__ == "__main__":
    main()
