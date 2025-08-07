#!/usr/bin/env python3
"""
一键离线Python包安装脚本
专为解决 'externally-managed-environment' 错误设计。
它通过将包安装到用户目录来安全地绕过系统限制。
"""

import os
import sys
import zipfile
import tempfile
import shutil
import site

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
    """
    手动解压并安装.whl包到指定目录。
    .whl文件本质上是一个zip文件。
    """
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
            copied_items = 0
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
                copied_items += 1
        
        print(f"✓  成功安装: {package_name}")
        return True
    except Exception as e:
        print(f"✗  安装失败: {package_name}")
        print(f"   原因: {e}")
        return False

def main():
    print("=" * 60)
    print(" Python 离线包安装程序 (用户模式)")
    print("=" * 60)
    
    # 检查当前用户是否为root，并给出提示
    if os.geteuid() == 0:
        print("ℹ️  提示: 您当前是root用户。")
        print("   包将安装到root的用户目录: /root/.local/lib/...")
    else:
        print(f"ℹ️  提示: 您当前是 {os.getlogin()} 用户。")

    # 1. 确定安装目标目录
    install_target_dir = get_user_site_packages()
    print(f"\n🎯 安装目标目录: {install_target_dir}\n")
    
    # 2. 定义包的安装顺序 (处理依赖关系)
    # 这是根据您提供的文件列表手动排序的
    install_order = [
        # 核心依赖
        "six-1.17.0-py2.py3-none-any.whl",
        "python_dateutil-2.9.0.post0-py2.py3-none-any.whl",
        "pytz-2025.2-py2.py3-none-any.whl",
        "tzdata-2025.2-py2.py3-none-any.whl",
        "numpy-2.2.6-cp312-cp312-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
        # requests 的依赖
        "certifi-2025.8.3-py3-none-any.whl",
        "charset_normalizer-3.4.2-cp312-cp312-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
        "idna-3.10-py3-none-any.whl",
        "urllib3-2.5.0-py3-none-any.whl",
        # openpyxl 的依赖
        "et_xmlfile-2.0.0-py3-none-any.whl",
        # 主要的包
        "requests-2.32.4-py3-none-any.whl",
        "openpyxl-3.1.5-py2.py3-none-any.whl",
        "pandas-2.3.1-cp312-cp312-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
    ]
    
    # 3. 执行安装
    success_count = 0
    failed_packages = []
    
    for package_file in install_order:
        if install_wheel_package(package_file, install_target_dir):
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
    
    # 5. 验证安装
    print("\n" + "=" * 60)
    print(" 验证已安装的包 (通过import)")
    print("=" * 60)
    
    packages_to_verify = ['pandas', 'openpyxl', 'requests', 'numpy']
    all_verified = True
    for module_name in packages_to_verify:
        try:
            __import__(module_name)
            print(f"✓  {module_name:<15} - 导入成功")
        except ImportError as e:
            print(f"✗  {module_name:<15} - 导入失败! Reason: {e}")
            all_verified = False
            
    if all_verified:
        print("\n🎉 全部核心包装验证成功！环境已准备就绪。")
    else:
        print("\n⚠️  部分包验证失败，请检查上面的错误信息。")


if __name__ == "__main__":
    main()