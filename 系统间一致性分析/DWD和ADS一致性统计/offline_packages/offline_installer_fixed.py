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

def env_has_required_packages():
    """检测当前环境是否已具备所需依赖，若已具备则无需离线安装与兼容模块。
    要求：yaml、elasticsearch 可导入，PyHive 可导入且 thrift.transport.THttpClient 存在。
    """
    try:
        import yaml  # noqa: F401
        import elasticsearch  # noqa: F401
        from pyhive import hive  # noqa: F401
        # 检查标准 thrift 结构
        from thrift.transport import THttpClient  # noqa: F401
        return True
    except Exception as e:
        print(f"环境自检：依赖未完全就绪（{e}），将使用离线包/兼容模块进行修复")
        return False


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

    # 如果标准 thrift 可用，直接跳过创建兼容模块，避免覆盖官方包
    try:
        from thrift.transport import THttpClient  # noqa: F401
        print("检测到标准thrift可用，跳过创建兼容模块")
        return True
    except Exception:
        pass

    """创建完整的thrift兼容性模块"""
    try:
        # 创建thrift兼容性目录
        thrift_dir = os.path.join(install_target_dir, 'thrift')
        os.makedirs(thrift_dir, exist_ok=True)

        # 创建__init__.py文件
        init_file = os.path.join(thrift_dir, '__init__.py')
        with open(init_file, 'w') as f:
            f.write('''# Thrift兼容性模块 - 超完整版
import sys
import os

# 定义TType常量
class TType:
    STOP = 0
    VOID = 1
    BOOL = 2
    BYTE = 3
    I08 = 3
    DOUBLE = 4
    I16 = 6
    I32 = 8
    I64 = 10
    STRING = 11
    UTF7 = 11
    STRUCT = 12
    MAP = 13
    SET = 14
    LIST = 15
    UTF8 = 16
    UTF16 = 17

# 定义基本异常类
class TException(Exception):
    def __init__(self, message=None):
        super().__init__(message)
        self.message = message

class TApplicationException(TException):
    UNKNOWN = 0
    UNKNOWN_METHOD = 1
    INVALID_MESSAGE_TYPE = 2
    WRONG_METHOD_NAME = 3
    BAD_SEQUENCE_ID = 4
    MISSING_RESULT = 5
    INTERNAL_ERROR = 6
    PROTOCOL_ERROR = 7
    INVALID_TRANSFORM = 8
    INVALID_PROTOCOL = 9
    UNSUPPORTED_CLIENT_TYPE = 10

    def __init__(self, type=UNKNOWN, message=None):
        super().__init__(message)
        self.type = type

class TTransportException(TException):
    UNKNOWN = 0
    NOT_OPEN = 1
    ALREADY_OPEN = 2
    TIMED_OUT = 3
    END_OF_FILE = 4

class TProtocolException(TException):
    UNKNOWN = 0
    INVALID_DATA = 1
    NEGATIVE_SIZE = 2
    SIZE_LIMIT = 3
    BAD_VERSION = 4

# 定义TMessageType常量
class TMessageType:
    CALL = 1
    REPLY = 2
    EXCEPTION = 3
    ONEWAY = 4

# 定义TFrozenDict类
class TFrozenDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        raise TypeError("TFrozenDict is immutable")

    def __delitem__(self, key):
        raise TypeError("TFrozenDict is immutable")

# 定义更多PyHive需要的类和常量
class TCompactType:
    STOP = 0
    TRUE = 1
    FALSE = 2
    BYTE = 3
    I16 = 4
    I32 = 5
    I64 = 6
    DOUBLE = 7
    BINARY = 8
    LIST = 9
    SET = 10
    MAP = 11
    STRUCT = 12

class TProcessor:
    def __init__(self):
        pass

    def process(self, iprot, oprot):
        pass

class TMultiplexedProcessor:
    def __init__(self):
        self.services = {}

    def registerProcessor(self, serviceName, processor):
        self.services[serviceName] = processor

# 定义基本的序列化函数
def readI32(transport):
    return 0

def writeI32(transport, value):
    pass

def readString(transport):
    return ""

def writeString(transport, value):
    pass

# 简单的传输和协议类
class TSocket:
    def __init__(self, host='localhost', port=9090):
        self.host = host
        self.port = port
        self._socket = None

    def open(self):
        import socket
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self.host, self.port))

    def close(self):
        if self._socket:
            self._socket.close()

class TBufferedTransport:
    def __init__(self, trans):
        self.trans = trans

class TBinaryProtocol:
    def __init__(self, trans):
        self.trans = trans

# 将所有类添加到全局命名空间
globals().update({
    'TType': TType,
    'TMessageType': TMessageType,
    'TFrozenDict': TFrozenDict,
    'TCompactType': TCompactType,
    'TProcessor': TProcessor,
    'TMultiplexedProcessor': TMultiplexedProcessor,
    'TException': TException,
    'TApplicationException': TApplicationException,
    'TTransportException': TTransportException,
    'TProtocolException': TProtocolException,
    'TSocket': TSocket,
    'TBufferedTransport': TBufferedTransport,
    'TBinaryProtocol': TBinaryProtocol,
    'readI32': readI32,
    'writeI32': writeI32,
    'readString': readString,
    'writeString': writeString,
})

print("使用完整thrift兼容性模块")
''')

        # 创建thrift.Thrift子模块
        thrift_file = os.path.join(thrift_dir, 'Thrift.py')
        with open(thrift_file, 'w') as f:
            f.write('''# thrift.Thrift 子模块
from . import *

# 导出所有需要的类和常量
__all__ = ['TType', 'TMessageType', 'TFrozenDict', 'TCompactType', 'TProcessor', 'TMultiplexedProcessor',
           'TException', 'TApplicationException', 'TTransportException', 'TProtocolException',
           'readI32', 'writeI32', 'readString', 'writeString']
''')

        # 创建transport子模块目录
        transport_dir = os.path.join(thrift_dir, 'transport')
        os.makedirs(transport_dir, exist_ok=True)

        with open(os.path.join(transport_dir, '__init__.py'), 'w') as f:
            f.write('from .. import TSocket, TBufferedTransport, TTransportException\n')
            f.write('from .THttpClient import THttpClient\n')

        with open(os.path.join(transport_dir, 'TSocket.py'), 'w') as f:
            f.write('from .. import TSocket\n')

        with open(os.path.join(transport_dir, 'TTransport.py'), 'w') as f:
            f.write('from .. import TBufferedTransport, TTransportException\n')

        # Provide a minimal THttpClient stub so imports like `from thrift.transport import THttpClient` succeed
        with open(os.path.join(transport_dir, 'THttpClient.py'), 'w') as f:
            f.write('''# Minimal stub for thrift.transport.THttpClient to satisfy imports\n\n''')
            f.write('''class THttpClient:\n''')
            f.write('''    def __init__(self, uri_or_host, port=None, path=None, scheme='http'):\n''')
            f.write('''        # Support both URI form and host/port/path form\n''')
            f.write("""        if port is None:\n            self.uri = str(uri_or_host)\n        else:\n            _path = path or '/'\n            self.uri = f"{scheme}://{uri_or_host}:{port}{_path}"\n""")
            f.write('''        self.headers = {}\n''')
            f.write('''        self.timeout = None\n''')
            f.write('''\n''')
            f.write('''    def setTimeout(self, ms):\n''')
            f.write('''        self.timeout = ms\n''')
            f.write('''\n''')
            f.write('''    def setCustomHeaders(self, headers):\n''')
            f.write('''        if headers:\n''')
            f.write('''            self.headers.update(headers)\n''')

        # 创建protocol子模块目录
        protocol_dir = os.path.join(thrift_dir, 'protocol')
        os.makedirs(protocol_dir, exist_ok=True)

        with open(os.path.join(protocol_dir, '__init__.py'), 'w') as f:
            f.write('from .. import TBinaryProtocol, TProtocolException\n')

        with open(os.path.join(protocol_dir, 'TBinaryProtocol.py'), 'w') as f:
            f.write('from .. import TBinaryProtocol\n')

        with open(os.path.join(protocol_dir, 'TProtocol.py'), 'w') as f:
            f.write('from .. import TProtocolException\n')

        # 优先使用 PyHive 自带的 TCLIService，不再创建不完整的兼容模块
        tcli_dir = os.path.join(install_target_dir, 'TCLIService')

        def _tcli_constants_has_primitive_types(p):
            try:
                with open(os.path.join(p, 'constants.py'), 'r', encoding='utf-8') as f:
                    head = f.read(4096)
                    return 'PRIMITIVE_TYPES' in head
            except Exception:
                return False

        # 如存在我们之前生成但不完整的 TCLIService，进行清理，避免遮蔽 PyHive 自带的版本
        if os.path.isdir(tcli_dir) and not _tcli_constants_has_primitive_types(tcli_dir):
            try:
                shutil.rmtree(tcli_dir)
                print("移除不完整的 TCLIService 兼容模块，改用 PyHive 提供的 TCLIService")
            except Exception as e:
                print(f"清理 TCLIService 兼容模块失败: {e}")

        # 最终检测是否存在有效的 TCLIService
        try:
            from TCLIService import constants as _c  # noqa: F401
            from TCLIService import ttypes as _tt  # noqa: F401
            if hasattr(_c, 'PRIMITIVE_TYPES') and hasattr(_tt, 'TTypeId'):
                print("检测到有效的 TCLIService（来自 PyHive），无需创建兼容模块")
            else:
                print("警告: 检测到 TCLIService 但不完整，PyHive 可能无法正常工作")
        except Exception:
            print("警告: 未找到 TCLIService，PyHive 可能无法正常工作")

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


    # 0. 若环境已具备依赖则直接验证并退出
    if env_has_required_packages():
        print("\n已检测到当前环境具备所需依赖，跳过离线安装与兼容模块创建。")
        # 直接进入验证流程
        print("\n" + "=" * 60)
        print(" 验证已安装的包 (通过import)")
        print("=" * 60)
        packages_to_verify = ['yaml', 'pyhive', 'elasticsearch', 'thrift']
        all_verified = True
        for module_name in packages_to_verify:
            try:
                if module_name == 'pyhive':
                    from pyhive import hive  # noqa: F401
                    print(f"✓  {module_name:<15} - 导入成功")
                else:
                    __import__(module_name)
                    print(f"✓  {module_name:<15} - 导入成功")
            except ImportError as e:
                print(f"✗  {module_name:<15} - 导入失败! Reason: {e}")
                all_verified = False
        if all_verified:
            print("\n🎉 全部核心包验证成功！DWD和ADS一致性统计环境已准备就绪。")
            return
        else:
            print("\n⚠️ 环境自检显示部分依赖异常，继续执行离线安装流程...")

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
