# DWD和ADS一致性统计 - 离线Python包

## 📦 概述

这个目录包含了DWD和ADS一致性统计所需的所有Python包，支持完全离线安装。就像IAM_USER_STATE项目一样，所有依赖包都已预先下载，无需网络连接即可安装。

## 🗂️ 目录结构

```
offline_packages/
├── README.md                    # 本说明文件
├── offline_installer.py         # 专用离线安装脚本
├── download_packages.py         # 包下载脚本（用于更新包）
├── requirements-offline.txt     # 离线包列表
└── *.whl, *.tar.gz             # Python包文件（需要下载）
```

## 🚀 使用方法

### 方法1：自动安装（推荐）
主安装脚本 `setup_and_run.sh` 会自动检测并使用离线包：

```bash
# 在项目根目录执行
./setup_and_run.sh
```

脚本会自动：
1. 检测 `offline_packages` 目录
2. 使用 `offline_installer.py` 进行离线安装
3. 如果离线安装失败，自动切换到在线安装

### 方法2：手动离线安装
如果需要单独安装Python依赖：

```bash
cd offline_packages
python3 offline_installer.py
```

## 📥 下载Python包

**重要：当前目录只包含安装脚本，还需要下载实际的Python包文件。**

### 在有网络的环境中下载包：

```bash
cd offline_packages
python3 download_packages.py
```

这将下载以下包：
- **基础依赖**: six, setuptools, wheel
- **配置解析**: PyYAML
- **Hive连接**: thrift, pure-sasl, sasl, thrift-sasl, PyHive
- **ES连接**: urllib3, certifi, elastic-transport, elasticsearch

### 手动下载（备选方案）：

```bash
cd offline_packages

# 下载所有依赖包
pip download PyYAML PyHive "elasticsearch<9.0.0" thrift sasl pure-sasl --dest .
```

## 🔧 离线安装器特性

`offline_installer.py` 具有以下特性：

1. **智能依赖处理**: 按正确顺序安装包，处理依赖关系
2. **用户目录安装**: 安装到用户目录，避免系统权限问题
3. **错误处理**: 详细的错误报告和失败包列表
4. **安装验证**: 自动验证所有核心包是否可以正常导入
5. **跨平台支持**: 支持Linux和Windows环境

## 📋 包列表

### 核心依赖包：
- `PyYAML` - YAML配置文件解析
- `PyHive` - Hive数据库连接
- `elasticsearch` - Elasticsearch客户端
- `thrift` - Apache Thrift框架
- `sasl` - SASL认证支持
- `pure-sasl` - 纯Python SASL实现
- `thrift-sasl` - Thrift SASL支持

### 基础依赖：
- `six` - Python 2/3兼容性
- `setuptools` - 包管理工具
- `wheel` - Wheel格式支持
- `urllib3` - HTTP客户端
- `certifi` - SSL证书
- `elastic-transport` - ES传输层

## 🔍 故障排除

### 问题1：没有包文件
```
错误: 当前目录下没有找到Python包文件
```

**解决方案**: 运行下载脚本
```bash
python3 download_packages.py
```

### 问题2：导入失败
```
✗ pyhive - 导入失败! Reason: No module named 'pyhive'
```

**解决方案**: 
1. 检查是否所有依赖包都安装成功
2. 尝试手动安装失败的包：
   ```bash
   pip3 install --user PyHive
   ```

### 问题3：权限问题
```
Permission denied: '/usr/local/lib/python3.x/site-packages'
```

**解决方案**: 离线安装器会自动使用用户目录，无需root权限

### 问题4：平台兼容性
某些包可能有平台特定版本（如Linux x86_64），如果在不同平台上安装失败，可以：

1. 重新下载适合目标平台的包
2. 使用在线安装作为备选方案

## 📊 安装验证

安装完成后，脚本会自动验证以下模块：
- ✓ yaml (PyYAML)
- ✓ pyhive (PyHive)  
- ✓ elasticsearch (elasticsearch)
- ✓ thrift (thrift)
- ✓ sasl (sasl)

如果所有模块都显示"导入成功"，说明环境已准备就绪。

## 🔄 更新包

如需更新到最新版本的包：

```bash
cd offline_packages

# 清理旧包
rm -f *.whl *.tar.gz

# 重新下载
python3 download_packages.py
```

## 💡 提示

1. **首次使用**: 确保先运行 `download_packages.py` 下载包文件
2. **网络环境**: 下载脚本需要网络连接，但安装脚本完全离线
3. **存储空间**: 所有包文件大约需要50-100MB存储空间
4. **兼容性**: 支持Python 3.6+，推荐Python 3.8+

这样设计的好处是，一旦下载完成，整个项目就可以在任何Linux服务器上完全离线部署和运行！
