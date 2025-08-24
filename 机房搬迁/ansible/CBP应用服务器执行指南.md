# CBP应用服务器集群 - Ansible执行指南

## 📋 概述

本指南用于E23机房搬迁项目中CBP应用服务器集群的自动化配置管理。

### 服务器信息
- **总数**: 20台服务器
- **旧网段**: 192.168.13.x (A1机房)
- **新网段**: 192.168.28.x (A2机房)
- **管理网段**: 192.168.30.x (iDRAC)

### 服务器分类
- **Nebula图数据库**: 3台 (nebula-s1~s3)
- **数据库服务器**: 2台 (mysql-s1, oracle-s1)
- **应用服务器**: 13台 (app-s2~s12, app-s21~s22)
- **Redis缓存**: 2台 (redis-s1~s2)

## 🚀 执行步骤

### 前置检查

1. **确认当前目录**
   ```bash
   cd /c/Users/semptian/IdeaProjects/E23_OPS/机房搬迁/ansible
   pwd
   ```

2. **检查inventory文件**
   ```bash
   ls -la inventory/
   cat inventory/app_server.ini
   ```

3. **检查ansible连通性**
   ```bash
   # 测试单个服务器
   ansible -i inventory/app_server.ini nebula-s1 -m ping
   
   # 测试所有服务器
   ansible -i inventory/app_server.ini cbp_all_servers -m ping
   ```

### 方法一：使用Shell脚本（推荐）

```bash
# 1. 给脚本执行权限
chmod +x update_cbp_hosts.sh

# 2. 执行脚本
./update_cbp_hosts.sh
```

**脚本执行流程：**
- ✅ 检查inventory文件
- ✅ 显示目标服务器信息
- ✅ 用户确认
- ✅ 测试服务器连接
- ✅ 备份原始hosts文件
- ✅ 移除旧配置
- ✅ 添加新配置
- ✅ 验证配置

### 方法二：使用Ansible Playbook

```bash
# 执行playbook
ansible-playbook -i inventory/app_server.ini update-cbp-hosts.yml

# 带详细输出
ansible-playbook -i inventory/app_server.ini update-cbp-hosts.yml -v

# 检查模式（不实际执行）
ansible-playbook -i inventory/app_server.ini update-cbp-hosts.yml --check
```

### 方法三：手动执行单个命令

```bash
# 1. 备份hosts文件
ansible -i inventory/app_server.ini cbp_all_servers -m copy -a "src=/etc/hosts dest=/etc/hosts.backup.$(date +%Y%m%d_%H%M%S) remote_src=yes"

# 2. 移除旧配置
ansible -i inventory/app_server.ini cbp_all_servers -m blockinfile -a "path=/etc/hosts marker='# {mark} CBP CLUSTER HOSTS' state=absent"

# 3. 添加新配置
ansible -i inventory/app_server.ini cbp_all_servers -m blockinfile -a "path=/etc/hosts marker='# {mark} CBP CLUSTER HOSTS' block='# CBP应用服务器集群hosts配置
192.168.28.1 nebula-s1
192.168.28.2 nebula-s2
192.168.28.3 nebula-s3
192.168.28.4 mysql-s1
192.168.28.5 oracle-s1
192.168.28.6 app-s2
192.168.28.7 app-s3
192.168.28.8 app-s4
192.168.28.9 app-s5
192.168.28.10 app-s6
192.168.28.11 app-s7
192.168.28.12 app-s8
192.168.28.13 app-s9
192.168.28.14 app-s10
192.168.28.15 app-s11
192.168.28.16 app-s12
192.168.28.17 app-s21
192.168.28.18 app-s22
192.168.28.19 redis-s1
192.168.28.20 redis-s2' backup=yes"
```

## 🔍 验证和测试

### 基本验证

```bash
# 1. 检查hosts文件内容
ansible -i inventory/app_server.ini cbp_all_servers -m shell -a "grep -A 25 'CBP CLUSTER HOSTS' /etc/hosts"

# 2. 测试域名解析
ansible -i inventory/app_server.ini mysql-s1 -m shell -a "nslookup nebula-s1"
ansible -i inventory/app_server.ini app-s2 -m shell -a "nslookup redis-s1"

# 3. 测试网络连通性
ansible -i inventory/app_server.ini nebula-s1 -m shell -a "ping -c 2 mysql-s1"
```

### 分组验证

```bash
# 验证Nebula服务器
ansible -i inventory/app_server.ini nebula_servers -m shell -a "hostname && grep nebula /etc/hosts"

# 验证数据库服务器
ansible -i inventory/app_server.ini database_servers -m shell -a "hostname && grep -E 'mysql|oracle' /etc/hosts"

# 验证应用服务器
ansible -i inventory/app_server.ini app_servers -m shell -a "hostname && grep app-s /etc/hosts"

# 验证Redis服务器
ansible -i inventory/app_server.ini redis_servers -m shell -a "hostname && grep redis /etc/hosts"
```

## 🛠️ 故障排除

### 常见问题

1. **SSH连接失败**
   ```bash
   # 检查SSH配置
   ansible -i inventory/app_server.ini cbp_all_servers -m setup -a "filter=ansible_ssh*"
   
   # 使用密码认证
   ansible -i inventory/app_server.ini cbp_all_servers -m ping --ask-pass
   ```

2. **权限不足**
   ```bash
   # 使用sudo
   ansible -i inventory/app_server.ini cbp_all_servers -m shell -a "whoami" --become
   ```

3. **部分服务器失败**
   ```bash
   # 限制执行特定服务器
   ansible -i inventory/app_server.ini nebula-s1,mysql-s1 -m ping
   
   # 跳过失败的服务器继续执行
   ansible-playbook -i inventory/app_server.ini update-cbp-hosts.yml --limit @/tmp/retry_hosts.txt
   ```

### 回滚操作

```bash
# 恢复备份的hosts文件
ansible -i inventory/app_server.ini cbp_all_servers -m shell -a "ls -la /etc/hosts.backup.*"
ansible -i inventory/app_server.ini cbp_all_servers -m copy -a "src=/etc/hosts.backup.XXXXXX dest=/etc/hosts remote_src=yes"
```

## 📝 执行日志

建议执行时保存日志：

```bash
# 执行并保存日志
./update_cbp_hosts.sh 2>&1 | tee cbp_hosts_update_$(date +%Y%m%d_%H%M%S).log

# 或者使用playbook
ansible-playbook -i inventory/app_server.ini update-cbp-hosts.yml 2>&1 | tee cbp_playbook_$(date +%Y%m%d_%H%M%S).log
```

## ⚠️ 注意事项

1. **执行时间**: 建议在业务低峰期执行
2. **分批执行**: 脚本默认每批处理5台服务器
3. **备份确认**: 执行前确保已备份重要配置
4. **网络稳定**: 确保管理网络稳定
5. **权限检查**: 确保有足够的sudo权限

## 📞 支持

如遇问题，请检查：
- 网络连通性
- SSH密钥配置
- 服务器权限
- Ansible版本兼容性
