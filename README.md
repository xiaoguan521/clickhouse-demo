# ClickHouse 数据导入工具

这是一个用于将 CSV 格式的 API 监控数据导入到 ClickHouse 数据库的工具集。该工具提供了高效的数据导入功能，支持多线程并行处理，并包含完整的错误处理和日志记录机制。同时提供了一系列数据分析和可视化工具，用于监控和分析 API 的性能指标。

## 功能特点

- 🚀 基于 Docker 的 ClickHouse 快速部署
- 📊 支持大规模 CSV 文件数据导入
- 🔄 多线程并行处理，提高导入效率
- 📈 实时进度显示和性能统计
- 📝 详细的导入日志记录
- ⚡ 批量数据处理，优化内存使用
- 🔍 数据验证和错误处理
- 📊 API 性能指标统计和可视化
- 📈 时间序列分析和趋势图
- 📉 每日峰值监控和报告

## 系统要求

- Docker 和 Docker Compose
- Python 3.6+
- 足够的磁盘空间用于存储数据

## 安装步骤

1. 克隆项目到本地：
```bash
git clone [项目地址]
cd clickhouse-demo
```

2. 安装 Python 依赖：
```bash
pip install -r requirements.txt
```

3. 启动 ClickHouse 服务：
```bash
docker-compose up -d
```

## 配置说明

### ClickHouse 配置
在 `docker-compose.yml` 中可以修改以下配置：
- 数据库用户名和密码
- 端口映射
- 数据持久化路径
- 时区设置

### 数据导入配置
在 `import.py` 中可以修改：
- CSV 文件目录路径（CSV_DIR）
- 批处理大小（batch_size）
- 并行处理的线程数（max_workers）

## 使用方法

### 1. 测试数据库连接
```bash
python test.py
```

### 2. 导入数据
```bash
python import.py
```

导入过程中会显示：
- 实时进度条
- 处理速度
- 成功/失败数量
- 预计剩余时间

### 3. 查看导入日志
导入日志保存在 `import_logs` 目录下：
- `success.log`：成功导入的文件记录
- `error.log`：导入失败的文件记录

### 4. 数据分析和可视化

#### 4.1 每日峰值分析 (daily_max_cpm.py)
```bash
python scripts/daily_max_cpm.py
```
功能：
- 统计每天中 CPM 最高的时间点
- 展示每日最大请求量的变化趋势
- 生成柱状图 `daily_max_cpm.png`

#### 4.2 时间序列分析 (time_stats.py)
```bash
python scripts/time_stats.py
```
功能：
- 按时间点统计所有接口的 CPM 总和
- 展示 CPM 最高的前 10 个时间点
- 生成时间序列图表 `time_series_top_10_cpm.png`

## 数据格式要求

CSV 文件需要包含以下列：
- service_name：服务名称
- endpoint：API 端点
- timestamp：时间戳
- cpm：每分钟调用次数
- latency：延迟时间
- query_start_time：查询开始时间
- query_end_time：查询结束时间

## 注意事项

1. 首次运行前请确保：
   - Docker 服务正常运行
   - 有足够的磁盘空间
   - CSV 文件格式正确

2. 数据导入过程中：
   - 请勿中断导入过程
   - 保持足够的磁盘空间
   - 定期检查日志文件

3. 性能优化：
   - 根据服务器配置调整并行线程数
   - 适当调整批处理大小
   - 监控系统资源使用情况

4. 数据分析注意事项：
   - 确保数据已完整导入
   - 检查时间范围的连续性
   - 注意图表保存的磁盘空间

## 常见问题

1. 连接失败
   - 检查 Docker 服务状态
   - 验证端口映射是否正确
   - 确认用户名密码配置

2. 导入速度慢
   - 检查系统资源使用情况
   - 调整并行线程数
   - 优化批处理大小

3. 内存占用过高
   - 减小批处理大小
   - 检查是否有内存泄漏
   - 监控系统资源

4. 统计图表问题
   - 检查数据完整性
   - 确认时间范围设置
   - 验证数据格式正确

## 贡献指南

欢迎提交 Issue 和 Pull Request 来帮助改进这个项目。

## 许可证

本项目采用 [MIT 许可证](https://opensource.org/licenses/MIT) 进行许可。

这意味着你可以自由地：
- ✅ 使用这份代码，包括商业用途
- ✅ 修改代码
- ✅ 分发代码
- ✅ 私有使用

唯一的要求是在你的项目中包含原始许可证和版权声明。

## 联系方式

- 📧 邮箱：xiaochen@201807.xyz
- 💬 如有问题，欢迎通过 Issues 或邮件与我联系

# ClickHouse 监控数据分析工具

这个项目包含了一系列用于分析ClickHouse中存储的监控数据的脚本。

## 中文字体显示问题解决方案

### 问题描述
在Linux环境下运行图表生成脚本时，可能会遇到中文字体显示不正常的问题，通常会显示方框或乱码。这是因为Linux系统默认可能没有安装中文字体。

### 解决方法

1. **脚本适配**
   
   我们已经更新了脚本，能够智能检测系统上已安装的中文字体。脚本会尝试以下方法：
   - 优先使用直接加载字体文件的方式
   - 如果字体文件不可用，则尝试通过字体名称设置
   - 提供多级回退机制确保至少能显示基本图形

2. **安装中文字体**
   
   在CentOS/RHEL系统上，你可以安装Google Noto CJK字体：
   ```bash
   sudo yum install google-noto-cjk-fonts
   ```

   如果需要安装其他中文字体：
   ```bash
   # 安装文泉驿字体
   sudo yum install wqy-microhei-fonts
   
   # 安装更多字体
   sudo yum install cjkuni-uming-fonts
   ```

   **Ubuntu/Debian系统：**
   ```bash
   sudo apt-get update
   sudo apt-get install fonts-noto-cjk fonts-wqy-microhei fonts-wqy-zenhei
   ```

3. **检查字体安装**
   
   安装字体后，可以使用以下命令检查字体是否正确安装：
   ```bash
   # 列出所有已安装的中文字体
   fc-list :lang=zh
   
   # 检查特定字符的支持情况（例如"中"字）
   fc-list :charset=4E2D
   ```
   
   使用 `fc-list :lang=zh` 命令时，输出示例：
   ```
   /usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc: Noto Sans CJK SC:style=Regular
   /usr/share/fonts/google-noto-cjk/NotoSansCJK-Bold.ttc: Noto Sans CJK SC:style=Bold
   /usr/share/fonts/cesi/CESI_HT_GB18030.TTF: CESI黑体-GB18030:style=Regular
   ```
   
   如果输出中包含中文字体，表示安装成功。如果输出为空，则需要安装中文字体。
   
4. **刷新字体缓存**
   
   安装完字体后，需要刷新字体缓存：
   ```bash
   sudo fc-cache -fv
   ```

5. **重启应用**
   
   刷新字体缓存后，重新运行脚本即可。

## 运行脚本

```bash
# 生成每日最大CPM报告
python scripts/daily_max_cpm.py

# 生成时间序列Top 10统计
python scripts/time_stats.py
```

## 常见问题

- **Q: 即使安装了字体，中文仍然无法正确显示?**
  
  A: 确保运行脚本的用户对字体目录有读取权限。也可以尝试使用fontproperties参数显式指定字体路径：
  ```python
  import matplotlib.font_manager as fm
  prop = fm.FontProperties(fname='/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc')
  plt.title('标题', fontproperties=prop)
  ```

- **Q: 如何查看系统中某个字体的详细信息?**
  
  A: 使用`fc-match`命令：
  ```bash
  fc-match "Noto Sans CJK SC"
  ```

- **Q: 如何解读`fc-list :lang=zh`的输出?**
  
  A: 输出格式为"字体文件路径: 字体名称:style=样式"。例如：
  ```
  /usr/share/fonts/google-noto-cjk/NotoSansCJK-Bold.ttc: Noto Sans CJK SC:style=Bold
  ```
  这表示路径为`/usr/share/fonts/google-noto-cjk/NotoSansCJK-Bold.ttc`的字体文件包含"Noto Sans CJK SC"字体的Bold样式。

- **Q: 如何为当前用户安装私有字体?**
  
  A: 将字体文件复制到`~/.fonts`目录下，然后运行`fc-cache -fv`命令刷新缓存：
  ```bash
  mkdir -p ~/.fonts
  cp yourfont.ttf ~/.fonts/
  fc-cache -fv
  ```