# 快速开始指南

## 🎯 GUI 版本使用

### 启动应用
```bash
java -jar build/libs/parquet-view-gui-1.0-SNAPSHOT.jar
```

### 基本操作

1. **加载文件**
   - 点击 "选择文件" 按钮
   - 选择 `.parquet` 文件
   - 等待加载完成

2. **浏览数据**
   - 使用 "上一页" / "下一页" 按钮翻页
   - 调整右上角的"每页行数"来改变显示数量

3. **过滤数据**
   - 在 WHERE 条件框输入条件，例如：
     - `age > 25`
     - `status = 'active' AND city = 'Beijing'`
     - `price >= 100 AND category IN ('A', 'B', 'C')`
   - 点击 "应用过滤"
   - 查看过滤结果

4. **复制数据** ⭐ 新功能

   **方式 1: 键盘快捷键**
   - 点击选择单元格（可多选）
   - 按 `Ctrl+C` (Windows/Linux) 或 `Cmd+C` (Mac)
   - 粘贴到 Excel、记事本等

   **方式 2: 右键菜单**
   - 在表格上右键
   - 选择：
     - **复制** - 复制选中的单元格
     - **复制整行** - 复制完整行（含表头）
     - **复制列名** - 复制列名
     - **复制所有数据** - 复制当前页所有数据

   **复制技巧：**
   - 单个单元格：点击后 Ctrl+C
   - 多个单元格：按住 Ctrl 点击多个单元格，或拖动选择
   - 整行：点击行号，然后右键 → 复制整行
   - 整列：按住 Ctrl 点击列的多个单元格

5. **重新加载**
   - 点击 "重新加载" 清除过滤并重新加载原始数据

## 💻 CLI 版本使用

### 交互模式
```bash
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar /path/to/file.parquet -i
```

在交互模式中：
- 查看 schema
- 输入 SQL WHERE 条件
- 浏览数据

### 命令行模式
```bash
# 显示前 100 行
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar data.parquet --limit 100

# 应用过滤
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar data.parquet \
  --where "age > 18 AND status = 'active'" \
  --limit 50

# 导出到文件
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar data.parquet \
  --where "city = 'Beijing'" > output.txt
```

## 📝 WHERE 条件示例

### 基础比较
```sql
age > 18
age >= 18 AND age <= 65
name = 'John'
price != 0
```

### 逻辑组合
```sql
age > 18 AND city = 'Beijing'
status = 'active' OR status = 'pending'
NOT (price > 1000)
(age > 18 AND age < 65) AND status = 'active'
```

### NULL 检查
```sql
email IS NOT NULL
deleted_at IS NULL
email IS NOT NULL AND verified = true
```

### IN 操作符
```sql
status IN ('active', 'pending', 'approved')
category IN ('A', 'B', 'C')
id NOT IN (1, 2, 3)
```

### 复杂组合
```sql
(age >= 18 AND age <= 65) AND (status = 'active' OR status = 'pending') AND city IN ('Beijing', 'Shanghai')
```

## ⚡ 性能优化建议

### 使用谓词下推（自动）
系统会自动尝试将 WHERE 条件转换为 Parquet Filter，实现谓词下推：
- ✅ 简单比较：`age > 18`
- ✅ AND/OR 组合：`age > 18 AND status = 'active'`
- ✅ IN 操作：`city IN ('A', 'B')`
- ❌ LIKE 模式：`name LIKE '%John%'` (会自动回退到内存过滤)

### 调整每页大小
- 小文件：使用较大的页面大小（5000-10000）
- 大文件：使用较小的页面大小（500-1000）
- 根据内存情况调整

### 过滤大文件
```
步骤：
1. 输入过滤条件
2. 点击"应用过滤"
3. 系统会扫描整个文件，只保存匹配行的索引（内存占用小）
4. 按需加载每页数据
```

## 🐛 故障排查

### 问题: 文件加载失败
**解决:**
- 确认文件是有效的 Parquet 文件
- 检查文件权限
- 查看错误信息

### 问题: 过滤速度慢
**解决:**
- 检查状态栏是否显示 "Applied Parquet filter predicate"
- 如果显示 "Could not convert"，说明使用了不支持的 SQL 表达式
- 简化 WHERE 条件，避免使用 LIKE 等复杂表达式

### 问题: 复制后粘贴到 Excel 格式错乱
**解决:**
- 使用 Ctrl+V 直接粘贴（不要用"选择性粘贴"）
- 确保 Excel 识别了 Tab 分隔符
- 可以先粘贴到记事本查看原始数据

### 问题: 内存不足
**解决:**
```bash
# 增加 JVM 堆内存
java -Xmx4g -jar parquet-view-gui-1.0-SNAPSHOT.jar

# 或减小每页行数
```

## 📚 更多文档

- [完整 README](README.md) - 详细功能说明
- [Parquet Filter 升级说明](PARQUET_FILTER_UPGRADE.md) - 技术细节
- [复制功能指南](COPY_FEATURE_GUIDE.md) - 复制功能详解

## 🎉 新功能亮点（v2.0）

✅ **Parquet Filter API** - 谓词下推，性能提升 2-6 倍  
✅ **单元格复制** - Ctrl+C + 右键菜单，支持多种复制模式  
✅ **复杂 SQL 支持** - AND/OR/NOT、IN、NULL 检查  
✅ **智能过滤** - 自动选择最优过滤策略  
✅ **TSV 格式** - 可直接粘贴到 Excel  

---

**快速反馈**: 如遇问题，查看状态栏的提示信息
