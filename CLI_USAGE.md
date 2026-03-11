# Parquet View CLI 使用说明

## 功能介绍

命令行界面（CLI）模式可以在终端中查看和过滤 Parquet 文件，支持两种模式：
1. **简单模式**：静态表格输出，适合快速查看
2. **交互模式**：全屏实时 TUI，支持滚动浏览、实时过滤

## 安装与编译

### 编译项目
```bash
./gradlew build
```

### 构建独立 CLI JAR（使用 Shadow 插件）
```bash
./gradlew cliJar
# 或者直接使用
./gradlew shadowJar
```
生成的 JAR 文件位于：`build/libs/parquet-view-cli-1.0-SNAPSHOT.jar`

Shadow 插件的优势：
- ✅ 自动处理依赖冲突
- ✅ 合并 service 文件
- ✅ 更可靠的打包方式
- ✅ 自动排除签名文件

## 使用方法

### 1. 基本用法 - 查看文件

```bash
# 使用 Gradle 运行
./gradlew runCli --args="path/to/file.parquet"

# 使用独立 JAR
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar path/to/file.parquet
```

### 2. 限制显示行数

```bash
# 显示前 50 行
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet --limit 50

# 简写
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet -l 50
```

### 3. 使用 WHERE 条件过滤

```bash
# 过滤年龄大于 18 的记录
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet --where "age > 18"

# 多个条件
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet -w "age > 18 AND status = 'active'"

# IS NOT NULL
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet -w "email IS NOT NULL"

# LIKE 模糊匹配
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet -w "name LIKE '%John%'"
```

### 4. 交互式模式（全屏 TUI）⭐ 推荐

```bash
# 启动交互式全屏界面
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet --interactive

# 简写
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet -i

# 限制加载行数 + 交互模式
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet -l 5000 -i
```

#### 交互式模式功能：
- ✨ **全屏界面**：占据整个终端窗口
- 📊 **实时表格**：可滚动浏览的数据表格
- 🔍 **实时过滤**：输入 WHERE 条件，点击 "Apply Filter" 或按 Enter 应用
- ⌨️ **键盘快捷键**：
  - `↑↓←→` - 在表格中导航/滚动
  - `Tab` - 切换到下一个控件
  - `Enter` - 应用过滤条件
  - `F5` - 重置过滤器，显示所有数据
  - `ESC` - 退出程序
- 📈 **状态栏**：实时显示当前记录数和状态信息
- 🎯 **按钮操作**：
  - "Apply Filter" - 应用 WHERE 条件
  - "Reset (F5)" - 重置过滤
  - "Quit (ESC)" - 退出

#### 交互模式截图示例：
```
┌─ Status ────────────────────────────────────────────────────┐
│ Ready | Records: 150 / 1000                                  │
└──────────────────────────────────────────────────────────────┘
┌─ Filter ────────────────────────────────────────────────────┐
│ WHERE Clause (press Enter to apply, F5 to reset):           │
│ age > 25 AND status = 'active'                               │
│ [Apply Filter] [Reset (F5)] [Quit (ESC)]                    │
└──────────────────────────────────────────────────────────────┘
┌─ Data (Use arrow keys to scroll) ───────────────────────────┐
│ id    │ name     │ age  │ email              │ status        │
│ 001   │ Alice    │ 28   │ alice@example.com  │ active        │
│ 003   │ Charlie  │ 30   │ charlie@example.com│ active        │
│ ...   │ ...      │ ...  │ ...                │ ...           │
└──────────────────────────────────────────────────────────────┘
Keys: ↑↓←→ Navigate | Tab: Next field | Enter: Apply | F5: Reset | ESC: Quit
```

### 5. 组合使用

```bash
# 限制 100 行 + 过滤条件
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet -l 100 -w "price > 1000"

# 限制 50 行 + 交互式模式
java -jar build/libs/parquet-view-cli-1.0-SNAPSHOT.jar file.parquet -l 50 -i
```

## WHERE 子句支持的操作符

### 比较操作符
- `=` - 等于
- `!=` 或 `<>` - 不等于
- `>` - 大于
- `<` - 小于
- `>=` - 大于等于
- `<=` - 小于等于

### NULL 检查
- `IS NULL` - 值为空
- `IS NOT NULL` - 值不为空

### 模糊匹配
- `LIKE` - 模式匹配（`%` 匹配任意字符，`_` 匹配单个字符）
- `NOT LIKE` - 反向模式匹配

### 范围检查
- `IN (value1, value2, ...)` - 值在列表中
- `NOT IN (value1, value2, ...)` - 值不在列表中

### 逻辑操作符
- `AND` - 与
- `OR` - 或
- `NOT` - 非
- `()` - 括号分组

## 示例

### 示例 1: 查看用户数据
```bash
java -jar parquet-view-cli.jar users.parquet -l 20
```

### 示例 2: 过滤活跃用户
```bash
java -jar parquet-view-cli.jar users.parquet -w "status = 'active' AND last_login IS NOT NULL"
```

### 示例 3: 查找特定邮箱域名
```bash
java -jar parquet-view-cli.jar users.parquet -w "email LIKE '%@gmail.com'"
```

### 示例 4: 复杂条件
```bash
java -jar parquet-view-cli.jar orders.parquet -w "(amount > 1000 OR priority = 'high') AND status != 'cancelled'"
```

### 示例 5: 交互式探索（全屏 TUI）⭐
```bash
java -jar parquet-view-cli.jar data.parquet -i

# 在交互界面中：
# 1. 使用箭头键浏览数据
# 2. 在 WHERE 输入框输入条件：age > 25
# 3. 点击 "Apply Filter" 或按 Enter
# 4. 查看过滤结果
# 5. 按 F5 重置
# 6. 输入新条件：status = 'active'
# 7. 按 ESC 退出
```

## 输出格式

CLI 会在终端中以表格形式显示数据：

```
┌──────────┬─────────┬──────────────────────┬────────────────────┐
│   id     │  name   │        email         │   created_at       │
├──────────┼─────────┼──────────────────────┼────────────────────┤
│        1 │ Alice   │ alice@example.com    │ 2024-01-01 10:00:00│
│        2 │ Bob     │ bob@example.com      │ 2024-01-02 11:30:00│
│        3 │ Charlie │ charlie@example.com  │ 2024-01-03 09:15:00│
└──────────┴─────────┴──────────────────────┴────────────────────┘
                    Total: 3 rows
```

## 帮助信息

```bash
java -jar parquet-view-cli.jar --help
```

## 注意事项

1. **交互式模式 vs 简单模式**：
   - 使用 `-i` 进入全屏交互模式，适合探索和多次过滤
   - 不使用 `-i` 则为简单表格输出模式，适合快速查看或脚本使用

2. 文件路径如果包含空格，需要用引号包裹：
   ```bash
   java -jar parquet-view-cli.jar "path/to/my file.parquet"
   ```

2. WHERE 条件中的字符串值需要用单引号：
   ```bash
   -w "name = 'John'"  # 正确
   -w "name = John"    # 错误
   ```

3. 默认最多显示 100 行，可以用 `-l` 参数调整

4. 时间类型会自动转换为可读格式（yyyy-MM-dd HH:mm:ss）

5. 过长的字符串会被截断显示（超过 50 字符）

6. **日志文件**：程序运行日志会保存到 `parquet-view.log` 文件中，错误信息也会输出到控制台
