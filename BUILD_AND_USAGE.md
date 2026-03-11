# Parquet View - 构建与使用指南

## 构建说明

### 构建所有版本
```bash
./gradlew buildAll
```

这将生成两个 JAR 文件：
- `build/libs/parquet-view-cli-1.0-SNAPSHOT.jar` - CLI 版本（150MB）
- `build/libs/parquet-view-gui-1.0-SNAPSHOT.jar` - GUI 版本（157MB）

### 单独构建

#### 构建 CLI 版本
```bash
./gradlew cliJar
# 或
./gradlew shadowJar
```

#### 构建 GUI 版本
```bash
./gradlew guiJar
```

## 使用说明

### CLI 版本（推荐用于命令行和服务器）

#### 特性
- ✅ 无需图形界面
- ✅ 默认交互式 TUI 模式
- ✅ 支持 SQL WHERE 过滤
- ✅ 实时语法验证
- ✅ 150MB，更轻量

#### 基本使用
```bash
# 默认进入交互式 TUI
java -jar parquet-view-cli-1.0-SNAPSHOT.jar data.parquet

# 简单模式（静态表格输出）
java -jar parquet-view-cli-1.0-SNAPSHOT.jar data.parquet -s

# 带过滤条件
java -jar parquet-view-cli-1.0-SNAPSHOT.jar data.parquet -w "age > 18"

# 限制行数
java -jar parquet-view-cli-1.0-SNAPSHOT.jar data.parquet -l 1000
```

#### 交互式 TUI 模式
进入后可以：
- 使用箭头键浏览数据
- 输入 WHERE 条件实时过滤
- 按 Enter 应用过滤
- 按 F5 重置过滤
- 按 ESC 退出

详细说明见 [CLI_USAGE.md](CLI_USAGE.md)

### GUI 版本（推荐用于桌面环境）

#### 特性
- ✅ 图形界面操作
- ✅ 文件选择对话框
- ✅ 表格视图
- ✅ WHERE 条件过滤
- ✅ 157MB，包含 JavaFX

#### 基本使用
```bash
# 启动 GUI 应用
java -jar parquet-view-gui-1.0-SNAPSHOT.jar
```

在 GUI 中：
1. 点击"选择文件"按钮选择 Parquet 文件
2. 文件自动加载到表格中
3. 在 WHERE 输入框输入过滤条件
4. 点击"应用过滤"查看结果
5. 点击"重新加载"恢复原始数据

## 版本对比

| 特性 | CLI 版本 | GUI 版本 |
|------|----------|----------|
| 文件大小 | 150MB | 157MB |
| 运行环境 | 任何支持 Java 的环境 | 需要图形界面 |
| 交互方式 | 终端 TUI | 图形界面 |
| 数据浏览 | 键盘导航 | 鼠标+键盘 |
| 过滤功能 | 实时验证 | 输入验证 |
| 适用场景 | 服务器、脚本、SSH | 桌面、开发环境 |

## 技术栈

### 共同依赖
- Kotlin 2.3.10
- Apache Parquet 1.17.0
- Apache Hadoop 3.4.3
- JSqlParser 5.3
- Logback 1.5.32

### CLI 专用
- Clikt 5.1.0（命令行参数解析）
- Lanterna 3.1.3（终端 UI）
- Picnic 0.7.0（表格渲染）

### GUI 专用
- JavaFX 21.0.6
- ControlsFX 11.2.1

## 打包技术

使用 Gradle Shadow Plugin 9.2.0+ 进行 Fat JAR 打包：
- 自动合并依赖
- 处理 service 文件
- 排除签名冲突
- 优化 JAR 大小

## 系统要求

- Java 21 或更高版本
- 内存：建议至少 512MB
- 操作系统：
  - CLI: Linux, macOS, Windows（任何支持终端的系统）
  - GUI: Linux, macOS, Windows（需要图形界面支持）

## 开发

### 运行开发版本

```bash
# 运行 GUI
./gradlew run

# 运行 CLI（需要传参数）
./gradlew runCli --args="path/to/file.parquet -i"
```

### 测试

```bash
./gradlew test
```

### 清理

```bash
./gradlew clean
```

## 日志

程序运行时会在当前目录生成 `parquet-view.log` 日志文件，包含：
- 文件加载信息
- 过滤操作记录
- 错误详情

## 问题排查

### CLI 模式表格不显示
检查终端是否支持 ANSI 转义序列，建议使用：
- Linux: 任何现代终端
- macOS: Terminal.app 或 iTerm2
- Windows: Windows Terminal（不推荐 CMD）

### GUI 模式无法启动
确保系统支持 JavaFX 并安装了图形界面环境

### 内存不足
使用 `-Xmx` 参数增加内存：
```bash
java -Xmx2G -jar parquet-view-cli-1.0-SNAPSHOT.jar data.parquet
```

## 许可证

请查看项目根目录的 LICENSE 文件
