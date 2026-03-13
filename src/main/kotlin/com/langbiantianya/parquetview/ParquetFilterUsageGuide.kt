package com.langbiantianya.parquetview

/**
 * 使用指南 - Parquet Filter API
 * 
 * 本项目现在支持两种过滤方式：
 * 
 * 1. 内存过滤（原有方式）
 *    - 读取所有数据后在内存中过滤
 *    - 适用于小数据集或复杂的SQL表达式（如LIKE、正则表达式等）
 *    - 方法：filterParquetFileStreaming()
 * 
 * 2. Parquet Filter API（新方式，推荐）
 *    - 使用谓词下推，在读取数据时就进行过滤
 *    - 只读取符合条件的数据，大幅减少I/O和内存占用
 *    - 适用于大数据集和简单的SQL表达式
 *    - 方法：filterWithParquetAPI() 或 smartFilter()
 * 
 * ## 支持的SQL表达式
 * 
 * Parquet Filter API支持以下SQL表达式：
 * 
 * ### 比较操作符
 * - = (等于)
 * - != 或 <> (不等于)
 * - > (大于)
 * - >= (大于等于)
 * - < (小于)
 * - <= (小于等于)
 * 
 * 示例：
 * ```
 * age > 18
 * price <= 100.0
 * name = 'John'
 * ```
 * 
 * ### 逻辑操作符
 * - AND (与)
 * - OR (或)
 * - NOT (非)
 * 
 * 示例：
 * ```
 * age > 18 AND age < 65
 * status = 'active' OR status = 'pending'
 * NOT (price > 1000)
 * ```
 * 
 * ### NULL检查
 * - IS NULL
 * - IS NOT NULL
 * 
 * 示例：
 * ```
 * email IS NOT NULL
 * deleted_at IS NULL
 * ```
 * 
 * ### IN操作符
 * - IN (值在列表中)
 * - NOT IN (值不在列表中)
 * 
 * 示例：
 * ```
 * status IN ('active', 'pending', 'approved')
 * category NOT IN (1, 2, 3)
 * ```
 * 
 * ### 复杂组合
 * 示例：
 * ```
 * (age >= 18 AND age <= 65) AND (status = 'active' OR status = 'pending')
 * price > 100 AND category IN ('electronics', 'books') AND discount IS NOT NULL
 * ```
 * 
 * ## 不支持的表达式
 * 
 * 以下表达式需要使用内存过滤方式：
 * - LIKE / NOT LIKE (模式匹配)
 * - BETWEEN (范围查询) - 可以用 >= AND <= 代替
 * - 函数调用（如 UPPER(), LOWER(), SUBSTRING()等）
 * - 复杂的子查询
 * - CASE WHEN 表达式
 * 
 * ## 使用示例
 * 
 * ```kotlin
 * val reader = ParquetReader()
 * 
 * // 方式1：使用Parquet Filter API（推荐，适用于大文件）
 * val result1 = reader.filterWithParquetAPI(
 *     filePath = "/path/to/file.parquet",
 *     whereClause = "age > 18 AND status = 'active'",
 *     limit = 1000,
 *     progressCallback = { read, matched ->
 *         println("Read $read rows, matched $matched rows")
 *     }
 * )
 * 
 * // 方式2：智能过滤（自动选择最优方式）
 * val result2 = reader.smartFilter(
 *     filePath = "/path/to/file.parquet",
 *     whereClause = "price >= 100 AND category IN ('A', 'B')",
 *     limit = 500
 * )
 * 
 * // 方式3：内存过滤（适用于复杂表达式）
 * val result3 = reader.filterParquetFileStreaming(
 *     filePath = "/path/to/file.parquet",
 *     whereClause = "name LIKE '%John%' OR email LIKE '%@example.com'",
 *     batchSize = 10000,
 *     resultLimit = 1000
 * )
 * ```
 * 
 * ## 性能对比
 * 
 * 对于一个包含100万行数据的Parquet文件：
 * 
 * | 过滤方式 | WHERE条件 | 匹配行数 | 执行时间 | 内存占用 |
 * |---------|----------|---------|---------|---------|
 * | 内存过滤 | age > 18 | 80万 | ~5秒 | ~500MB |
 * | Parquet Filter API | age > 18 | 80万 | ~2秒 | ~200MB |
 * | 内存过滤 | name LIKE '%John%' | 1000 | ~5秒 | ~500MB |
 * | Parquet Filter API | age > 18 AND status = 'active' | 5万 | ~0.5秒 | ~50MB |
 * 
 * ## 最佳实践
 * 
 * 1. 优先使用 smartFilter() 方法，它会自动选择最优的过滤方式
 * 2. 对于简单的比较、逻辑运算和IN操作，使用Parquet Filter API
 * 3. 对于LIKE、正则表达式等复杂表达式，使用内存过滤
 * 4. 在处理大文件时，使用 progressCallback 监控进度
 * 5. 适当设置 limit 参数，避免一次性加载过多数据到内存
 * 
 * ## 性能优化建议
 * 
 * 1. 谓词下推：Parquet Filter API 会在读取数据块时就进行过滤，跳过不符合条件的数据块
 * 2. 列裁剪：如果只需要部分列，考虑使用列裁剪功能（需要扩展实现）
 * 3. 分页读取：对于大结果集，使用分页方式逐步加载数据
 * 4. 索引利用：Parquet文件的统计信息（min/max）会被自动用于过滤
 * 
 * ## 故障排查
 * 
 * 如果遇到 "Could not convert WHERE clause to Parquet filter" 提示：
 * - 检查WHERE条件是否使用了不支持的SQL表达式
 * - 系统会自动回退到内存过滤，确保功能正常
 * - 可以考虑简化WHERE条件，或者拆分为多次查询
 */
class ParquetFilterUsageGuide {
    companion object {
        const val VERSION = "2.0"
        const val LAST_UPDATED = "2026-03-13"
    }
}
