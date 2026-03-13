package com.langbiantianya.parquetview

import net.sf.jsqlparser.expression.*
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.conditional.OrExpression
import net.sf.jsqlparser.expression.operators.relational.*
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Column
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import org.apache.parquet.hadoop.ParquetReader as ApacheParquetReader

class ParquetReader {
    
    private val filterConverter = SqlToParquetFilterConverter()

    data class ParquetData(
        val schema: List<String>,
        val rows: List<Map<String, Any?>>
    )
    
    data class PagedParquetData(
        val schema: List<String>,
        val rows: List<Map<String, Any?>>,
        val currentPage: Int,
        val totalRowsRead: Int,
        val hasMore: Boolean
    )
    
    /**
     * 过滤结果 - 只保存匹配行的索引，减少内存占用
     */
    data class FilterResult(
        val schema: List<String>,
        val matchedRowIndices: List<Long>,  // 匹配行的索引位置
        val totalRowsScanned: Int
    )

    fun readParquetFile(filePath: String, limit: Int = 10000): ParquetData {
        val conf = Configuration()
        
        // Configure Hadoop to use local filesystem
        conf.set("fs.defaultFS", "file:///")
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        
        val path = Path(filePath)

        val schema = mutableListOf<String>()
        val rows = mutableListOf<Map<String, Any?>>()

        val readSupport = GroupReadSupport()
        val reader = ApacheParquetReader.builder<Group>(readSupport, path)
            .withConf(conf)
            .build()

        reader.use { parquetReader ->
            var group: Group? = parquetReader.read()
            var fileSchema: GroupType? = null

            // Get schema from first record
            if (group != null) {
                fileSchema = group.type
                fileSchema.fields.forEach { field ->
                    schema.add(field.name)
                }
            }

            var rowCount = 0
            while (group != null && rowCount < limit) {
                val rowMap = mutableMapOf<String, Any?>()

                fileSchema?.fields?.forEachIndexed { index, field ->
                    try {
                        val value = if (group.getFieldRepetitionCount(index) > 0) {
                            extractValue(group, index, field)
                        } else {
                            null
                        }
                        rowMap[field.name] = value
                    } catch (e: Exception) {
                        rowMap[field.name] = null
                    }
                }

                rows.add(rowMap)
                rowCount++
                group = parquetReader.read()
            }
        }

        return ParquetData(schema, rows)
    }
    
    /**
     * 分页读取Parquet文件
     * @param filePath 文件路径
     * @param pageSize 每页行数
     * @param skip 跳过的行数（用于实现分页）
     * @return 分页数据
     */
    fun readParquetFilePaged(filePath: String, pageSize: Int, skip: Int = 0): PagedParquetData {
        val conf = Configuration()
        
        // Configure Hadoop to use local filesystem
        conf.set("fs.defaultFS", "file:///")
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        
        val path = Path(filePath)

        val schema = mutableListOf<String>()
        val rows = mutableListOf<Map<String, Any?>>()

        val readSupport = GroupReadSupport()
        val reader = ApacheParquetReader.builder<Group>(readSupport, path)
            .withConf(conf)
            .build()

        var hasMore = false
        
        reader.use { parquetReader ->
            var group: Group? = parquetReader.read()
            var fileSchema: GroupType? = null

            // Get schema from first record
            if (group != null) {
                fileSchema = group.type
                fileSchema.fields.forEach { field ->
                    schema.add(field.name)
                }
            }

            var rowCount = 0
            var totalRowsRead = 0
            
            // Skip rows
            while (group != null && rowCount < skip) {
                rowCount++
                totalRowsRead++
                group = parquetReader.read()
            }
            
            // Reset rowCount for actual page reading
            rowCount = 0
            
            // Read page
            while (group != null && rowCount < pageSize) {
                val rowMap = mutableMapOf<String, Any?>()

                fileSchema?.fields?.forEachIndexed { index, field ->
                    try {
                        val value = if (group.getFieldRepetitionCount(index) > 0) {
                            extractValue(group, index, field)
                        } else {
                            null
                        }
                        rowMap[field.name] = value
                    } catch (e: Exception) {
                        rowMap[field.name] = null
                    }
                }

                rows.add(rowMap)
                rowCount++
                totalRowsRead++
                group = parquetReader.read()
            }
            
            // Check if there are more rows
            hasMore = group != null
        }

        val currentPage = (skip / pageSize) + 1
        return PagedParquetData(schema, rows, currentPage, skip + rows.size, hasMore)
    }
    
    /**
     * 流式过滤Parquet文件 - 分批读取和过滤，减少内存压力
     * @param filePath 文件路径
     * @param whereClause WHERE条件
     * @param batchSize 每批读取的行数
     * @param resultLimit 结果限制（返回的最大匹配行数）
     * @param progressCallback 进度回调 (已读取行数, 已匹配行数)
     * @return 过滤后的数据
     */
    fun filterParquetFileStreaming(
        filePath: String, 
        whereClause: String, 
        batchSize: Int = 10000,
        resultLimit: Int = Int.MAX_VALUE,
        progressCallback: ((Int, Int) -> Unit)? = null
    ): ParquetData {
        val conf = Configuration()
        
        // Configure Hadoop to use local filesystem
        conf.set("fs.defaultFS", "file:///")
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        
        val path = Path(filePath)
        val schema = mutableListOf<String>()
        val filteredRows = mutableListOf<Map<String, Any?>>()
        
        // Parse WHERE clause once
        val expression = CCJSqlParserUtil.parseCondExpression(whereClause)
        
        val readSupport = GroupReadSupport()
        val reader = ApacheParquetReader.builder<Group>(readSupport, path)
            .withConf(conf)
            .build()
        
        reader.use { parquetReader ->
            var group: Group? = parquetReader.read()
            var fileSchema: GroupType? = null
            
            // Get schema from first record
            if (group != null) {
                fileSchema = group.type
                fileSchema.fields.forEach { field ->
                    schema.add(field.name)
                }
            }
            
            var totalRead = 0
            var batchCount = 0
            val batchRows = mutableListOf<Map<String, Any?>>()
            
            while (group != null && filteredRows.size < resultLimit) {
                val rowMap = mutableMapOf<String, Any?>()
                
                fileSchema?.fields?.forEachIndexed { index, field ->
                    try {
                        val value = if (group.getFieldRepetitionCount(index) > 0) {
                            extractValue(group, index, field)
                        } else {
                            null
                        }
                        rowMap[field.name] = value
                    } catch (e: Exception) {
                        rowMap[field.name] = null
                    }
                }
                
                batchRows.add(rowMap)
                totalRead++
                batchCount++
                
                // Process batch
                if (batchCount >= batchSize) {
                    // Filter this batch
                    val filteredBatch = batchRows.filter { row ->
                        try {
                            evaluateExpression(expression, row)
                        } catch (e: Exception) {
                            false
                        }
                    }
                    
                    filteredRows.addAll(filteredBatch.take(resultLimit - filteredRows.size))
                    
                    // Report progress
                    progressCallback?.invoke(totalRead, filteredRows.size)
                    
                    // Clear batch
                    batchRows.clear()
                    batchCount = 0
                }
                
                group = parquetReader.read()
            }
            
            // Process remaining rows in final batch
            if (batchRows.isNotEmpty() && filteredRows.size < resultLimit) {
                val filteredBatch = batchRows.filter { row ->
                    try {
                        evaluateExpression(expression, row)
                    } catch (e: Exception) {
                        false
                    }
                }
                
                filteredRows.addAll(filteredBatch.take(resultLimit - filteredRows.size))
                progressCallback?.invoke(totalRead, filteredRows.size)
            }
        }
        
        return ParquetData(schema, filteredRows)
    }
    
    /**
     * 流式过滤 - 只返回匹配行的索引，不保存实际数据
     * 内存占用最小化
     */
    fun filterParquetFileIndices(
        filePath: String,
        whereClause: String,
        progressCallback: ((Int, Int) -> Unit)? = null
    ): FilterResult {
        val conf = Configuration()
        
        // Configure Hadoop to use local filesystem
        conf.set("fs.defaultFS", "file:///")
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        
        val path = Path(filePath)
        val schema = mutableListOf<String>()
        val matchedIndices = mutableListOf<Long>()
        
        // Parse WHERE clause once
        val expression = CCJSqlParserUtil.parseCondExpression(whereClause)
        
        val readSupport = GroupReadSupport()
        val reader = ApacheParquetReader.builder<Group>(readSupport, path)
            .withConf(conf)
            .build()
        
        reader.use { parquetReader ->
            var group: Group? = parquetReader.read()
            var fileSchema: GroupType? = null
            
            // Get schema from first record
            if (group != null) {
                fileSchema = group.type
                fileSchema.fields.forEach { field ->
                    schema.add(field.name)
                }
            }
            
            var rowIndex = 0L
            var lastReportedIndex = 0
            
            while (group != null) {
                val rowMap = mutableMapOf<String, Any?>()
                
                fileSchema?.fields?.forEachIndexed { index, field ->
                    try {
                        val value = if (group.getFieldRepetitionCount(index) > 0) {
                            extractValue(group, index, field)
                        } else {
                            null
                        }
                        rowMap[field.name] = value
                    } catch (e: Exception) {
                        rowMap[field.name] = null
                    }
                }
                
                // Check if row matches filter
                try {
                    if (evaluateExpression(expression, rowMap)) {
                        matchedIndices.add(rowIndex)
                    }
                } catch (e: Exception) {
                    // Row doesn't match or error - skip it
                }
                
                rowIndex++
                
                // Report progress every 10000 rows
                if (rowIndex - lastReportedIndex >= 10000) {
                    progressCallback?.invoke(rowIndex.toInt(), matchedIndices.size)
                    lastReportedIndex = rowIndex.toInt()
                }
                
                group = parquetReader.read()
            }
            
            // Final progress report
            progressCallback?.invoke(rowIndex.toInt(), matchedIndices.size)
        }
        
        return FilterResult(schema, matchedIndices, matchedIndices.size)
    }
    
    /**
     * 根据索引列表读取指定的行
     * 用于分页显示过滤结果
     */
    fun readRowsByIndices(
        filePath: String,
        rowIndices: List<Long>,
        schema: List<String>
    ): ParquetData {
        if (rowIndices.isEmpty()) {
            return ParquetData(schema, emptyList())
        }
        
        val conf = Configuration()
        conf.set("fs.defaultFS", "file:///")
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        
        val path = Path(filePath)
        val rows = mutableListOf<Map<String, Any?>>()
        
        // Sort indices for efficient sequential reading
        val sortedIndices = rowIndices.sorted().toSet()
        
        val readSupport = GroupReadSupport()
        val reader = ApacheParquetReader.builder<Group>(readSupport, path)
            .withConf(conf)
            .build()
        
        reader.use { parquetReader ->
            var group: Group? = parquetReader.read()
            var fileSchema: GroupType? = null
            
            if (group != null) {
                fileSchema = group.type
            }
            
            var currentIndex = 0L
            
            while (group != null && rows.size < rowIndices.size) {
                if (currentIndex in sortedIndices) {
                    val rowMap = mutableMapOf<String, Any?>()
                    
                    fileSchema?.fields?.forEachIndexed { index, field ->
                        try {
                            val value = if (group.getFieldRepetitionCount(index) > 0) {
                                extractValue(group, index, field)
                            } else {
                                null
                            }
                            rowMap[field.name] = value
                        } catch (e: Exception) {
                            rowMap[field.name] = null
                        }
                    }
                    
                    rows.add(rowMap)
                }
                
                currentIndex++
                group = parquetReader.read()
            }
        }
        
        return ParquetData(schema, rows)
    }
    
    /**
     * 使用Parquet Filter API进行高效过滤（谓词下推）
     * 这种方法在读取时就过滤数据，而不是读取后再过滤，效率更高
     * 
     * @param filePath 文件路径
     * @param whereClause WHERE条件
     * @param limit 结果限制
     * @param progressCallback 进度回调
     * @return 过滤后的数据
     */
    fun filterWithParquetAPI(
        filePath: String,
        whereClause: String,
        limit: Int = Int.MAX_VALUE,
        progressCallback: ((Int, Int) -> Unit)? = null
    ): ParquetData {
        val conf = Configuration()
        
        // Configure Hadoop to use local filesystem
        conf.set("fs.defaultFS", "file:///")
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        
        val path = Path(filePath)
        val schema = mutableListOf<String>()
        val rows = mutableListOf<Map<String, Any?>>()
        
        // 转换SQL WHERE条件为Parquet FilterPredicate
        val filterPredicate = filterConverter.convertToFilter(whereClause)
        
        val readSupport = GroupReadSupport()
        val readerBuilder = ApacheParquetReader.builder<Group>(readSupport, path)
            .withConf(conf)
        
        // 如果成功转换为FilterPredicate，则应用过滤
        if (filterPredicate != null) {
            readerBuilder.withFilter(FilterCompat.get(filterPredicate))
            println("Applied Parquet filter predicate for WHERE clause: $whereClause")
        } else {
            println("Could not convert WHERE clause to Parquet filter, will filter in memory")
        }
        
        val reader = readerBuilder.build()
        
        reader.use { parquetReader ->
            var group: Group? = parquetReader.read()
            var fileSchema: GroupType? = null
            
            // Get schema from first record
            if (group != null) {
                fileSchema = group.type
                fileSchema.fields.forEach { field ->
                    schema.add(field.name)
                }
            }
            
            var rowCount = 0
            var totalRead = 0
            
            while (group != null && rowCount < limit) {
                val rowMap = mutableMapOf<String, Any?>()
                
                fileSchema?.fields?.forEachIndexed { index, field ->
                    try {
                        val value = if (group.getFieldRepetitionCount(index) > 0) {
                            extractValue(group, index, field)
                        } else {
                            null
                        }
                        rowMap[field.name] = value
                    } catch (e: Exception) {
                        rowMap[field.name] = null
                    }
                }
                
                // 如果没有使用Parquet过滤器，则在内存中过滤
                val shouldInclude = if (filterPredicate == null && whereClause.isNotBlank()) {
                    try {
                        val expression = CCJSqlParserUtil.parseCondExpression(whereClause)
                        evaluateExpression(expression, rowMap)
                    } catch (e: Exception) {
                        false
                    }
                } else {
                    true // Parquet已经过滤或无需过滤
                }
                
                if (shouldInclude) {
                    rows.add(rowMap)
                    rowCount++
                }
                
                totalRead++
                
                // Report progress every 10000 rows
                if (totalRead % 10000 == 0) {
                    progressCallback?.invoke(totalRead, rowCount)
                }
                
                group = parquetReader.read()
            }
            
            // Final progress report
            progressCallback?.invoke(totalRead, rowCount)
        }
        
        return ParquetData(schema, rows)
    }
    
    /**
     * 智能过滤 - 优先使用Parquet Filter API，回退到内存过滤
     * 这是推荐的过滤方法
     * 
     * @param filePath 文件路径
     * @param whereClause WHERE条件
     * @param limit 结果限制
     * @param progressCallback 进度回调
     * @return 过滤后的数据
     */
    fun smartFilter(
        filePath: String,
        whereClause: String,
        limit: Int = Int.MAX_VALUE,
        progressCallback: ((Int, Int) -> Unit)? = null
    ): ParquetData {
        return filterWithParquetAPI(filePath, whereClause, limit, progressCallback)
    }

    private fun extractValue(group: Group, fieldIndex: Int, field: Type): Any? {
        return try {
            val primitiveType = field.asPrimitiveType()
            val logicalType = primitiveType.logicalTypeAnnotation
            
            // Handle logical types (dates, timestamps, etc.)
            when (logicalType) {
                is LogicalTypeAnnotation.TimestampLogicalTypeAnnotation -> {
                    val value = when (primitiveType.primitiveTypeName) {
                        PrimitiveType.PrimitiveTypeName.INT64 -> group.getLong(fieldIndex, 0)
                        PrimitiveType.PrimitiveTypeName.INT96 -> {
                            // INT96 is deprecated but still used for timestamps
                            return convertInt96ToTimestamp(group.getInt96(fieldIndex, 0))
                        }
                        else -> return group.getValueToString(fieldIndex, 0)
                    }
                    
                    // Convert based on timestamp unit
                    val instant = when (logicalType.unit) {
                        LogicalTypeAnnotation.TimeUnit.MILLIS -> Instant.ofEpochMilli(value)
                        LogicalTypeAnnotation.TimeUnit.MICROS -> Instant.ofEpochMilli(value / 1000)
                        LogicalTypeAnnotation.TimeUnit.NANOS -> Instant.ofEpochMilli(value / 1_000_000)
                    }
                    
                    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                        .withZone(ZoneId.systemDefault())
                    return formatter.format(instant)
                }
                
                is LogicalTypeAnnotation.DateLogicalTypeAnnotation -> {
                    // Date is stored as INT32 (days since epoch)
                    val daysSinceEpoch = group.getInteger(fieldIndex, 0)
                    val date = LocalDate.ofEpochDay(daysSinceEpoch.toLong())
                    return date.format(DateTimeFormatter.ISO_LOCAL_DATE)
                }
                
                is LogicalTypeAnnotation.TimeLogicalTypeAnnotation -> {
                    val value = when (primitiveType.primitiveTypeName) {
                        PrimitiveType.PrimitiveTypeName.INT32 -> group.getInteger(fieldIndex, 0).toLong()
                        PrimitiveType.PrimitiveTypeName.INT64 -> group.getLong(fieldIndex, 0)
                        else -> return group.getValueToString(fieldIndex, 0)
                    }
                    
                    // Convert based on time unit
                    val millis = when (logicalType.unit) {
                        LogicalTypeAnnotation.TimeUnit.MILLIS -> value
                        LogicalTypeAnnotation.TimeUnit.MICROS -> value / 1000
                        LogicalTypeAnnotation.TimeUnit.NANOS -> value / 1_000_000
                    }
                    
                    val seconds = millis / 1000
                    val hours = seconds / 3600
                    val minutes = (seconds % 3600) / 60
                    val secs = seconds % 60
                    val millisPart = millis % 1000
                    
                    return String.format("%02d:%02d:%02d.%03d", hours, minutes, secs, millisPart)
                }
            }
            
            // Handle primitive types without logical type annotation
            when (primitiveType.primitiveTypeName) {
                PrimitiveType.PrimitiveTypeName.INT32 ->
                    group.getInteger(fieldIndex, 0)

                PrimitiveType.PrimitiveTypeName.INT64 ->
                    group.getLong(fieldIndex, 0)

                PrimitiveType.PrimitiveTypeName.FLOAT ->
                    group.getFloat(fieldIndex, 0)

                PrimitiveType.PrimitiveTypeName.DOUBLE ->
                    group.getDouble(fieldIndex, 0)

                PrimitiveType.PrimitiveTypeName.BOOLEAN ->
                    group.getBoolean(fieldIndex, 0)

                PrimitiveType.PrimitiveTypeName.BINARY,
                PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ->
                    group.getString(fieldIndex, 0)

                PrimitiveType.PrimitiveTypeName.INT96 ->
                    convertInt96ToTimestamp(group.getInt96(fieldIndex, 0))

                else -> group.getValueToString(fieldIndex, 0)
            }
        } catch (e: Exception) {
            group.getValueToString(fieldIndex, 0)
        }
    }
    
    private fun convertInt96ToTimestamp(int96: org.apache.parquet.io.api.Binary): String {
        // INT96: 12 bytes - first 8 bytes are nanoseconds of day, last 4 bytes are Julian day number
        val bytes = int96.bytes
        
        // Extract nanoseconds (first 8 bytes, little-endian)
        var nanoOfDay = 0L
        for (i in 0..7) {
            nanoOfDay = nanoOfDay or ((bytes[i].toLong() and 0xFF) shl (i * 8))
        }
        
        // Extract Julian day (last 4 bytes, little-endian)
        var julianDay = 0
        for (i in 8..11) {
            julianDay = julianDay or ((bytes[i].toInt() and 0xFF) shl ((i - 8) * 8))
        }
        
        // Convert Julian day to epoch day (Julian day 2440588 = Unix epoch 1970-01-01)
        val epochDay = julianDay - 2440588L
        
        // Convert to timestamp
        val epochSeconds = epochDay * 86400L + nanoOfDay / 1_000_000_000L
        val instant = Instant.ofEpochSecond(epochSeconds, nanoOfDay % 1_000_000_000L)
        
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault())
        return formatter.format(instant)
    }

    fun filterData(data: ParquetData, whereClause: String): ParquetData {
        if (whereClause.isBlank()) {
            return data
        }

        return try {
            // Parse WHERE clause using JSqlParser
            val expression = CCJSqlParserUtil.parseCondExpression(whereClause)

            val filteredRows = data.rows.filter { row ->
                evaluateExpression(expression, row)
            }

            ParquetData(data.schema, filteredRows)
        } catch (e: Exception) {
            println("Error parsing WHERE clause: ${e.message}")
            e.printStackTrace()
            data // Return original data if parsing fails
        }
    }

    private fun evaluateExpression(expression: Expression, row: Map<String, Any?>): Boolean {
        return when (expression) {
            is AndExpression -> {
                evaluateExpression(expression.leftExpression, row) &&
                        evaluateExpression(expression.rightExpression, row)
            }

            is OrExpression -> {
                evaluateExpression(expression.leftExpression, row) ||
                        evaluateExpression(expression.rightExpression, row)
            }

            is ParenthesedExpressionList<*> -> {
                // Handle parenthesized expressions
                if (expression.isEmpty()) {
                    true
                } else {
                    // For a single expression in parentheses, evaluate it
                    expression.firstOrNull()?.let { evaluateExpression(it as Expression, row) } ?: true
                }
            }

            is ComparisonOperator -> {
                evaluateComparison(expression, row)
            }

            is IsNullExpression -> {
                val column = expression.leftExpression as? Column
                val columnName = column?.columnName ?: return false
                val value = row[columnName]

                if (expression.isNot) {
                    value != null
                } else {
                    value == null
                }
            }

            is LikeExpression -> {
                val column = expression.leftExpression as? Column
                val columnName = column?.columnName ?: return false
                val value = row[columnName]?.toString() ?: ""

                val pattern = when (val right = expression.rightExpression) {
                    is StringValue -> right.value
                    else -> right.toString()
                }

                val regexPattern = pattern
                    .replace("%", ".*")
                    .replace("_", ".")

                val matches = value.matches(Regex(regexPattern, RegexOption.IGNORE_CASE))

                if (expression.isNot) !matches else matches
            }

            is InExpression -> {
                val column = expression.leftExpression as? Column
                val columnName = column?.columnName ?: return false
                val value = row[columnName]?.toString()

                val rightItemsList = expression.rightExpression as? ExpressionList<*>
                val items = rightItemsList?.let { list ->
                    list.map { item ->
                        when (item) {
                            is StringValue -> item.value
                            is LongValue -> item.value.toString()
                            is DoubleValue -> item.value.toString()
                            else -> item.toString()
                        }
                    }
                } ?: emptyList()

                val contains = items.any { it.equals(value, ignoreCase = true) }

                if (expression.isNot) !contains else contains
            }

            is NotExpression -> {
                !evaluateExpression(expression.expression, row)
            }

            else -> {
                println("Unsupported expression type: ${expression.javaClass.simpleName}")
                true
            }
        }
    }

    private fun evaluateComparison(comparison: ComparisonOperator, row: Map<String, Any?>): Boolean {
        val column = comparison.leftExpression as? Column ?: return false
        val columnName = column.columnName
        val actualValue = row[columnName]

        val expectedValue = when (val right = comparison.rightExpression) {
            is StringValue -> right.value
            is LongValue -> right.value
            is DoubleValue -> right.value
            else -> right.toString()
        }

        return when (comparison) {
            is EqualsTo -> compareEquals(actualValue, expectedValue)
            is NotEqualsTo -> !compareEquals(actualValue, expectedValue)
            is GreaterThan -> compareNumeric(actualValue, expectedValue) { a, b -> a > b }
            is GreaterThanEquals -> compareNumeric(actualValue, expectedValue) { a, b -> a >= b }
            is MinorThan -> compareNumeric(actualValue, expectedValue) { a, b -> a < b }
            is MinorThanEquals -> compareNumeric(actualValue, expectedValue) { a, b -> a <= b }
            else -> false
        }
    }

    private fun compareEquals(actualValue: Any?, expectedValue: Any?): Boolean {
        if (actualValue == null || expectedValue == null) {
            return actualValue == expectedValue
        }

        // Try numeric comparison first
        val actualNum = toNumber(actualValue)
        val expectedNum = toNumber(expectedValue)

        if (actualNum != null && expectedNum != null) {
            return actualNum == expectedNum
        }

        // Fall back to string comparison
        return actualValue.toString().equals(expectedValue.toString(), ignoreCase = true)
    }

    private fun compareNumeric(
        actualValue: Any?,
        expectedValue: Any?,
        comparator: (Double, Double) -> Boolean
    ): Boolean {
        val actualNum = toNumber(actualValue) ?: return false
        val expectedNum = toNumber(expectedValue) ?: return false

        return comparator(actualNum, expectedNum)
    }

    private fun toNumber(value: Any?): Double? {
        return when (value) {
            is Number -> value.toDouble()
            is String -> value.toDoubleOrNull()
            else -> value?.toString()?.toDoubleOrNull()
        }
    }

    private fun compareValues(
        actualValue: Any?,
        thresholdStr: String,
        comparator: (Double, Double) -> Boolean
    ): Boolean {
        // Try to compare as numbers
        val actualDouble = when (actualValue) {
            is Number -> actualValue.toDouble()
            is String -> actualValue.toDoubleOrNull()
            else -> actualValue?.toString()?.toDoubleOrNull()
        } ?: return false

        val thresholdDouble = thresholdStr.toDoubleOrNull() ?: return false

        return comparator(actualDouble, thresholdDouble)
    }
}
