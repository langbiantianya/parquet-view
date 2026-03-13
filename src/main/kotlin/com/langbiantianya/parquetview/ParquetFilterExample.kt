package com.langbiantianya.parquetview

/**
 * 示例：使用Parquet Filter API进行高效过滤
 * 
 * 这个示例展示了如何使用新的Parquet Filter API进行高效的数据过滤
 */
object ParquetFilterExample {
    
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.isEmpty()) {
            printUsage()
            return
        }
        
        val filePath = args[0]
        val reader = ParquetReader()
        
        println("=".repeat(80))
        println("Parquet Filter API Example")
        println("=".repeat(80))
        println()
        
        // 示例1: 简单的比较过滤
        println("Example 1: Simple comparison filter")
        println("-".repeat(80))
        val example1 = "age > 18"
        println("WHERE: $example1")
        
        val result1 = reader.smartFilter(
            filePath = filePath,
            whereClause = example1,
            limit = 10
        ) { read, matched ->
            if (read % 10000 == 0) {
                println("Progress: Read $read rows, matched $matched rows")
            }
        }
        
        println("Result: ${result1.rows.size} rows matched")
        printRows(result1)
        println()
        
        // 示例2: AND操作符
        println("Example 2: AND operator")
        println("-".repeat(80))
        val example2 = "age > 18 AND age < 65"
        println("WHERE: $example2")
        
        val result2 = reader.smartFilter(
            filePath = filePath,
            whereClause = example2,
            limit = 10
        )
        
        println("Result: ${result2.rows.size} rows matched")
        printRows(result2)
        println()
        
        // 示例3: OR操作符
        println("Example 3: OR operator")
        println("-".repeat(80))
        val example3 = "status = 'active' OR status = 'pending'"
        println("WHERE: $example3")
        
        val result3 = reader.smartFilter(
            filePath = filePath,
            whereClause = example3,
            limit = 10
        )
        
        println("Result: ${result3.rows.size} rows matched")
        printRows(result3)
        println()
        
        // 示例4: IN操作符
        println("Example 4: IN operator")
        println("-".repeat(80))
        val example4 = "category IN ('A', 'B', 'C')"
        println("WHERE: $example4")
        
        val result4 = reader.smartFilter(
            filePath = filePath,
            whereClause = example4,
            limit = 10
        )
        
        println("Result: ${result4.rows.size} rows matched")
        printRows(result4)
        println()
        
        // 示例5: 复杂组合
        println("Example 5: Complex combination")
        println("-".repeat(80))
        val example5 = "(age >= 18 AND age <= 65) AND (status = 'active' OR status = 'pending')"
        println("WHERE: $example5")
        
        val result5 = reader.smartFilter(
            filePath = filePath,
            whereClause = example5,
            limit = 10
        )
        
        println("Result: ${result5.rows.size} rows matched")
        printRows(result5)
        println()
        
        // 示例6: NULL检查
        println("Example 6: NULL check")
        println("-".repeat(80))
        val example6 = "email IS NOT NULL"
        println("WHERE: $example6")
        
        val result6 = reader.smartFilter(
            filePath = filePath,
            whereClause = example6,
            limit = 10
        )
        
        println("Result: ${result6.rows.size} rows matched")
        printRows(result6)
        println()
        
        println("=".repeat(80))
        println("Examples completed!")
        println("=".repeat(80))
    }
    
    private fun printUsage() {
        println("""
            Usage: java -jar parquet-view-cli.jar <parquet-file>
            
            This example demonstrates the Parquet Filter API with various WHERE clauses.
            
            Example:
                java -jar parquet-view-cli.jar /path/to/data.parquet
        """.trimIndent())
    }
    
    private fun printRows(data: ParquetReader.ParquetData) {
        if (data.rows.isEmpty()) {
            println("  (No rows matched)")
            return
        }
        
        // Print header
        println("  Columns: ${data.schema.joinToString(", ")}")
        println()
        
        // Print rows
        data.rows.take(5).forEachIndexed { index, row ->
            println("  Row ${index + 1}:")
            data.schema.forEach { column ->
                val value = row[column]
                println("    $column: $value")
            }
            println()
        }
        
        if (data.rows.size > 5) {
            println("  ... and ${data.rows.size - 5} more rows")
        }
    }
}
