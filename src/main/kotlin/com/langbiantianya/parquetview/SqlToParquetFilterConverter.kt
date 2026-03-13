package com.langbiantianya.parquetview

import net.sf.jsqlparser.expression.*
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.conditional.OrExpression
import net.sf.jsqlparser.expression.operators.relational.*
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Column
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.io.api.Binary

/**
 * 将SQL WHERE子句转换为Parquet FilterPredicate
 * 使用Parquet Filter API实现谓词下推，提升过滤性能
 */
class SqlToParquetFilterConverter {
    
    /**
     * 将SQL WHERE子句转换为Parquet FilterPredicate
     * @param whereClause SQL WHERE子句（不含WHERE关键字）
     * @return Parquet FilterPredicate，如果无法转换则返回null
     */
    fun convertToFilter(whereClause: String): FilterPredicate? {
        if (whereClause.isBlank()) {
            return null
        }
        
        return try {
            val expression = CCJSqlParserUtil.parseCondExpression(whereClause)
            convertExpression(expression)
        } catch (e: Exception) {
            println("Error converting WHERE clause to Parquet filter: ${e.message}")
            e.printStackTrace()
            null
        }
    }
    
    private fun convertExpression(expression: Expression): FilterPredicate? {
        return when (expression) {
            is AndExpression -> {
                val left = convertExpression(expression.leftExpression)
                val right = convertExpression(expression.rightExpression)
                
                if (left != null && right != null) {
                    FilterApi.and(left, right)
                } else {
                    left ?: right
                }
            }
            
            is OrExpression -> {
                val left = convertExpression(expression.leftExpression)
                val right = convertExpression(expression.rightExpression)
                
                if (left != null && right != null) {
                    FilterApi.or(left, right)
                } else {
                    null // OR需要两个操作数都存在
                }
            }
            
            is NotExpression -> {
                val inner = convertExpression(expression.expression)
                if (inner != null) {
                    FilterApi.not(inner)
                } else {
                    null
                }
            }
            
            is Parenthesis -> {
                convertExpression(expression.expression)
            }
            
            is EqualsTo -> convertComparison(expression, ComparisonType.EQ)
            is NotEqualsTo -> convertComparison(expression, ComparisonType.NOT_EQ)
            is GreaterThan -> convertComparison(expression, ComparisonType.GT)
            is GreaterThanEquals -> convertComparison(expression, ComparisonType.GT_EQ)
            is MinorThan -> convertComparison(expression, ComparisonType.LT)
            is MinorThanEquals -> convertComparison(expression, ComparisonType.LT_EQ)
            
            is IsNullExpression -> {
                val column = expression.leftExpression as? Column ?: return null
                val columnPath = FilterApi.binaryColumn(column.columnName)
                
                if (expression.isNot) {
                    FilterApi.notEq(columnPath, null as Binary?)
                } else {
                    FilterApi.eq(columnPath, null as Binary?)
                }
            }
            
            is InExpression -> convertInExpression(expression)
            
            else -> {
                println("Unsupported expression type for Parquet filter: ${expression.javaClass.simpleName}")
                null
            }
        }
    }
    
    private enum class ComparisonType {
        EQ, NOT_EQ, GT, GT_EQ, LT, LT_EQ
    }
    
    private fun convertComparison(comparison: ComparisonOperator, type: ComparisonType): FilterPredicate? {
        val column = comparison.leftExpression as? Column ?: return null
        val columnName = column.columnName
        
        val rightValue = comparison.rightExpression
        
        return when (rightValue) {
            is LongValue -> {
                val intColumn = FilterApi.intColumn(columnName)
                val longColumn = FilterApi.longColumn(columnName)
                val value = rightValue.value
                
                // 尝试使用Int或Long列
                try {
                    when (type) {
                        ComparisonType.EQ -> if (value in Int.MIN_VALUE..Int.MAX_VALUE) {
                            FilterApi.eq(intColumn, value.toInt())
                        } else {
                            FilterApi.eq(longColumn, value)
                        }
                        ComparisonType.NOT_EQ -> if (value in Int.MIN_VALUE..Int.MAX_VALUE) {
                            FilterApi.notEq(intColumn, value.toInt())
                        } else {
                            FilterApi.notEq(longColumn, value)
                        }
                        ComparisonType.GT -> if (value in Int.MIN_VALUE..Int.MAX_VALUE) {
                            FilterApi.gt(intColumn, value.toInt())
                        } else {
                            FilterApi.gt(longColumn, value)
                        }
                        ComparisonType.GT_EQ -> if (value in Int.MIN_VALUE..Int.MAX_VALUE) {
                            FilterApi.gtEq(intColumn, value.toInt())
                        } else {
                            FilterApi.gtEq(longColumn, value)
                        }
                        ComparisonType.LT -> if (value in Int.MIN_VALUE..Int.MAX_VALUE) {
                            FilterApi.lt(intColumn, value.toInt())
                        } else {
                            FilterApi.lt(longColumn, value)
                        }
                        ComparisonType.LT_EQ -> if (value in Int.MIN_VALUE..Int.MAX_VALUE) {
                            FilterApi.ltEq(intColumn, value.toInt())
                        } else {
                            FilterApi.ltEq(longColumn, value)
                        }
                    }
                } catch (e: Exception) {
                    null
                }
            }
            
            is DoubleValue -> {
                val doubleColumn = FilterApi.doubleColumn(columnName)
                val floatColumn = FilterApi.floatColumn(columnName)
                val value = rightValue.value
                
                try {
                    when (type) {
                        ComparisonType.EQ -> FilterApi.eq(doubleColumn, value)
                        ComparisonType.NOT_EQ -> FilterApi.notEq(doubleColumn, value)
                        ComparisonType.GT -> FilterApi.gt(doubleColumn, value)
                        ComparisonType.GT_EQ -> FilterApi.gtEq(doubleColumn, value)
                        ComparisonType.LT -> FilterApi.lt(doubleColumn, value)
                        ComparisonType.LT_EQ -> FilterApi.ltEq(doubleColumn, value)
                    }
                } catch (e: Exception) {
                    null
                }
            }
            
            is StringValue -> {
                val binaryColumn = FilterApi.binaryColumn(columnName)
                val value = Binary.fromString(rightValue.value)
                
                try {
                    when (type) {
                        ComparisonType.EQ -> FilterApi.eq(binaryColumn, value)
                        ComparisonType.NOT_EQ -> FilterApi.notEq(binaryColumn, value)
                        ComparisonType.GT -> FilterApi.gt(binaryColumn, value)
                        ComparisonType.GT_EQ -> FilterApi.gtEq(binaryColumn, value)
                        ComparisonType.LT -> FilterApi.lt(binaryColumn, value)
                        ComparisonType.LT_EQ -> FilterApi.ltEq(binaryColumn, value)
                    }
                } catch (e: Exception) {
                    null
                }
            }
            
            else -> null
        }
    }
    
    private fun convertInExpression(inExpression: InExpression): FilterPredicate? {
        val column = inExpression.leftExpression as? Column ?: return null
        val columnName = column.columnName
        
        val rightItemsList = inExpression.rightExpression as? ExpressionList<*> ?: return null
        
        if (rightItemsList.isEmpty()) {
            return null
        }
        
        // 构建OR链
        var result: FilterPredicate? = null
        
        for (item in rightItemsList) {
            val itemPredicate = when (item) {
                is LongValue -> {
                    val value = item.value
                    if (value in Int.MIN_VALUE..Int.MAX_VALUE) {
                        FilterApi.eq(FilterApi.intColumn(columnName), value.toInt())
                    } else {
                        FilterApi.eq(FilterApi.longColumn(columnName), value)
                    }
                }
                
                is DoubleValue -> {
                    FilterApi.eq(FilterApi.doubleColumn(columnName), item.value)
                }
                
                is StringValue -> {
                    FilterApi.eq(FilterApi.binaryColumn(columnName), Binary.fromString(item.value))
                }
                
                else -> null
            }
            
            if (itemPredicate != null) {
                result = if (result == null) {
                    itemPredicate
                } else {
                    FilterApi.or(result, itemPredicate)
                }
            }
        }
        
        return if (inExpression.isNot && result != null) {
            FilterApi.not(result)
        } else {
            result
        }
    }
}
