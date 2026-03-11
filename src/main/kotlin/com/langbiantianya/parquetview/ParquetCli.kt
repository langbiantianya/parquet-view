package com.langbiantianya.parquetview

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.jakewharton.picnic.table
import java.io.File

class ParquetCli : CliktCommand(
    name = "parquet-view",
) {
    private val filePath by argument(
        name = "FILE",
        help = "Path to the Parquet file"
    )

    private val limit by option(
        "-l", "--limit",
        help = "Maximum number of rows to display"
    ).int().default(1000)

    private val where by option(
        "-w", "--where",
        help = "WHERE clause for filtering (e.g., 'age > 18 AND status = \"active\"')"
    )

    private val interactive by option(
        "-i", "--interactive",
        help = "Enable interactive TUI mode with real-time filtering and scrolling"
    ).flag(default = true)

    private val simple by option(
        "-s", "--simple",
        help = "Use simple table output mode instead of interactive TUI"
    ).flag(default = false)

    private val parquetReader = ParquetReader()

    override fun run() {
        val file = File(filePath)

        if (!file.exists()) {
            echo("Error: File not found: $filePath", err = true)
            return
        }

        if (!file.canRead()) {
            echo("Error: Cannot read file: $filePath", err = true)
            return
        }

        echo("Loading Parquet file: ${file.absolutePath}")
        echo("Please wait...\n")

        try {
            // Check mode: interactive by default, unless --simple or --where is specified
            if (!simple && where == null) {
                // Launch interactive TUI (default mode)
                val interactiveCli = InteractiveCli(file.absolutePath, limit)
                interactiveCli.run()
                return
            }

            // Otherwise use simple CLI mode
            val data = parquetReader.readParquetFile(file.absolutePath, limit)

            echo("Schema: ${data.schema.joinToString(", ")}")
            echo("Total rows loaded: ${data.rows.size}\n")

            if (where != null) {
                // Apply filter from command line
                displayFilteredData(data, where!!)
            } else {
                // Display all data
                displayData(data)
            }

        } catch (e: Exception) {
            echo("Error reading Parquet file: ${e.message}", err = true)
            e.printStackTrace()
        }
    }

    private fun displayFilteredData(data: ParquetReader.ParquetData, whereClause: String) {
        try {
            echo("Applying filter: $whereClause")
            val filteredData = parquetReader.filterData(data, whereClause)
            echo("Filtered rows: ${filteredData.rows.size}\n")
            displayData(filteredData)
        } catch (e: Exception) {
            echo("Error applying filter: ${e.message}", err = true)
        }
    }

    private fun displayData(data: ParquetReader.ParquetData) {
        if (data.rows.isEmpty()) {
            echo("No data to display")
            return
        }

        val table = table {
            cellStyle {
                border = true
                paddingLeft = 1
                paddingRight = 1
            }

            header {
                row {
                    data.schema.forEach { columnName ->
                        cell(columnName)
                    }
                }
            }

            body {
                data.rows.forEach { rowData ->
                    row {
                        data.schema.forEach { columnName ->
                            val value = rowData[columnName]
                            cell(formatValue(value))
                        }
                    }
                }
            }

            footer {
                row {
                    cell("Total: ${data.rows.size} rows") {
                        columnSpan = data.schema.size
                    }
                }
            }
        }

        echo(table.toString())
    }

    private fun formatValue(value: Any?): String {
        return when (value) {
            null -> "NULL"
            is Double -> String.format("%.2f", value)
            is Float -> String.format("%.2f", value)
            else -> {
                val str = value.toString()
                // Truncate long strings
                if (str.length > 50) {
                    str.take(47) + "..."
                } else {
                    str
                }
            }
        }
    }
}

fun main(args: Array<String>) = ParquetCli().main(args)
