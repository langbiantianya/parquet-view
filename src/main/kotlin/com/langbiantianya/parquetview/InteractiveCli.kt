package com.langbiantianya.parquetview

import com.googlecode.lanterna.TerminalSize
import com.googlecode.lanterna.TextColor
import com.googlecode.lanterna.gui2.*
import com.googlecode.lanterna.gui2.table.Table
import com.googlecode.lanterna.input.KeyStroke
import com.googlecode.lanterna.input.KeyType
import com.googlecode.lanterna.screen.TerminalScreen
import com.googlecode.lanterna.terminal.DefaultTerminalFactory
import kotlinx.coroutines.*
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

class InteractiveCli(private val filePath: String, private var pageSize: Int = 1000) {
    
    private val logger = LoggerFactory.getLogger(InteractiveCli::class.java)
    private val parquetReader = ParquetReader()
    private var currentData: ParquetReader.ParquetData? = null
    private var filteredData: ParquetReader.ParquetData? = null
    
    // Available page sizes
    private val pageSizes = listOf(100, 500, 1000, 2000, 5000, 10000)
    private var pageSizeIndex = pageSizes.indexOf(pageSize).let { if (it == -1) 2 else it } // Default to 1000
    
    // Pagination state
    private var currentPageData: ParquetReader.PagedParquetData? = null
    private var currentSkip = 0
    private var isPagingMode = false
    private var isFiltering = false // Track if we're in filtering mode
    private var allData: ParquetReader.ParquetData? = null // Store all data for filtering
    private var filterPageIndex = 0 // Current page index for filtered data
    
    // Filter result - only store indices, not actual data
    private var filterResult: ParquetReader.FilterResult? = null
    
    // Coroutine scope
    private val coroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    
    // Loading state
    private var isLoading = false
    private var loadingAnimationJob: Job? = null
    private val loadingFrames = listOf("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")
    private var loadingFrameIndex = 0
    
    private lateinit var screen: TerminalScreen
    private lateinit var gui: MultiWindowTextGUI
    private lateinit var mainWindow: BasicWindow
    private lateinit var dataTable: Table<String>
    private lateinit var filterInput: TextBox
    private lateinit var statusLabel: Label
    private lateinit var recordCountLabel: Label
    private lateinit var pageSizeLabel: Label
    private lateinit var hintLabel: Label
    private lateinit var validationLabel: Label
    
    // Buttons that need to be disabled during loading
    private lateinit var applyButton: Button
    private lateinit var resetButton: Button
    private lateinit var quitButton: Button
    private lateinit var decreasePageSizeButton: Button
    private lateinit var increasePageSizeButton: Button
    private var prevPageButton: Button? = null
    private var nextPageButton: Button? = null
    
    fun run() {
        try {
            val file = File(filePath)
            
            if (!file.exists()) {
                println("Error: File not found: $filePath")
                return
            }
            
            // Use paged reading for real-time view
            isPagingMode = true
            currentSkip = 0
            currentPageData = parquetReader.readParquetFilePaged(file.absolutePath, pageSize, currentSkip)
            
            // Also keep the data in the old format for compatibility
            currentData = ParquetReader.ParquetData(
                currentPageData!!.schema,
                currentPageData!!.rows
            )
            filteredData = currentData
            
            // Initialize terminal UI
            initializeTerminal()
            
            // Start GUI
            gui.addWindowAndWait(mainWindow)
            
        } catch (e: Exception) {
            logger.error("Error in interactive CLI", e)
            println("Error: ${e.message}")
        } finally {
            cleanup()
        }
    }
    
    private fun initializeTerminal() {
        val terminalFactory = DefaultTerminalFactory()
        val terminal = terminalFactory.createTerminal()
        screen = TerminalScreen(terminal)
        screen.startScreen()
        
        gui = MultiWindowTextGUI(screen, DefaultWindowManager(), EmptySpace(TextColor.ANSI.BLUE))
        
        mainWindow = BasicWindow("Parquet Viewer - $filePath")
        mainWindow.setHints(listOf(Window.Hint.FULL_SCREEN, Window.Hint.NO_DECORATIONS))
        
        val contentPanel = Panel()
        contentPanel.layoutManager = LinearLayout(Direction.VERTICAL)
        
        // Status bar
        val statusPanel = Panel()
        statusPanel.layoutManager = LinearLayout(Direction.HORIZONTAL)
        statusLabel = Label("Ready")
        
        // Update record count label to show pagination info
        val recordInfo = if (isPagingMode && currentPageData != null) {
            "Page: ${currentPageData!!.currentPage} | Rows Read: ${currentPageData!!.totalRowsRead}" +
            if (currentPageData!!.hasMore) " | More available" else ""
        } else {
            "Records: ${currentData?.rows?.size ?: 0}"
        }
        recordCountLabel = Label(recordInfo)
        
        // Page size label
        pageSizeLabel = Label("Page Size: $pageSize")
        
        statusPanel.addComponent(statusLabel)
        statusPanel.addComponent(Label(" | ").addTo(statusPanel))
        statusPanel.addComponent(recordCountLabel)
        statusPanel.addComponent(Label(" | ").addTo(statusPanel))
        statusPanel.addComponent(pageSizeLabel)
        contentPanel.addComponent(statusPanel.withBorder(Borders.singleLine("Status")))
        
        // Filter input
        val filterPanel = Panel()
        filterPanel.layoutManager = LinearLayout(Direction.VERTICAL)
        filterPanel.addComponent(Label("WHERE Clause (press Enter to apply, F5 to reset):"))
        
        // Add hint label for real-time feedback (declare before TextBox)
        hintLabel = Label("Examples: age > 18, name LIKE '%John%', status = 'active'")
        hintLabel.foregroundColor = TextColor.ANSI.CYAN
        
        // Add validation label (declare before TextBox)
        validationLabel = Label("")
        
        // Create custom TextBox with keystroke handling
        filterInput = object : TextBox(TerminalSize(80, 1)) {
            override fun handleKeyStroke(keyStroke: KeyStroke): Interactable.Result {
                val result = super.handleKeyStroke(keyStroke)
                // Update hint after any key stroke (except special keys)
                if (keyStroke.keyType == KeyType.Character || 
                    keyStroke.keyType == KeyType.Backspace ||
                    keyStroke.keyType == KeyType.Delete) {
                    updateFilterHint()
                }
                return result
            }
        }
        
        filterPanel.addComponent(filterInput)
        filterPanel.addComponent(hintLabel)
        filterPanel.addComponent(validationLabel)
        
        val buttonPanel = Panel()
        buttonPanel.layoutManager = LinearLayout(Direction.HORIZONTAL)

        applyButton = Button("Apply Filter") {
            if (!isLoading) applyFilter()
        }
        buttonPanel.addComponent(applyButton)

        // Pagination buttons
        if (isPagingMode) {
            prevPageButton = Button("Prev Page (F7)") {
                if (!isLoading) loadPreviousPage()
            }
            buttonPanel.addComponent(prevPageButton!!)
            
            nextPageButton = Button("Next Page (F8)") {
                if (!isLoading) loadNextPage()
            }
            buttonPanel.addComponent(nextPageButton!!)
        }
        // Page size adjustment buttons
        decreasePageSizeButton = Button("Page Size -") {
            if (!isLoading) decreasePageSize()
        }
        buttonPanel.addComponent(decreasePageSizeButton)

        increasePageSizeButton = Button("Page Size +") {
            if (!isLoading) increasePageSize()
        }
        buttonPanel.addComponent(increasePageSizeButton)
        resetButton = Button("Reset (F5)") {
            if (!isLoading) resetFilter()
        }
        buttonPanel.addComponent(resetButton)
        
        quitButton = Button("Quit (ESC)") {
            mainWindow.close()
        }
        buttonPanel.addComponent(quitButton)
        
        filterPanel.addComponent(buttonPanel)
        contentPanel.addComponent(filterPanel.withBorder(Borders.singleLine("Filter")))
        
        // Data table
        dataTable = Table<String>(*getColumnLabels())
        dataTable.tableModel.rows.clear()
        
        // Enable cell selection mode for horizontal scrolling with arrow keys
        dataTable.isCellSelection = true
        
        // Set visible columns (optional - limits how many columns shown at once)
        // If not set, all columns that fit will be shown
        // dataTable.visibleColumns = 5  // Uncomment to limit visible columns
        
        // Enable select action
        dataTable.setSelectAction {
            // Keep table focused to allow navigation
        }
        
        updateTable(filteredData!!)
        
        // Wrap table in a scrollable panel
        val tablePanel = dataTable.withBorder(Borders.singleLine("Data Table (↑↓ rows, ←→ columns)"))
        contentPanel.addComponent(tablePanel)
        
        // Help text
        val helpText = if (isPagingMode) {
            "Keys: ↑↓ Rows | ←→ Columns | Tab: Fields | Enter: Apply | F5: Reset | F7: Prev | F8: Next | ESC: Quit"
        } else {
            "Keys: ↑↓ Rows | ←→ Columns | Tab: Fields | Enter: Apply | F5: Reset | ESC: Quit"
        }
        val helpLabel = Label(helpText)
        contentPanel.addComponent(helpLabel)
        
        mainWindow.component = contentPanel
        
        // Add key listeners
        mainWindow.addWindowListener(object : WindowListenerAdapter() {
            override fun onInput(basePane: Window?, keyStroke: com.googlecode.lanterna.input.KeyStroke?, deliverEvent: AtomicBoolean?) {
                keyStroke?.let { key ->
                    when (key.keyType) {
                        com.googlecode.lanterna.input.KeyType.F5 -> {
                            resetFilter()
                            deliverEvent?.set(false)
                        }
                        com.googlecode.lanterna.input.KeyType.F7 -> {
                            if (isPagingMode) {
                                loadPreviousPage()
                                deliverEvent?.set(false)
                            }
                        }
                        com.googlecode.lanterna.input.KeyType.F8 -> {
                            if (isPagingMode) {
                                loadNextPage()
                                deliverEvent?.set(false)
                            }
                        }
                        com.googlecode.lanterna.input.KeyType.Escape -> {
                            mainWindow.close()
                            deliverEvent?.set(false)
                        }
                        else -> {}
                    }
                }
            }
        })
    }
    
    private fun updateFilterHint() {
        val text = filterInput.text.trim()
        
        if (text.isEmpty()) {
            hintLabel.text = "Examples: age > 18, name LIKE '%John%', status = 'active'"
            hintLabel.foregroundColor = TextColor.ANSI.CYAN
            validationLabel.text = ""
            return
        }
        
        // Try to parse with JSqlParser
        try {
            CCJSqlParserUtil.parseCondExpression(text)
            
            // Valid SQL - provide helpful hints
            val hint = when {
                text.matches(Regex(".*\\w+\\s+(>|<|>=|<=|=|!=)\\s*\\d+.*", RegexOption.IGNORE_CASE)) ->
                    "✓ Numeric comparison"
                text.matches(Regex(".*\\w+\\s+IS\\s+(NOT\\s+)?NULL.*", RegexOption.IGNORE_CASE)) ->
                    "✓ NULL check"
                text.matches(Regex(".*\\w+\\s+(NOT\\s+)?LIKE\\s+.*", RegexOption.IGNORE_CASE)) ->
                    "✓ LIKE pattern - % matches any characters"
                text.contains(" AND ", ignoreCase = true) ->
                    "✓ AND condition - both must be true"
                text.contains(" OR ", ignoreCase = true) ->
                    "✓ OR condition - at least one must be true"
                text.matches(Regex(".*\\w+\\s+IN\\s*\\(.*", RegexOption.IGNORE_CASE)) ->
                    "✓ IN list"
                else -> "✓ Valid SQL syntax"
            }
            
            hintLabel.text = hint
            hintLabel.foregroundColor = TextColor.ANSI.GREEN
            
            validationLabel.text = "✓ Valid WHERE clause - press Enter to apply"
            validationLabel.foregroundColor = TextColor.ANSI.GREEN
            
        } catch (e: Exception) {
            // Invalid SQL
            hintLabel.text = "Available columns: ${currentData?.schema?.joinToString(", ") ?: ""}"
            hintLabel.foregroundColor = TextColor.ANSI.CYAN
            
            val errorMsg = when {
                text.contains("=") && !text.contains("'") && !text.contains("\"") && !text.matches(Regex(".*=\\s*\\d+.*")) ->
                    "⚠ String values need quotes: column = 'value'"
                text.contains("LIKE", ignoreCase = true) && !text.contains("'") && !text.contains("\"") ->
                    "⚠ LIKE pattern needs quotes: column LIKE '%pattern%'"
                else -> "⚠ Invalid SQL: ${e.message?.take(50) ?: "Check syntax"}"
            }
            
            validationLabel.text = errorMsg
            validationLabel.foregroundColor = TextColor.ANSI.YELLOW
        }
    }
    
    private fun setLoadingState(loading: Boolean) {
        isLoading = loading
        
        // Disable/enable all interactive controls
        filterInput.isEnabled = !loading
        applyButton.isEnabled = !loading
        resetButton.isEnabled = !loading
        decreasePageSizeButton.isEnabled = !loading
        increasePageSizeButton.isEnabled = !loading
        prevPageButton?.isEnabled = !loading
        nextPageButton?.isEnabled = !loading
        
        // Show loading animation
        if (loading) {
            startLoadingAnimation()
        } else {
            stopLoadingAnimation()
        }
    }
    
    private fun startLoadingAnimation() {
        loadingAnimationJob?.cancel()
        loadingFrameIndex = 0
        
        loadingAnimationJob = coroutineScope.launch {
            while (isActive) {
                val currentText = statusLabel.text
                val baseText = currentText.replace(Regex("[⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏]"), "").trim()
                statusLabel.text = "${loadingFrames[loadingFrameIndex]} $baseText"
                loadingFrameIndex = (loadingFrameIndex + 1) % loadingFrames.size
                
                try {
                    gui.updateScreen()
                } catch (e: Exception) {
                    // Ignore update errors
                }
                
                delay(100)
            }
        }
    }
    
    private fun stopLoadingAnimation() {
        loadingAnimationJob?.cancel()
        loadingAnimationJob = null
    }
    
    private fun getColumnLabels(): Array<String> {
        return currentData?.schema?.toTypedArray() ?: arrayOf()
    }
    
    private fun recreateTableWithSchema(schema: List<String>) {
        // Get the parent panel
        val contentPanel = mainWindow.component as Panel
        
        // Find and remove old table panel
        val components = contentPanel.children.toList()
        val oldTablePanel = components.find { component ->
            // Find the panel that contains the table (has a border with "Data" in title)
            if (component is Panel) {
                try {
                    // Check if this is the bordered panel containing the table
                    component.children.any { it is Table<*> }
                } catch (e: Exception) {
                    false
                }
            } else {
                false
            }
        }
        
        if (oldTablePanel != null) {
            contentPanel.removeComponent(oldTablePanel)
        }
        
        // Create new table with proper schema
        dataTable = Table<String>(*schema.toTypedArray())
        
        // Enable cell selection mode for horizontal scrolling with arrow keys
        dataTable.isCellSelection = true
        
        // Set visible columns (optional)
        // dataTable.visibleColumns = 5
        
        // Enable cell selection for navigation
        dataTable.setSelectAction {
            // Keep table focused to allow navigation
        }
        
        // Wrap table in a scrollable panel
        val tablePanel = dataTable.withBorder(Borders.singleLine("Data Table (↑↓ rows, ←→ columns)"))
        
        // Insert table panel before help text
        val helpTextIndex = components.indexOfFirst { 
            it is Label && it.text.contains("Keys:")
        }
        
        if (helpTextIndex > 0) {
            contentPanel.addComponent(helpTextIndex, tablePanel)
        } else {
            contentPanel.addComponent(tablePanel)
        }
    }
    
    private fun updateTable(data: ParquetReader.ParquetData) {
        // Clear existing rows
        dataTable.tableModel.clear()
        
        // Add new rows
        data.rows.forEach { row ->
            val rowData = data.schema.map { columnName ->
                formatValue(row[columnName])
            }.toTypedArray()
            dataTable.tableModel.addRow(*rowData)
        }
        
        // Update record count
        updateRecordCountLabel()
        
        // Force GUI refresh
        try {
//            dataTable.invalidate()
            gui.updateScreen()
        } catch (e: Exception) {
            logger.warn("Error refreshing GUI: ${e.message}")
        }
    }
    
    private fun updateRecordCountLabel() {
        recordCountLabel.text = if (isFiltering && filterResult != null) {
            // In filtering mode, show filtered pagination info
            val totalFiltered = filterResult!!.matchedRowIndices.size
            val totalPages = (totalFiltered + pageSize - 1) / pageSize
            val currentPage = filterPageIndex + 1
            "Filtered: Page $currentPage/$totalPages | $totalFiltered rows total"
        } else if (isPagingMode && currentPageData != null) {
            // In normal paging mode
            "Page: ${currentPageData!!.currentPage} | Rows Read: ${currentPageData!!.totalRowsRead}" +
            if (currentPageData!!.hasMore) " | More available" else ""
        } else {
            "Records: ${filteredData?.rows?.size ?: 0} / ${currentData?.rows?.size ?: 0}"
        }
    }
    
    private fun loadNextPage() {
        if (isLoading) return
        
        if (isFiltering) {
            // In filtering mode, navigate through filtered results
            if (filterResult == null) return
            
            val totalPages = (filterResult!!.matchedRowIndices.size + pageSize - 1) / pageSize
            if (filterPageIndex >= totalPages - 1) {
                statusLabel.text = "Already at last page of filtered results"
                statusLabel.foregroundColor = TextColor.ANSI.YELLOW
                return
            }
            
            filterPageIndex++
            loadFilteredPage(filterPageIndex)
            return
        }
        
        // Normal paging mode
        if (!isPagingMode || currentPageData?.hasMore != true) {
            statusLabel.text = "No more pages available"
            statusLabel.foregroundColor = TextColor.ANSI.YELLOW
            return
        }
        
        setLoadingState(true)
        statusLabel.text = "Loading next page..."
        
        coroutineScope.launch {
            try {
                currentSkip += pageSize
                
                val file = File(filePath)
                currentPageData = parquetReader.readParquetFilePaged(file.absolutePath, pageSize, currentSkip)
                
                currentData = ParquetReader.ParquetData(
                    currentPageData!!.schema,
                    currentPageData!!.rows
                )
                filteredData = currentData
                
                updateTable(filteredData!!)
                statusLabel.text = "Page loaded"
                statusLabel.foregroundColor = TextColor.ANSI.GREEN
                setLoadingState(false)
                
            } catch (e: Exception) {
                statusLabel.text = "Error loading page: ${e.message}"
                statusLabel.foregroundColor = TextColor.ANSI.RED
                logger.error("Error loading next page", e)
                setLoadingState(false)
            }
        }
    }
    
    private fun loadPreviousPage() {
        if (isLoading) return
        
        if (isFiltering) {
            // In filtering mode, navigate through filtered results
            if (filterResult == null || filterPageIndex == 0) {
                statusLabel.text = "Already at first page of filtered results"
                statusLabel.foregroundColor = TextColor.ANSI.YELLOW
                return
            }
            
            filterPageIndex--
            loadFilteredPage(filterPageIndex)
            return
        }
        
        // Normal paging mode
        if (!isPagingMode || currentSkip == 0) {
            statusLabel.text = "Already at first page"
            statusLabel.foregroundColor = TextColor.ANSI.YELLOW
            return
        }
        
        setLoadingState(true)
        statusLabel.text = "Loading previous page..."
        
        coroutineScope.launch {
            try {
                currentSkip = maxOf(0, currentSkip - pageSize)
                
                val file = File(filePath)
                currentPageData = parquetReader.readParquetFilePaged(file.absolutePath, pageSize, currentSkip)
                
                currentData = ParquetReader.ParquetData(
                    currentPageData!!.schema,
                    currentPageData!!.rows
                )
                filteredData = currentData
                
                updateTable(filteredData!!)
                statusLabel.text = "Page loaded"
                statusLabel.foregroundColor = TextColor.ANSI.GREEN
                setLoadingState(false)
                
            } catch (e: Exception) {
                statusLabel.text = "Error loading page: ${e.message}"
                statusLabel.foregroundColor = TextColor.ANSI.RED
                logger.error("Error loading previous page", e)
                setLoadingState(false)
            }
        }
    }
    
    private fun formatValue(value: Any?): String {
        return when (value) {
            null -> "NULL"
            is Double -> String.format("%.2f", value)
            is Float -> String.format("%.2f", value)
            else -> value.toString()
        }
    }
    
    private fun applyFilter() {
        val whereClause = filterInput.text.trim()
        
        if (whereClause.isEmpty()) {
            resetFilter()
            return
        }
        
        if (isLoading) return
        
        setLoadingState(true)
        statusLabel.text = "Starting streaming filter..."
        statusLabel.foregroundColor = TextColor.ANSI.CYAN
        
        try {
            gui.updateScreen()
        } catch (e: Exception) {
            // Ignore
        }
        
        // Run filter in coroutine - only store indices
        coroutineScope.launch {
            try {
                val file = File(filePath)
                
                // Use index-only filtering - minimal memory usage
                filterResult = parquetReader.filterParquetFileIndices(
                    filePath = file.absolutePath,
                    whereClause = whereClause,
                    progressCallback = { totalRead, matchedCount ->
                        // Update progress
                        statusLabel.text = "Filtering: Scanned $totalRead rows, matched $matchedCount..."
                        try {
                            gui.updateScreen()
                        } catch (e: Exception) {
                            // Ignore
                        }
                    }
                )
                
                // Enter filtering mode and reset to first page
                isFiltering = true
                filterPageIndex = 0
                allData = null
                
                // Load first page of filtered results
                loadFilteredPage(0)
                
                statusLabel.text = "Filter applied: ${filterResult!!.matchedRowIndices.size} rows matched"
                statusLabel.foregroundColor = TextColor.ANSI.GREEN
                setLoadingState(false)
                
                try {
                    gui.updateScreen()
                } catch (e: Exception) {
                    // Ignore
                }
                
            } catch (e: Exception) {
                val errorMsg = "Filter error: ${e.message}"
                statusLabel.text = errorMsg
                statusLabel.foregroundColor = TextColor.ANSI.RED
                setLoadingState(false)
                logger.error("Filter error for clause '$whereClause'", e)
                e.printStackTrace()
                
                try {
                    gui.updateScreen()
                } catch (ex: Exception) {
                    // Ignore
                }
            }
        }
    }
    
    private fun loadFilteredPage(pageIndex: Int) {
        if (filterResult == null) return
        
        setLoadingState(true)
        statusLabel.text = "Loading filtered page ${pageIndex + 1}..."
        
        coroutineScope.launch {
            try {
                val startIndex = pageIndex * pageSize
                val endIndex = minOf(startIndex + pageSize, filterResult!!.matchedRowIndices.size)
                
                if (startIndex >= filterResult!!.matchedRowIndices.size) {
                    filteredData = ParquetReader.ParquetData(filterResult!!.schema, emptyList())
                    updateTable(filteredData!!)
                    setLoadingState(false)
                    return@launch
                }
                
                // Get indices for this page
                val pageIndices = filterResult!!.matchedRowIndices.subList(startIndex, endIndex)
                
                // Read only the rows for this page
                val file = File(filePath)
                filteredData = parquetReader.readRowsByIndices(
                    filePath = file.absolutePath,
                    rowIndices = pageIndices,
                    schema = filterResult!!.schema
                )
                
                updateTable(filteredData!!)
                statusLabel.text = "Showing filtered page ${pageIndex + 1}"
                statusLabel.foregroundColor = TextColor.ANSI.GREEN
                setLoadingState(false)
                
            } catch (e: Exception) {
                statusLabel.text = "Error loading page: ${e.message}"
                statusLabel.foregroundColor = TextColor.ANSI.RED
                logger.error("Error loading filtered page", e)
                setLoadingState(false)
            }
        }
    }
    
    private fun paginateFilteredData(data: ParquetReader.ParquetData, pageIndex: Int): ParquetReader.ParquetData {
        val startIndex = pageIndex * pageSize
        val endIndex = minOf(startIndex + pageSize, data.rows.size)
        
        if (startIndex >= data.rows.size) {
            return ParquetReader.ParquetData(data.schema, emptyList())
        }
        
        val pagedRows = data.rows.subList(startIndex, endIndex)
        return ParquetReader.ParquetData(data.schema, pagedRows)
    }
    
    private fun resetFilter() {
        filterInput.text = ""
        isFiltering = false
        filterPageIndex = 0
        filterResult = null
        filteredData = currentData
        updateTable(filteredData!!)
        statusLabel.text = "Filter reset - showing all data"
        statusLabel.foregroundColor = TextColor.ANSI.CYAN
    }
    
    private fun increasePageSize() {
        if (pageSizeIndex < pageSizes.size - 1) {
            pageSizeIndex++
            pageSize = pageSizes[pageSizeIndex]
            pageSizeLabel.text = "Page Size: $pageSize"
            onPageSizeChanged()
        }
    }
    
    private fun decreasePageSize() {
        if (pageSizeIndex > 0) {
            pageSizeIndex--
            pageSize = pageSizes[pageSizeIndex]
            pageSizeLabel.text = "Page Size: $pageSize"
            onPageSizeChanged()
        }
    }
    
    private fun onPageSizeChanged() {
        if (isLoading) return
        
        setLoadingState(true)
        statusLabel.text = "Changing page size..."
        
        coroutineScope.launch {
            try {
                // Reset to first page when page size changes
                if (isFiltering && filterResult != null) {
                    filterPageIndex = 0
                    loadFilteredPage(0)
                } else if (isPagingMode) {
                    currentSkip = 0
                    val file = File(filePath)
                    currentPageData = parquetReader.readParquetFilePaged(file.absolutePath, pageSize, currentSkip)
                    currentData = ParquetReader.ParquetData(
                        currentPageData!!.schema,
                        currentPageData!!.rows
                    )
                    filteredData = currentData
                    updateTable(filteredData!!)
                    statusLabel.text = "Page size changed to $pageSize"
                    statusLabel.foregroundColor = TextColor.ANSI.CYAN
                    setLoadingState(false)
                }
            } catch (e: Exception) {
                statusLabel.text = "Error reloading: ${e.message}"
                statusLabel.foregroundColor = TextColor.ANSI.RED
                setLoadingState(false)
            }
        }
    }
    
    private fun cleanup() {
        try {
            // Cancel all coroutines
            coroutineScope.cancel()
            screen.stopScreen()
        } catch (e: Exception) {
            logger.error("Error cleaning up terminal", e)
        }
    }
}
