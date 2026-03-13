package com.langbiantianya.parquetview

import javafx.application.Platform
import javafx.beans.property.SimpleStringProperty
import javafx.collections.FXCollections
import javafx.fxml.FXML
import javafx.scene.control.*
import javafx.scene.input.*
import javafx.stage.FileChooser
import kotlinx.coroutines.*

class ParquetViewController {
    
    @FXML
    private lateinit var filePathLabel: Label
    
    @FXML
    private lateinit var whereTextField: TextField
    
    @FXML
    private lateinit var dataTableView: TableView<Map<String, Any?>>
    
    @FXML
    private lateinit var statusLabel: Label
    
    @FXML
    private lateinit var recordCountLabel: Label
    
    @FXML
    private lateinit var prevPageButton: Button
    
    @FXML
    private lateinit var nextPageButton: Button
    
    @FXML
    private lateinit var pageInfoLabel: Label
    
    @FXML
    private lateinit var pageSizeComboBox: ComboBox<Int>
    
    private val parquetReader = ParquetReader()
    private var currentData: ParquetReader.ParquetData? = null
    private var currentFilePath: String? = null
    
    // Loading state
    private var isLoading = false
    
    // Pagination state
    private var currentPageData: ParquetReader.PagedParquetData? = null
    private var currentSkip = 0
    private var pageSize = 1000  // Default, will be updated from ComboBox
    private var isPagingMode = false
    private var isFiltering = false // Track if we're in filtering mode
    private var allData: ParquetReader.ParquetData? = null // Store all data for filtering
    private var filteredData: ParquetReader.ParquetData? = null // Store filtered results
    private var filterPageIndex = 0 // Current page index for filtered data
    
    // Filter result - only store indices, not actual data
    private var filterResult: ParquetReader.FilterResult? = null
    
    // Coroutine scope tied to JavaFX lifecycle
    private val coroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    
    @FXML
    private fun initialize() {
        statusLabel.text = "就绪"
        recordCountLabel.text = "记录数: 0"
        
        // Initialize page size combo box
        pageSizeComboBox.items.addAll(100, 500, 1000, 2000, 5000, 10000)
        pageSizeComboBox.value = 1000
        pageSize = 1000
        
        // Listen for page size changes
        pageSizeComboBox.valueProperty().addListener { _, _, newValue ->
            if (newValue != null && newValue != pageSize) {
                pageSize = newValue
                onPageSizeChanged()
            }
        }
        
        // Setup copy functionality for table
        setupTableCopyFunctionality()
    }
    
    private fun onPageSizeChanged() {
        // Reset to first page when page size changes
        if (isFiltering && filterResult != null) {
            filterPageIndex = 0
            loadFilteredPage(0)
            updatePaginationControls()
            statusLabel.text = "每页行数已更改为 $pageSize"
        } else if (isPagingMode) {
            currentSkip = 0
            currentFilePath?.let { loadParquetFile(it) }
        }
    }
    
    private fun setLoadingState(loading: Boolean) {
        isLoading = loading
        
        // Disable/enable all interactive controls
        Platform.runLater {
            whereTextField.isDisable = loading
            prevPageButton.isDisable = loading || (isFiltering && filterPageIndex == 0) || (!isFiltering && currentSkip == 0)
            nextPageButton.isDisable = loading || (isFiltering && filterResult != null && filterPageIndex >= (filterResult!!.matchedRowIndices.size + pageSize - 1) / pageSize - 1) || (!isFiltering && currentPageData?.hasMore != true)
            pageSizeComboBox.isDisable = loading
            
            // Show loading animation in status
            if (loading) {
                startLoadingAnimation()
            } else {
                stopLoadingAnimation()
            }
        }
    }
    
    private var loadingAnimationJob: kotlinx.coroutines.Job? = null
    private val loadingFrames = listOf("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")
    private var loadingFrameIndex = 0
    
    private fun startLoadingAnimation() {
        loadingAnimationJob?.cancel()
        loadingFrameIndex = 0
        
        loadingAnimationJob = coroutineScope.launch {
            while (isActive) {
                Platform.runLater {
                    val currentText = statusLabel.text
                    if (currentText.contains("...") || currentText.contains("⠋") || currentText.contains("加载") || currentText.contains("过滤")) {
                        val baseText = currentText.replace(Regex("[⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏]"), "").trim()
                        statusLabel.text = "${loadingFrames[loadingFrameIndex]} $baseText"
                        loadingFrameIndex = (loadingFrameIndex + 1) % loadingFrames.size
                    }
                }
                delay(100)
            }
        }
    }
    
    private fun stopLoadingAnimation() {
        loadingAnimationJob?.cancel()
        loadingAnimationJob = null
    }
    
    @FXML
    private fun onSelectFile() {
        val fileChooser = FileChooser()
        fileChooser.title = "选择 Parquet 文件"
        fileChooser.extensionFilters.add(
            FileChooser.ExtensionFilter("Parquet Files", "*.parquet", "*.pq")
        )
        
        val selectedFile = fileChooser.showOpenDialog(filePathLabel.scene.window)
        
        if (selectedFile != null) {
            currentFilePath = selectedFile.absolutePath
            filePathLabel.text = selectedFile.name
            loadParquetFile(selectedFile.absolutePath)
        }
    }
    
    @FXML
    private fun onApplyFilter() {
        val whereClause = whereTextField.text
        if (whereClause.isBlank()) {
            showInfo("提示", "请输入 WHERE 条件")
            return
        }
        
        if (isLoading) return
        
        currentFilePath?.let { path ->
            setLoadingState(true)
            statusLabel.text = "正在启动流式过滤..."
            coroutineScope.launch {
                try {
                    println("Applying filter: $whereClause")
                    
                    // Use index-only filtering - minimal memory usage
                    filterResult = parquetReader.filterParquetFileIndices(
                        filePath = path,
                        whereClause = whereClause,
                        progressCallback = { totalRead, matchedCount ->
                            // Update progress on UI thread
                            Platform.runLater {
                                statusLabel.text = "过滤中: 已扫描 $totalRead 行, 匹配 $matchedCount 行..."
                            }
                        }
                    )
                    
                    println("Total rows matched: ${filterResult!!.matchedRowIndices.size}")
                    
                    Platform.runLater {
                        // Enter filtering mode and reset to first page
                        isFiltering = true
                        filterPageIndex = 0
                        allData = null
                        
                        // Load first page of filtered results
                        loadFilteredPage(0)
                        
                        updatePaginationControls()
                        statusLabel.text = "过滤完成: 匹配到 ${filterResult!!.matchedRowIndices.size} 行"
                        setLoadingState(false)
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                    Platform.runLater {
                        statusLabel.text = "过滤失败: ${e.message}"
                        showError("过滤失败", e.message ?: "未知错误")
                        setLoadingState(false)
                    }
                }
            }
        } ?: run {
            showInfo("提示", "请先选择并加载一个 Parquet 文件")
        }
    }
    
    private fun loadFilteredPage(pageIndex: Int) {
        if (filterResult == null || currentFilePath == null) return
        
        setLoadingState(true)
        statusLabel.text = "正在加载第 ${pageIndex + 1} 页..."
        
        coroutineScope.launch {
            try {
                val startIndex = pageIndex * pageSize
                val endIndex = minOf(startIndex + pageSize, filterResult!!.matchedRowIndices.size)
                
                if (startIndex >= filterResult!!.matchedRowIndices.size) {
                    Platform.runLater {
                        filteredData = ParquetReader.ParquetData(filterResult!!.schema, emptyList())
                        displayData(filteredData!!)
                        setLoadingState(false)
                    }
                    return@launch
                }
                
                // Get indices for this page
                val pageIndices = filterResult!!.matchedRowIndices.subList(startIndex, endIndex)
                
                // Read only the rows for this page
                filteredData = parquetReader.readRowsByIndices(
                    filePath = currentFilePath!!,
                    rowIndices = pageIndices,
                    schema = filterResult!!.schema
                )
                
                Platform.runLater {
                    displayData(filteredData!!)
                    updatePaginationControls()
                    statusLabel.text = "加载完成"
                    setLoadingState(false)
                }
            } catch (e: Exception) {
                Platform.runLater {
                    statusLabel.text = "加载失败: ${e.message}"
                    showError("加载失败", e.message ?: "未知错误")
                    setLoadingState(false)
                }
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
    
    @FXML
    private fun onReload() {
        currentFilePath?.let { path ->
            whereTextField.clear()
            isFiltering = false
            filterPageIndex = 0
            allData = null
            filteredData = null
            filterResult = null
            loadParquetFile(path)
        }
    }
    
    private fun loadParquetFile(filePath: String) {
        setLoadingState(true)
        statusLabel.text = "正在加载..."
        dataTableView.columns.clear()
        dataTableView.items.clear()
        
        // Enable paging mode
        isPagingMode = true
        currentSkip = 0
        
        coroutineScope.launch {
            try {
                currentPageData = parquetReader.readParquetFilePaged(filePath, pageSize, currentSkip)
                
                // Keep data in old format for compatibility
                currentData = ParquetReader.ParquetData(
                    currentPageData!!.schema,
                    currentPageData!!.rows
                )
                
                Platform.runLater {
                    displayData(currentData!!)
                    updatePaginationControls()
                    statusLabel.text = "加载完成"
                    setLoadingState(false)
                }
            } catch (e: Exception) {
                Platform.runLater {
                    statusLabel.text = "加载失败: ${e.message}"
                    showError("加载失败", e.message ?: "未知错误")
                    setLoadingState(false)
                }
            }
        }
    }
    
    private fun displayData(data: ParquetReader.ParquetData) {
        dataTableView.columns.clear()
        
        // Create columns dynamically based on schema
        data.schema.forEach { columnName ->
            val column = TableColumn<Map<String, Any?>, String>(columnName)
            column.setCellValueFactory { cellData ->
                val value = cellData.value[columnName]
                SimpleStringProperty(value?.toString() ?: "")
            }
            column.prefWidth = 150.0
            dataTableView.columns.add(column)
        }
        
        // Add data to table
        val items = FXCollections.observableArrayList(data.rows)
        dataTableView.items = items
        
        // Update record count
        updateRecordCountLabel()
    }
    
    private fun updateRecordCountLabel() {
        recordCountLabel.text = if (isFiltering && filterResult != null) {
            "本页记录数: ${filteredData?.rows?.size ?: 0} (共 ${filterResult!!.matchedRowIndices.size} 条过滤结果)"
        } else if (isPagingMode && currentPageData != null) {
            "本页记录数: ${currentPageData!!.rows.size}"
        } else {
            "记录数: ${currentData?.rows?.size ?: 0}"
        }
    }
    
    private fun updatePaginationControls() {
        if (isFiltering && filterResult != null) {
            // In filtering mode
            val totalFiltered = filterResult!!.matchedRowIndices.size
            val totalPages = (totalFiltered + pageSize - 1) / pageSize
            val currentPage = filterPageIndex + 1
            
            prevPageButton.isDisable = filterPageIndex == 0
            nextPageButton.isDisable = filterPageIndex >= totalPages - 1
            
            pageInfoLabel.text = "页码: $currentPage/$totalPages | 共 $totalFiltered 条过滤结果"
        } else if (isPagingMode && currentPageData != null) {
            // In normal paging mode
            prevPageButton.isDisable = currentSkip == 0
            nextPageButton.isDisable = !currentPageData!!.hasMore
            
            val pageInfo = "页码: ${currentPageData!!.currentPage} | 已读取: ${currentPageData!!.totalRowsRead}" +
                if (currentPageData!!.hasMore) " | 还有更多数据" else ""
            pageInfoLabel.text = pageInfo
        } else {
            prevPageButton.isDisable = true
            nextPageButton.isDisable = true
            pageInfoLabel.text = "页码: - | 已读取: -"
        }
    }
    
    @FXML
    private fun onNextPage() {
        if (isLoading) return
        
        if (isFiltering) {
            // In filtering mode, navigate through filtered results
            if (filterResult == null) return
            
            val totalPages = (filterResult!!.matchedRowIndices.size + pageSize - 1) / pageSize
            if (filterPageIndex >= totalPages - 1) {
                return
            }
            
            filterPageIndex++
            loadFilteredPage(filterPageIndex)
            return
        }
        
        // Normal paging mode
        if (!isPagingMode || currentPageData?.hasMore != true) {
            return
        }
        
        currentSkip += pageSize
        loadCurrentPage()
    }
    
    @FXML
    private fun onPrevPage() {
        if (isLoading) return
        
        if (isFiltering) {
            // In filtering mode, navigate through filtered results
            if (filterResult == null || filterPageIndex == 0) {
                return
            }
            
            filterPageIndex--
            loadFilteredPage(filterPageIndex)
            return
        }
        
        // Normal paging mode
        if (!isPagingMode || currentSkip == 0) {
            return
        }
        
        currentSkip = maxOf(0, currentSkip - pageSize)
        loadCurrentPage()
    }
    
    private fun loadCurrentPage() {
        currentFilePath?.let { path ->
            setLoadingState(true)
            statusLabel.text = "正在加载..."
            coroutineScope.launch {
                try {
                    currentPageData = parquetReader.readParquetFilePaged(path, pageSize, currentSkip)
                    
                    currentData = ParquetReader.ParquetData(
                        currentPageData!!.schema,
                        currentPageData!!.rows
                    )
                    
                    Platform.runLater {
                        displayData(currentData!!)
                        updatePaginationControls()
                        statusLabel.text = "加载完成"
                        setLoadingState(false)
                    }
                } catch (e: Exception) {
                    Platform.runLater {
                        statusLabel.text = "加载失败: ${e.message}"
                        showError("加载失败", e.message ?: "未知错误")
                        setLoadingState(false)
                    }
                }
            }
        }
    }
    
    private fun showError(title: String, message: String) {
        val alert = Alert(Alert.AlertType.ERROR)
        alert.title = title
        alert.headerText = null
        alert.contentText = message
        alert.showAndWait()
    }
    
    private fun showInfo(title: String, message: String) {
        val alert = Alert(Alert.AlertType.INFORMATION)
        alert.title = title
        alert.headerText = null
        alert.contentText = message
        alert.showAndWait()
    }
    
    /**
     * 设置表格的复制功能
     */
    private fun setupTableCopyFunctionality() {
        // Enable cell selection
        dataTableView.selectionModel.cellSelectionEnabledProperty().value = true
        dataTableView.selectionModel.selectionMode = SelectionMode.MULTIPLE
        
        // Add keyboard shortcut (Ctrl+C or Cmd+C)
        val copyKeyCombo = KeyCodeCombination(KeyCode.C, KeyCombination.CONTROL_DOWN)
        dataTableView.setOnKeyPressed { event ->
            if (copyKeyCombo.match(event)) {
                copySelectionToClipboard()
                event.consume()
            }
        }
        
        // Add context menu (right-click)
        val contextMenu = ContextMenu()
        
        val copyMenuItem = MenuItem("复制")
        copyMenuItem.setOnAction {
            copySelectionToClipboard()
        }
        
        val copyRowMenuItem = MenuItem("复制整行")
        copyRowMenuItem.setOnAction {
            copySelectedRowsToClipboard()
        }
        
        val copyColumnMenuItem = MenuItem("复制列名")
        copyColumnMenuItem.setOnAction {
            copySelectedColumnsToClipboard()
        }
        
        val copyAllMenuItem = MenuItem("复制所有数据")
        copyAllMenuItem.setOnAction {
            copyAllDataToClipboard()
        }
        
        contextMenu.items.addAll(copyMenuItem, copyRowMenuItem, copyColumnMenuItem, SeparatorMenuItem(), copyAllMenuItem)
        
        dataTableView.contextMenu = contextMenu
        
        // Show context menu on right click
        dataTableView.setOnContextMenuRequested { event ->
            if (dataTableView.selectionModel.selectedCells.isNotEmpty()) {
                contextMenu.show(dataTableView, event.screenX, event.screenY)
            }
        }
    }
    
    /**
     * 复制选中的单元格到剪贴板
     * 支持单个或多个单元格选择，以TSV格式复制（适合粘贴到Excel）
     */
    private fun copySelectionToClipboard() {
        val selectedCells = dataTableView.selectionModel.selectedCells
        
        if (selectedCells.isEmpty()) {
            return
        }
        
        try {
            // Group cells by row
            val cellsByRow = selectedCells.groupBy { it.row }
            
            val stringBuilder = StringBuilder()
            
            // Sort by row index
            cellsByRow.keys.sorted().forEach { rowIndex ->
                val cellsInRow = cellsByRow[rowIndex]!!.sortedBy { it.column }
                
                val rowData = mutableListOf<String>()
                cellsInRow.forEach { cell ->
                    val column = cell.tableColumn as? TableColumn<Map<String, Any?>, String>
                    if (column != null) {
                        val item = dataTableView.items[rowIndex]
                        val columnName = column.text
                        val value = item[columnName]?.toString() ?: ""
                        rowData.add(value)
                    }
                }
                
                stringBuilder.append(rowData.joinToString("\t"))
                stringBuilder.append("\n")
            }
            
            val content = ClipboardContent()
            content.putString(stringBuilder.toString().trim())
            Clipboard.getSystemClipboard().setContent(content)
            
            // Show feedback
            val cellCount = selectedCells.size
            statusLabel.text = "已复制 $cellCount 个单元格到剪贴板"
            
        } catch (e: Exception) {
            statusLabel.text = "复制失败: ${e.message}"
            e.printStackTrace()
        }
    }
    
    /**
     * 复制选中的整行到剪贴板
     */
    private fun copySelectedRowsToClipboard() {
        val selectedRows = dataTableView.selectionModel.selectedItems
        
        if (selectedRows.isEmpty()) {
            return
        }
        
        try {
            val stringBuilder = StringBuilder()
            
            // Add header row
            val schema = currentData?.schema ?: return
            stringBuilder.append(schema.joinToString("\t"))
            stringBuilder.append("\n")
            
            // Add data rows
            selectedRows.forEach { row ->
                val rowData = schema.map { columnName ->
                    row[columnName]?.toString() ?: ""
                }
                stringBuilder.append(rowData.joinToString("\t"))
                stringBuilder.append("\n")
            }
            
            val content = ClipboardContent()
            content.putString(stringBuilder.toString().trim())
            Clipboard.getSystemClipboard().setContent(content)
            
            statusLabel.text = "已复制 ${selectedRows.size} 行到剪贴板"
            
        } catch (e: Exception) {
            statusLabel.text = "复制失败: ${e.message}"
            e.printStackTrace()
        }
    }
    
    /**
     * 复制选中列的列名
     */
    private fun copySelectedColumnsToClipboard() {
        val selectedCells = dataTableView.selectionModel.selectedCells
        
        if (selectedCells.isEmpty()) {
            return
        }
        
        try {
            val columnNames = selectedCells
                .map { it.tableColumn.text }
                .distinct()
                .sorted()
            
            val content = ClipboardContent()
            content.putString(columnNames.joinToString("\t"))
            Clipboard.getSystemClipboard().setContent(content)
            
            statusLabel.text = "已复制 ${columnNames.size} 个列名到剪贴板"
            
        } catch (e: Exception) {
            statusLabel.text = "复制失败: ${e.message}"
            e.printStackTrace()
        }
    }
    
    /**
     * 复制所有数据到剪贴板（包括表头）
     */
    private fun copyAllDataToClipboard() {
        val data = currentData ?: return
        
        if (data.rows.isEmpty()) {
            statusLabel.text = "没有数据可复制"
            return
        }
        
        try {
            val stringBuilder = StringBuilder()
            
            // Add header row
            stringBuilder.append(data.schema.joinToString("\t"))
            stringBuilder.append("\n")
            
            // Add all data rows
            data.rows.forEach { row ->
                val rowData = data.schema.map { columnName ->
                    row[columnName]?.toString() ?: ""
                }
                stringBuilder.append(rowData.joinToString("\t"))
                stringBuilder.append("\n")
            }
            
            val content = ClipboardContent()
            content.putString(stringBuilder.toString().trim())
            Clipboard.getSystemClipboard().setContent(content)
            
            statusLabel.text = "已复制所有 ${data.rows.size} 行数据到剪贴板"
            
        } catch (e: Exception) {
            statusLabel.text = "复制失败: ${e.message}"
            e.printStackTrace()
        }
    }
}
