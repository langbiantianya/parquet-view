module com.langbiantianya.parquetview {
    requires javafx.controls;
    requires javafx.fxml;
    requires javafx.web;
    requires kotlin.stdlib;

    requires org.controlsfx.controls;
    requires org.kordamp.ikonli.javafx;
    requires org.kordamp.bootstrapfx.core;
    requires eu.hansolo.tilesfx;
    
    // Kotlin Coroutines
    requires kotlinx.coroutines.core;
    requires kotlinx.coroutines.javafx;
    
    // SQL Parser
    requires net.sf.jsqlparser;
    
    // CLI and Terminal UI
    requires com.googlecode.lanterna;
    
    // Logging
    requires org.slf4j;
    requires ch.qos.logback.classic;
    requires ch.qos.logback.core;

    opens com.langbiantianya.parquetview to javafx.fxml;
    exports com.langbiantianya.parquetview;
}