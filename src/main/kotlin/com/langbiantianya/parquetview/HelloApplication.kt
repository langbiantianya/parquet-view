package com.langbiantianya.parquetview

import javafx.application.Application
import javafx.fxml.FXMLLoader
import javafx.scene.Scene
import javafx.stage.Stage

class HelloApplication : Application() {
    override fun start(stage: Stage) {
        val fxmlLoader = FXMLLoader(HelloApplication::class.java.getResource("parquet-view.fxml"))
        val scene = Scene(fxmlLoader.load(), 1000.0, 700.0)
        stage.title = "Parquet 文件查看器"
        stage.scene = scene
        stage.show()
    }
}
  
