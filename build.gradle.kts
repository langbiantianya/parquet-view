import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    application
    id("org.jetbrains.kotlin.jvm") version "2.3.10"
    id("org.javamodularity.moduleplugin") version "2.0.0"
    id("org.openjfx.javafxplugin") version "0.1.0"
    id("org.beryx.jlink") version "3.2.1"
    id("com.gradleup.shadow") version "9.2.0+"
}

group = "com.langbiantianya"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val junitVersion = "5.12.1"


tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

application {
    mainModule.set("com.langbiantianya.parquetview")
    mainClass.set("com.langbiantianya.parquetview.HelloApplication")
}
kotlin {
    jvmToolchain(21)
}
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
    targetCompatibility = JavaVersion.VERSION_21
    sourceCompatibility = JavaVersion.VERSION_21
}

javafx {
    version = "21.0.6"
    modules = listOf("javafx.controls", "javafx.fxml", "javafx.web", "javafx.swing")
    configuration = "implementation"
}

dependencies {
    // JavaFX platform-specific dependencies for cross-platform JAR
    // Note: linux-aarch64 is not available in JavaFX 21.0.6
    val javafxVersion = "21.0.6"
    val platforms = listOf("win", "linux", "mac", "mac-aarch64")
    for (platform in platforms) {
        implementation("org.openjfx:javafx-base:${javafxVersion}:${platform}")
        implementation("org.openjfx:javafx-controls:${javafxVersion}:${platform}")
        implementation("org.openjfx:javafx-fxml:${javafxVersion}:${platform}")
        implementation("org.openjfx:javafx-graphics:${javafxVersion}:${platform}")
        implementation("org.openjfx:javafx-web:${javafxVersion}:${platform}")
        implementation("org.openjfx:javafx-swing:${javafxVersion}:${platform}")
    }
    
    implementation("org.controlsfx:controlsfx:11.2.1")
    implementation("org.kordamp.ikonli:ikonli-javafx:12.3.1")
    implementation("org.kordamp.bootstrapfx:bootstrapfx-core:0.4.0")
    implementation("eu.hansolo:tilesfx:21.0.9") {
        exclude(group = "org.openjfx")
    }
    
    // Kotlin Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-javafx:1.10.2")
    
    // Parquet dependencies
    implementation("org.apache.parquet:parquet-hadoop:1.17.0")
    implementation("org.apache.parquet:parquet-column:1.17.0")
    implementation("org.apache.parquet:parquet-common:1.17.0")
    implementation("org.apache.hadoop:hadoop-common:3.4.3")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.3")
    
    // Hadoop local filesystem support
    implementation("org.apache.hadoop:hadoop-client:3.4.3")
    
    // Apache Calcite for SQL parsing and optimization
    implementation("org.apache.calcite:calcite-core:1.40.0")
    
    // Logging - Logback
    implementation("ch.qos.logback:logback-classic:1.5.32")
    implementation("org.slf4j:slf4j-api:2.0.17")
    
    // SQL Parser
    implementation("com.github.jsqlparser:jsqlparser:5.3")
    
    // Terminal UI and CLI
    implementation("com.github.ajalt.clikt:clikt:5.1.0")
    implementation("com.jakewharton.picnic:picnic:0.7.0")
    implementation("com.googlecode.lanterna:lanterna:3.1.3")
    
    testImplementation("org.junit.jupiter:junit-jupiter-api:${junitVersion}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${junitVersion}")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

jlink {
    imageZip.set(layout.buildDirectory.file("/distributions/app-${javafx.platform.classifier}.zip"))
    options.set(listOf("--strip-debug", "--compress", "2", "--no-header-files", "--no-man-pages"))
    launcher {
        name = "app"
    }
}

// Task to run CLI version
tasks.register<JavaExec>("runCli") {
    group = "application"
    description = "Run the CLI version of parquet-view"
    
    mainClass.set("com.langbiantianya.parquetview.ParquetCliKt")
    classpath = sourceSets["main"].runtimeClasspath
    standardInput = System.`in`
    
    // Allow passing arguments: ./gradlew runCli --args="path/to/file.parquet -i"
    args = if (project.hasProperty("appArgs")) {
        (project.property("appArgs") as String).split("\\s+".toRegex())
    } else {
        emptyList()
    }
}

// Configure Shadow plugin for CLI
tasks.shadowJar {
    archiveBaseName.set("parquet-view-cli")
    archiveClassifier.set("")
    archiveVersion.set(version.toString())
    
    // Enable zip64 for large archives
    isZip64 = true
    
    manifest {
        attributes["Main-Class"] = "com.langbiantianya.parquetview.ParquetCliKt"
    }
    
    // Exclude JavaFX modules (not needed for CLI)
    exclude("javafx/**")
    exclude("com/sun/javafx/**")
    exclude("META-INF/versions/*/module-info.class")
    
    // Merge service files
    mergeServiceFiles()
    
    // Exclude signature files from signed JARs
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

// Create GUI version with JavaFX
tasks.register<ShadowJar>("guiJar") {
    group = "build"
    description = "Create a standalone GUI jar with JavaFX"
    
    archiveBaseName.set("parquet-view-gui")
    archiveClassifier.set("")
    archiveVersion.set(version.toString())
    
    // Enable zip64 for large archives
    isZip64 = true
    
    manifest {
        attributes["Main-Class"] = "com.langbiantianya.parquetview.LauncherKt"
    }
    
    from(sourceSets["main"].output)
    configurations = listOf(project.configurations.runtimeClasspath.get())
    
    // Merge service files
    mergeServiceFiles()
    
    // Exclude signature files from signed JARs
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

// Alias for shadowJar (CLI version)
tasks.register("cliJar") {
    group = "build"
    description = "Alias for shadowJar - Create a standalone CLI jar"
    dependsOn(tasks.shadowJar)
}

// Task to build both versions
tasks.register("buildAll") {
    group = "build"
    description = "Build both CLI and GUI versions"
    dependsOn(tasks.shadowJar, tasks.named("guiJar"))
}
