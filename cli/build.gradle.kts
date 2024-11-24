plugins {
    application
}

application {
    mainClass.set("org.github.seonwkim.MainKt")
}

dependencies {
    implementation(project(":core"))
    implementation("org.jetbrains.kotlinx:kotlinx-cli:0.3.6")
    implementation("io.github.microutils:kotlin-logging")
    testImplementation(kotlin("test"))
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "org.github.seonwkim.MainKt"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.compileClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}
