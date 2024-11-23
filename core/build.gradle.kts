dependencies {
    implementation("com.google.guava:guava")
    implementation("com.github.ben-manes.caffeine:caffeine")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("ch.qos.logback:logback-classic")
    implementation("io.github.microutils:kotlin-logging")

    testImplementation(kotlin("test"))
    testImplementation("com.tngtech.archunit:archunit-junit5:1.3.0")
}
