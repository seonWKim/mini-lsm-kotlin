plugins {
    kotlin("jvm") version "2.0.21"
}

group = "org.seonWKim"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.google.guava:guava:33.3.1-jre")
    implementation("com.tngtech.archunit:archunit-junit5:0.23.1")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}
