plugins {
    kotlin("jvm") version "2.0.21"
}

group = "org.github.seonwkim"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")

    repositories {
        mavenCentral()
    }

    dependencies {
        implementation("com.google.guava:guava:33.3.1-jre")
        implementation("com.github.ben-manes.caffeine:caffeine:3.1.8")

        implementation("com.fasterxml.jackson.core:jackson-databind:2.18.1")
        implementation("ch.qos.logback:logback-classic:1.5.12")
        implementation("io.github.microutils:kotlin-logging:2.0.4")

        testImplementation(kotlin("test"))
    }

    tasks.test {
        useJUnitPlatform()
    }

    kotlin {
        jvmToolchain(21)
    }
}
