plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}
rootProject.name = "mini-lsm-kotlin"

include("core")
include("cli")
include("mini-lsm-mvcc")
include("mini-lsm-starter")

project(":core").projectDir = file("mini-lsm-mvcc")
