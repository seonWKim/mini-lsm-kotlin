package org.github.seonwkim.lsm

object Configuration {
    val TS_ENABLED: Boolean = System.getenv("MINI_LSM_TS_ENABLED")?.toBoolean() ?: true
}
