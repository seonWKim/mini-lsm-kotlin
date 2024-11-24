package org.github.seonwkim.lsm

object Configuration {
    val TS_DEFAULT: Long = System.getenv("MINI_LSM_TS_DEFAULT")?.toLong() ?: 0L

    // TODO: update the default value to true when TS is enabled
    val TS_ENABLED: Boolean = System.getenv("MINI_LSM_TS_ENABLED")?.toBoolean() ?: false
}
