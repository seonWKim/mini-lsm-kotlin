package org.github.seonwkim.lsm.storage

import mu.KotlinLogging
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.toComparableByteArray
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class MiniLsm private constructor(
    val inner: LsmStorageInner,
) {

    private val log = KotlinLogging.logger { }

    private val flushScheduler = Executors.newSingleThreadScheduledExecutor()
    private val compactionScheduler = Executors.newSingleThreadScheduledExecutor()

    companion object {
        fun open(path: Path, options: LsmStorageOptions): MiniLsm {
            val inner = LsmStorageInner.open(path, options)
            val lsm = MiniLsm(inner)
            lsm.scheduleFlush()
            lsm.scheduleCompaction()
            return lsm
        }
    }

    fun get(key: String): String? {
        return inner.get(key.toComparableByteArray())?.toString()
    }

    fun get(key: ComparableByteArray): ComparableByteArray? {
        return inner.get(key)
    }

    fun put(key: String, value: String) {
        inner.put(key.toComparableByteArray(), value.toComparableByteArray())
    }

    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        inner.put(key, value)
    }

    fun delete(key: String) {
        inner.delete(key.toComparableByteArray())
    }

    private fun scheduleFlush() {
        flushScheduler.scheduleWithFixedDelay(
            {
                try {
                    inner.triggerFlush()
                } catch (e: Exception) {
                    log.error { "Flush operation failed: $e" }
                }
            },
            inner.options.flushIntervalMillis,
            inner.options.flushIntervalMillis,
            TimeUnit.MILLISECONDS
        )
    }

    private fun scheduleCompaction() {
        when (inner.options.compactionOptions) {
            is Simple,
            is Leveled,
            is Tiered -> {
                // TODO
            }

            is NoCompaction -> {
                // do nothing
            }
        }
    }
}
