package org.github.seonwkim.lsm.storage

import mu.KotlinLogging
import org.github.seonwkim.common.ComparableByteArray
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class MiniLsm private constructor(val inner: LsmStorageInner) {

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

    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        inner.put(key, value)
    }

    fun scheduleFlush() {
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

    fun scheduleCompaction() {
        when (inner.options.compactionOptions) {
            is CompactionOptions.Simple,
            is CompactionOptions.Leveled,
            is CompactionOptions.Tiered -> {
                // TODO
            }

            is CompactionOptions.NoCompaction -> {
                // do nothing
            }
        }
    }
}
