package org.github.seonwkim.lsm

import com.google.common.annotations.VisibleForTesting
import mu.KotlinLogging
import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.compaction.option.LeveledCompactionOptions
import org.github.seonwkim.lsm.compaction.option.NoCompaction
import org.github.seonwkim.lsm.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.lsm.compaction.option.TieredCompactionOptions
import org.github.seonwkim.lsm.iterator.FusedIterator
import java.nio.file.Path
import java.util.concurrent.*

class MiniLsm private constructor(
    val inner: LsmStorageInner,
) {

    private val log = KotlinLogging.logger { }

    class SingleThreadedScheduler(capacity: Int) : ScheduledThreadPoolExecutor(1) {
        private val queue = LinkedBlockingQueue<Runnable>(capacity)

        override fun getQueue(): LinkedBlockingQueue<Runnable> {
            return queue
        }
    }

    private val flushScheduler = SingleThreadedScheduler(10)
    private val compactionScheduler = SingleThreadedScheduler(10)

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

    fun scan(lower: Bound, upper: Bound): FusedIterator {
        return inner.scan(lower, upper)
    }

    fun close() {

    }

    @VisibleForTesting
    fun dumpStructure() {
        inner.dumpStructure()
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
            is SimpleLeveledCompactionOptions,
            is LeveledCompactionOptions,
            is TieredCompactionOptions -> {
                compactionScheduler.scheduleWithFixedDelay(
                    {
                        try {
                            inner.triggerCompaction()
                        } catch (e: Exception) {
                            log.error { "Compaction operation failed: $e" }
                        }
                    },
                    inner.options.compactionIntervalMillis,
                    inner.options.compactionIntervalMillis,
                    TimeUnit.MILLISECONDS
                )
            }

            is NoCompaction -> {
                // do nothing
            }
        }
    }
}
