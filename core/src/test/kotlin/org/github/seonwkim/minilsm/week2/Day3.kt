package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.MiniLsm
import org.github.seonwkim.lsm.storage.compaction.option.TieredCompactionOptions
import org.github.seonwkim.minilsm.week2.Utils.checkCompactionRatio
import org.github.seonwkim.minilsm.week2.Utils.compactionBench
import org.github.seonwkim.minilsm.week2.Utils.waitUntilCompactionEnds
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals

class Day3 {

    @Test
    fun `tiered compaction integration test`() {
        val storage = createTieredCompactionMiniLsm("tiered_compaction_integration_test")
        compactionBench(storage)
        checkCompactionRatio(storage)
    }

    @Test
    fun `tiered compaction concurrent write and read test`() {
        val storage = createTieredCompactionMiniLsm("tiered_compaction_concurrent_write_and_read_test")
        val availableProcessors = maxOf(Runtime.getRuntime().availableProcessors(), 5)
        val executors = (1..availableProcessors).map {
            Executors.newVirtualThreadPerTaskExecutor()
        }

        val genKey = { it: Int -> "key_%10d".format(it) }
        val genValue = { it: Int -> "value_%100d".format(it) }
        val futures = mutableListOf<Future<Int>>()
        val succeeded = hashSetOf<Int>()
        var failureCount = 0
        for (i in 0 until 100_000) {
            val key = genKey(i)
            val value = genValue(i)

            executors[i % availableProcessors].submit<Int> {
                storage.put(key, value)
                i
            }.let { futures.add(it) }
        }
        futures.forEach { future ->
            try {
                val key = future.get() // Wait for the future to complete and get the key
                if (future.isDone && !future.isCancelled) {
                    succeeded.add(key)
                }
            } catch (e: Exception) {
                failureCount++
                println("Future failed: ${e.message}")
            }
        }

        waitUntilCompactionEnds(storage)
        println("Succeeded count: ${succeeded.size}, failure count: $failureCount")

        for (i in succeeded) {
            assertEquals(genValue(i), storage.get(genKey(i)))
        }
    }

    private fun createTieredCompactionMiniLsm(dirName: String): MiniLsm {
        val dir = createTempDirectory(dirName)
        return MiniLsm.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 1 shl 20,
                compactionOptions = TieredCompactionOptions(
                    numTiers = 3,
                    maxSizeAmplificationPercent = 200,
                    sizeRatio = 1,
                    minMergeWidth = 2,
                    maxMergeWidth = null
                ),
                enableWal = false,
                numMemTableLimit = 2,
                serializable = false
            )
        )
    }
}
