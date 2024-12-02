package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.lsm.LsmStorageOptions
import org.github.seonwkim.lsm.MiniLsm
import org.github.seonwkim.lsm.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.minilsm.week2.Utils.checkCompactionRatio
import org.github.seonwkim.minilsm.week2.Utils.compactionBench
import org.github.seonwkim.minilsm.week2.Utils.waitUntilCompactionEnds
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals

class Day2 {

    @Test
    fun `simple leveled compaction integration test`() {
        val storage = createSimpleLeveledCompactionMiniLsm()
        compactionBench(storage)
        checkCompactionRatio(storage)
    }

    @Test
    fun `simple leveled compaction concurrent write and read test`() {
        val storage =
            createSimpleLeveledCompactionMiniLsm()
        val availableProcessors = maxOf(Runtime.getRuntime().availableProcessors(), 5)
        val executors = (1..availableProcessors).map {
            Executors.newSingleThreadExecutor()
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
                val idx = future.get() // Wait for the future to complete and get the key
                if (future.isDone && !future.isCancelled) {
                    succeeded.add(idx)
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

    private fun createSimpleLeveledCompactionMiniLsm(): MiniLsm {
        val dir = createTempDirectory()
        return MiniLsm.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 1 shl 20,
                compactionOptions = SimpleLeveledCompactionOptions(
                    sizeRatioPercent = 200,
                    level0FileNumCompactionTrigger = 2,
                    maxLevels = 3
                ),
                enableWal = false,
                numMemTableLimit = 2,
                serializable = false
            )
        )
    }
}
