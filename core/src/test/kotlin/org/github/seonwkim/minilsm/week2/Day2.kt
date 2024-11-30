package org.github.seonwkim.minilsm.week2

import mu.KotlinLogging
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.MiniLsm
import org.github.seonwkim.lsm.storage.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.minilsm.week2.Utils.checkCompactionRatio
import org.github.seonwkim.minilsm.week2.Utils.compactionBench
import org.github.seonwkim.minilsm.week2.Utils.waitUntilCompactionEnds
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals

class Day2 {

    private val log = KotlinLogging.logger { }

    @Test
    fun `simple leveled compaction integration test`() {
        val storage = createSimpleLeveledCompactionMiniLsm("simple_leveled_compaction_integration_test")
        compactionBench(storage)
        checkCompactionRatio(storage)
    }

    @Test
    fun `simple leveled compaction concurrent write and read test`() {
        val storage = createSimpleLeveledCompactionMiniLsm("simple_leveled_compaction_concurrent_write_and_read_test")
        val availableProcessors = maxOf(Runtime.getRuntime().availableProcessors(), 5)
        val executors = (1..availableProcessors).map {
            Executors.newVirtualThreadPerTaskExecutor()
        }

        val genKey = { it: Int -> "key_%10d".format(it) }
        val genValue = { it: Int -> "value_%100d".format(it) }
        val futures = mutableListOf<Future<*>>()
        for (i in 0 until 100_000) {
            val key = genKey(i)
            val value = genValue(i)

            executors[i % availableProcessors].submit { storage.put(key, value) }.let { futures.add(it) }
        }
        futures.forEach { it.get() }
        waitUntilCompactionEnds(storage)

        for (i in 0 until 100_000) {
            assertEquals(genValue(i), storage.get(genKey(i)))
        }
    }

    private fun createSimpleLeveledCompactionMiniLsm(dirName: String): MiniLsm {
        val dir = createTempDirectory(dirName)
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
