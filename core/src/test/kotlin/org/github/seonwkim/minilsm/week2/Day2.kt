package org.github.seonwkim.minilsm.week2

import mu.KotlinLogging
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.Unbounded
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.Configuration
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.MiniLsm
import org.github.seonwkim.lsm.storage.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.minilsm.week2.Utils.checkCompactionRatio
import org.github.seonwkim.minilsm.week2.Utils.checkIterator
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day2 {

    private val log = KotlinLogging.logger { }

    @Test
    fun `test integration`() {
        val dir = createTempDirectory("test_integration")
        val storage = MiniLsm.open(
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

        compactionBatch(storage)
        checkCompactionRatio(storage)
    }

    private fun compactionBatch(storage: MiniLsm) {
        val keyMap = hashMapOf<Int, Int>()
        val genKey = { i: Int -> "%10d".format(i) }
        val genValue = { i: Int -> "%110d".format(i) }
        var maxKey = 0
        val overlaps = if (Configuration.TS_ENABLED) 10_000 else 20_000
        for (iter in 0 until 10) {
            val rangeBegin = iter * 5000
            for (i in rangeBegin until (rangeBegin + overlaps)) {
                val key = genKey(i)
                val version = keyMap[i]!! + 1
                val value = genValue(version)
                keyMap[i] = version
                storage.put(key.toComparableByteArray(), value.toComparableByteArray())
                maxKey = maxOf(maxKey, i)
            }
        }

        // wait until all memtables flush
        // TODO: use Awaitility
        Thread.sleep(1000)
        while (storage.inner.state.immutableMemTables.read().isNotEmpty()) {
            storage.inner.forceFlushNextImmMemTable()
        }

        var prevL0Sstables = storage.inner.state.l0Sstables.read().toList()
        var prevLevels = storage.inner.state.levels.read().toList()

        while (true) {
            Thread.sleep(1000)
            val currentL0Sstables = storage.inner.state.l0Sstables.read().toList()
            val currentLevels = storage.inner.state.levels.read().toList()

            if (prevL0Sstables == currentL0Sstables && prevLevels == currentLevels) {
                break
            } else {
                prevL0Sstables = currentL0Sstables
                prevLevels = currentLevels
            }

            log.info { "Waiting for compaction to converge" }
        }

        val expectedKeyValuePairs = mutableListOf<Pair<ComparableByteArray, ComparableByteArray>>()
        for (i in 0 until (maxKey + 40_000)) {
            val key = genKey(i)
            val value = storage.get(key)
            if (keyMap.containsKey(i)) {
                val expectedValue = genValue(keyMap[i]!!)
                assertEquals(value, expectedValue)
                expectedKeyValuePairs.add(Pair(key.toComparableByteArray(), value!!.toComparableByteArray()))
            } else {
                assertTrue { value == null }
            }
        }

        checkIterator(
            actual = storage.scan(Unbounded, Unbounded),
            expected = expectedKeyValuePairs
        )

        storage.dumpStructure()
        log.info { "This test case does not guarantee your compaction algorithm produces a LSM state as expected. " +
                "It only does minimal checks on the size of the levels. " +
                "Please use the compaction simulator to check if the compaction is correctly going on." }
    }
}
