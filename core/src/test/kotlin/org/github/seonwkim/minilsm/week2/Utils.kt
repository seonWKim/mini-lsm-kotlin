package org.github.seonwkim.minilsm.week2

import mu.KotlinLogging
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.Unbounded
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.Configuration
import org.github.seonwkim.lsm.iterator.StorageIterator
import org.github.seonwkim.lsm.storage.MiniLsm
import org.github.seonwkim.lsm.storage.compaction.option.LeveledCompactionOptions
import org.github.seonwkim.lsm.storage.compaction.option.NoCompaction
import org.github.seonwkim.lsm.storage.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.lsm.storage.compaction.option.TieredCompactionOptions
import kotlin.test.assertEquals
import kotlin.test.assertTrue

object Utils {
    fun checkIterator(
        actual: StorageIterator,
        expected: List<Pair<ComparableByteArray, ComparableByteArray>>
    ) {
        for (e in expected) {
            assertTrue { actual.isValid() }
            assertEquals(actual.key(), e.first)
            assertEquals(actual.value(), e.second)
            actual.next()
        }
        assertTrue { !actual.isValid() }
    }

    private val log = KotlinLogging.logger { }

    fun compactionBench(storage: MiniLsm) {
        val keyMap = hashMapOf<Int, Int>()
        val genKey = { i: Int -> "%10d".format(i) }
        val genValue = { i: Int -> "%110d".format(i) }
        var maxKey = 0
        val overlaps = if (Configuration.TS_ENABLED) 10_000 else 20_000
        for (iter in 0 until 10) {
            val rangeBegin = iter * 5000
            for (i in rangeBegin until (rangeBegin + overlaps)) {
                val key = genKey(i)
                val version = keyMap.getOrDefault(i, 0) + 1
                val value = genValue(version)
                keyMap[i] = version
                storage.put(key.toComparableByteArray(), value.toComparableByteArray())
                maxKey = maxOf(maxKey, i)
            }
        }

        // wait until all memtables flush
        Thread.sleep(1000)
        while (storage.inner.state.immutableMemTables.readValue().isNotEmpty()) {
            storage.inner.forceFlushNextImmMemTable()
        }

        waitUntilCompactionEnds(storage)
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
        log.info {
            "This test case does not guarantee your compaction algorithm produces a LSM state as expected. " +
                    "It only does minimal checks on the size of the levels. " +
                    "Please use the compaction simulator to check if the compaction is correctly going on."
        }
    }

    fun waitUntilCompactionEnds(storage: MiniLsm) {
        var (prevL0Sstables, prevLevels) = storage.inner.state.diskSnapshot()
        while (true) {
            Thread.sleep(1000)
            val (currentL0Sstables, currentLevels) = storage.inner.state.diskSnapshot()
            if (prevL0Sstables == currentL0Sstables && prevLevels == currentLevels) {
                break
            } else {
                prevL0Sstables = currentL0Sstables
                prevLevels = currentLevels
            }

            log.info { "Waiting for compaction to end" }
        }
    }


    fun checkCompactionRatio(storage: MiniLsm) {
        val state = storage.inner.state
        val compactionOptions = storage.inner.options.compactionOptions
        val extraIterators = if (Configuration.TS_ENABLED) 1 else 0
        val levelSize = mutableListOf<Long>()
        val l0SstNum = state.l0Sstables.readValue().size
        for ((_, files) in state.levels.readValue()) {
            val size = when (compactionOptions) {
                is SimpleLeveledCompactionOptions -> {
                    files.size.toLong()
                }

                is TieredCompactionOptions -> {
                    files.size.toLong()
                }

                is LeveledCompactionOptions -> {
                    TODO()
                }

                else -> {
                    throw Error("Unreachable!!")
                }
            }
            levelSize.add(size)
        }

        val numIters = storage.scan(Unbounded, Unbounded).numActiveIterators()
        val numMemTables = storage.inner.state.immutableMemTables.readValue().size + 1
        when (compactionOptions) {
            is NoCompaction -> throw Error("Unreachable!!")
            is SimpleLeveledCompactionOptions -> {
                val (sizeRatioPercent, level0FileNumCompactionTrigger, maxLevels) = compactionOptions
                assertTrue { l0SstNum < level0FileNumCompactionTrigger }
                assertTrue { levelSize.size <= maxLevels }
                for (lowerLevelIdx in 1 until levelSize.size) {
                    val upperLevelSize = levelSize[lowerLevelIdx - 1]
                    val lowerLevelSize = levelSize[lowerLevelIdx]
                    if (upperLevelSize == 0L && lowerLevelSize == 0L) {
                        continue
                    }

                    val sizePercent = lowerLevelSize.toDouble() / upperLevelSize.toDouble()
                    val sizeRatio = sizeRatioPercent / 100.0
                    val levels = state.levels.readValue()
                    assertTrue(
                        "L${levels[lowerLevelIdx - 1].id}/L${levels[lowerLevelIdx].id}, ${sizePercent}<${sizeRatio}"
                    ) { sizePercent >= sizeRatio }
                }

                assertTrue(
                    "We found $numIters iterators in your implementation, (l0SstNum = $l0SstNum, numMemTables = $numMemTables, maxLevels = $maxLevels) did you use concat iterators?)"
                ) { numIters <= l0SstNum + numMemTables + maxLevels + extraIterators }
            }

            is LeveledCompactionOptions -> {
            }

            is TieredCompactionOptions -> {
                val sizeRatioTrigger = (100.0 + compactionOptions.sizeRatio.toDouble()) / 100.0
                assertEquals(l0SstNum, 0)
                assertTrue { levelSize.size <= compactionOptions.numTiers }

                var sumSize = levelSize[0]
                for (idx in 1 until levelSize.size) {
                    val thisSize = levelSize[idx]
                    val levels = state.levels.readValue()
                    if (levelSize.size > compactionOptions.minMergeWidth) {
                        assertTrue(
                            "violation of size ratio: sum(L${levels[0].id})/L${levels[0].id}, ${sumSize}/${thisSize}>${sizeRatioTrigger}"
                        ) {
                            sumSize.toDouble() / thisSize.toDouble() <= sizeRatioTrigger
                        }
                    }

                    if (idx + 1 == levelSize.size) {
                        assertTrue(
                            "violation of space amplification: sum(L${levels[idx - 1].id})/L${levels[idx].id}, ${sumSize}/${thisSize}>${compactionOptions.maxSizeAmplificationPercent}%"
                        ) {
                            (sumSize.toDouble() / thisSize.toDouble()) <= compactionOptions.maxSizeAmplificationPercent.toDouble() / 100.0
                        }
                    }

                    sumSize += thisSize
                }

                assertTrue(
                    "we found $numIters iterators in your implementation, (numMemTables=${numMemTables}, numTiers=${compactionOptions.numTiers} did you use concat iterators?"
                ) { numIters <= numMemTables + compactionOptions.numTiers + extraIterators }
            }
        }
    }
}
