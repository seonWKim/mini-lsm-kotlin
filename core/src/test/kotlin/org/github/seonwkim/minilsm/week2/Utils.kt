package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.Unbounded
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


    fun checkCompactionRatio(storage: MiniLsm) {
        val state = storage.inner.state
        val compactionOptions = storage.inner.options.compactionOptions
        val extraIterators = if (Configuration.TS_ENABLED) 1 else 0
        val levelSize = mutableListOf<Long>()
        val l0SstNum = state.l0Sstables.read().size
        for ((_, files) in state.levels.read()) {
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
        val numMemTables = storage.inner.state.immutableMemTables.read().size + 1
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
                    val levels = state.levels.read()
                    assertTrue(
                        "L${levels[lowerLevelIdx-1].level}/L${levels[lowerLevelIdx].level}, ${sizePercent}<${sizeRatio}"
                    ) { sizePercent >= sizeRatio }
                }

                assertTrue(
                    "We found $numIters iterators in your implementation, (l0SstNum = $l0SstNum, numMemTables = $numMemTables, maxLevels = $maxLevels) did you use concat iterators?)"
                ) { numIters <= l0SstNum + numMemTables + maxLevels + extraIterators }
            }

            is LeveledCompactionOptions -> {
                TODO()
            }

            is TieredCompactionOptions -> {
                TODO()
            }
        }
    }
}
