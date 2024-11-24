package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.Configuration
import org.github.seonwkim.lsm.iterator.MergeIterator
import org.github.seonwkim.lsm.iterator.SsTableIterator
import org.github.seonwkim.lsm.iterator.StorageIterator
import org.github.seonwkim.lsm.storage.LsmStorageInner
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.LsmStorageState
import org.github.seonwkim.lsm.storage.NoCompaction
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day1 {

    @Test
    fun `test task1 full compaction`() {
        val dir = createTempDirectory("test_task1_full_compaction")
        val storage = LsmStorageInner.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                compactionOptions = NoCompaction,
                enableWal = false,
                numMemTableLimit = 50,
                serializable = false
            )
        )

        storage.put("0".toComparableByteArray(), "v1".toComparableByteArray())
        sync(storage)

        storage.put("0".toComparableByteArray(), "v2".toComparableByteArray())
        storage.put("1".toComparableByteArray(), "v2".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "v2".toComparableByteArray())
        sync(storage)

        storage.delete("0".toComparableByteArray())
        storage.delete("2".toComparableByteArray())
        sync(storage)

        assertEquals(storage.snapshot().getL0SstablesSize(), 3)
        val iter = constructMergeIteratorOverStorage(storage.snapshot().state)

        if (Configuration.TS_ENABLED) {
            checkIterator(
                actual = iter,
                expected = listOf(
                    Pair("0".toComparableByteArray(), "".toComparableByteArray()),
                    Pair("0".toComparableByteArray(), "v2".toComparableByteArray()),
                    Pair("0".toComparableByteArray(), "v1".toComparableByteArray()),
                    Pair("1".toComparableByteArray(), "v2".toComparableByteArray()),
                    Pair("2".toComparableByteArray(), "".toComparableByteArray()),
                    Pair("2".toComparableByteArray(), "v2".toComparableByteArray()),
                )
            )
        } else {
            checkIterator(
                actual = iter,
                expected = listOf(
                    Pair("0".toComparableByteArray(), "".toComparableByteArray()),
                    Pair("1".toComparableByteArray(), "v2".toComparableByteArray()),
                    Pair("2".toComparableByteArray(), "".toComparableByteArray()),
                )
            )
        }

    }

    private fun sync(storage: LsmStorageInner) {
        storage.forceFreezeMemTable()
        storage.forceFlushNextImmMemTable()
    }

    private fun constructMergeIteratorOverStorage(
        state: LsmStorageState
    ): MergeIterator<SsTableIterator> {
        val iters = mutableListOf<SsTableIterator>()
        state.l0Sstables.forEach {
            iters.add(SsTableIterator.createAndSeekToFirst(state.sstables[it]!!))
        }

        state.levels.forEach { level ->
            level.sstIds.forEach {
                iters.add(SsTableIterator.createAndSeekToFirst(state.sstables[it]!!))
            }
        }

        return MergeIterator(iters)
    }

    private fun checkIterator(
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
}
