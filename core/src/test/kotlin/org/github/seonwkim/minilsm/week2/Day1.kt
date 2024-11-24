package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.Configuration
import org.github.seonwkim.lsm.iterator.MergeIterator
import org.github.seonwkim.lsm.iterator.SsTableIterator
import org.github.seonwkim.lsm.storage.LsmStorageInner
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.LsmStorageState
import org.github.seonwkim.lsm.storage.NoCompaction
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals

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

        assertEquals(storage.state.read().l0SsTables.size, 3)
        val iter = constructMergeIteratorOverStorage(storage.state.read())

        if (Configuration.TS_ENABLED) {

        }
    }

    private fun sync(storage: LsmStorageInner) {
        storage.state.withWriteLock {
            storage.forceFreezeMemTable(it)
        }
        storage.forceFlushNextImmMemTable()
    }

    private fun constructMergeIteratorOverStorage(
        state: LsmStorageState
    ): MergeIterator {
        val iters = mutableListOf<SsTableIterator>()
        state.l0SsTables.forEach {
            iters.add(SsTableIterator.createAndSeekToFirst(state.ssTables[it]!!))
        }

        state.levels.forEach { level ->
            level.tables.forEach {
                iters.add(SsTableIterator.createAndSeekToFirst(state.ssTables[it]!!))
            }
        }

        return MergeIterator(iters)
    }
}
