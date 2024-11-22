package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.BoundFlag
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.iterator.StorageIterator
import org.github.seonwkim.lsm.storage.CompactionOptions
import org.github.seonwkim.lsm.storage.LsmStorageInner
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.MiniLsm
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day6 {

    @Test
    fun `test task1 storage scan`() {
        val dir = createTempDirectory("test_task1_storage_scan")
        val storage = LsmStorageInner.open(
            path = dir,
            options = lsmStorageOptionForTest()
        )
        storage.put("0".toComparableByteArray(), "2333333".toComparableByteArray())
        storage.put("00".toComparableByteArray(), "2333333".toComparableByteArray())
        storage.put("4".toComparableByteArray(), "23".toComparableByteArray())
        sync(storage)

        storage.delete("4".toComparableByteArray())
        sync(storage)

        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        storage.state.withWriteLock { storage.forceFreezeMemTable(it) }

        storage.put("00".toComparableByteArray(), "2333".toComparableByteArray())
        storage.state.withWriteLock { storage.forceFreezeMemTable(it) }

        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        storage.delete("1".toComparableByteArray())

        storage.state.withReadLock {
            assertEquals(it.l0SsTables.size, 2)
            assertEquals(it.immutableMemTables.size, 2)
        }

        checkIterator(
            storage.scan(Bound.unbounded(), Bound.unbounded()),
            listOf(
                Pair("0".toComparableByteArray(), "2333333".toComparableByteArray()),
                Pair("00".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("3".toComparableByteArray(), "23333".toComparableByteArray()),
            )
        )

        checkIterator(
            storage.scan(
                Bound("1".toComparableByteArray(), BoundFlag.INCLUDED),
                Bound("2".toComparableByteArray(), BoundFlag.INCLUDED)
            ),
            listOf(
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
            )
        )

        checkIterator(
            storage.scan(
                Bound("1".toComparableByteArray(), BoundFlag.EXCLUDED),
                Bound("3".toComparableByteArray(), BoundFlag.EXCLUDED)
            ),
            listOf(
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task1 storage get`() {
        val dir = createTempDirectory("test_task1_storage_get")
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        storage.put("0".toComparableByteArray(), "2333333".toComparableByteArray())
        storage.put("00".toComparableByteArray(), "2333333".toComparableByteArray())
        storage.put("4".toComparableByteArray(), "23".toComparableByteArray())
        sync(storage)

        storage.delete("4".toComparableByteArray())
        sync(storage)

        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        storage.state.withWriteLock { storage.forceFreezeMemTable(it) }

        storage.put("00".toComparableByteArray(), "2333".toComparableByteArray())
        storage.state.withWriteLock { storage.forceFreezeMemTable(it) }

        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        storage.delete("1".toComparableByteArray())

        storage.state.withReadLock {
            assertEquals(it.l0SsTables.size, 2)
            assertEquals(it.immutableMemTables.size, 2)
        }

        assertEquals(storage.get("0".toComparableByteArray()), "2333333".toComparableByteArray())
        assertEquals(storage.get("00".toComparableByteArray()), "2333".toComparableByteArray())
        assertEquals(storage.get("2".toComparableByteArray()), "2333".toComparableByteArray())
        assertEquals(storage.get("3".toComparableByteArray()), "23333".toComparableByteArray())
        assertEquals(storage.get("4".toComparableByteArray()), null)
        assertEquals(storage.get("--".toComparableByteArray()), null)
        assertEquals(storage.get("555".toComparableByteArray()), null)
    }

    @Test
    fun `test task2 auto flush`() {
        val dir = createTempDirectory("test_task2_auto_flush")
        val storage = MiniLsm.open(dir, lsmStorageOptionForTest())

        val oneKBData = "1".repeat(1024).toComparableByteArray()

        // approximately 6MB
        repeat(6_000) {
            storage.put("$it".toComparableByteArray(), oneKBData)
        }

        Thread.sleep(500)
        assertTrue { storage.inner.state.read().l0SsTables.isNotEmpty() }
    }

    private fun sync(storage: LsmStorageInner) {
        storage.state.withWriteLock {
            storage.forceFreezeMemTable(it)
        }
        storage.forceFlushNextImmMemTable()
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
    }

    private fun lsmStorageOptionForTest(): LsmStorageOptions = LsmStorageOptions(
        blockSize = 4096,
        targetSstSize = 2 shl 20,
        compactionOptions = CompactionOptions.NoCompaction,
        enableWal = false,
        numMemTableLimit = 2
    )
}
