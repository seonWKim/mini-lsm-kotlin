package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.iterator.StorageIterator
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
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemTableLimit = 50
            )
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
            storage.scan(Bound.Unbounded, Bound.Unbounded),
            listOf(
                Pair("0".toComparableByteArray(), "2333333".toComparableByteArray()),
                Pair("00".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("3".toComparableByteArray(), "23333".toComparableByteArray()),
            )
        )

        checkIterator(
            storage.scan(
                Bound.Included("1".toComparableByteArray()),
                Bound.Included("2".toComparableByteArray())
            ),
            listOf(
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
            )
        )

        checkIterator(
            storage.scan(
                Bound.Excluded("1".toComparableByteArray()),
                Bound.Excluded("3".toComparableByteArray())
            ),
            listOf(
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task1 storage get`() {
        val dir = createTempDirectory("test_task1_storage_get")
        val storage = LsmStorageInner.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemTableLimit = 50
            )
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
        val storage = MiniLsm.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemTableLimit = 2
            )
        )

        val oneKBData = "1".repeat(1024).toComparableByteArray()

        // approximately 6MB
        repeat(6_000) {
            storage.put("$it".toComparableByteArray(), oneKBData)
        }

        Thread.sleep(500)
        assertTrue { storage.inner.state.read().l0SsTables.isNotEmpty() }
    }

    @Test
    fun `test task3 sst filter`() {
        val dir = createTempDirectory("test_task3_sst_filter")
        val storage = LsmStorageInner.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemTableLimit = 50
            )
        )

        for (i in 1..10_000) {
            if (i % 1000 == 0) {
                sync(storage)
            }
            storage.put("%05d".format(i).toComparableByteArray(), "2333333".toComparableByteArray())
        }

        val iter1 = storage.scan(Bound.Unbounded, Bound.Unbounded)
        assertTrue { iter1.numActiveIterators() >= 10 }

        val maxNum = iter1.numActiveIterators()
        val iter2 = storage.scan(
            lower = Bound.Excluded("%05d".format(10000).toComparableByteArray()),
            upper = Bound.Unbounded
        )
        assertTrue { iter2.numActiveIterators() < maxNum }

        val minNum = iter2.numActiveIterators()
        val iter3 = storage.scan(
            lower = Bound.Unbounded,
            upper = Bound.Excluded("%05d".format(1).toComparableByteArray())
        )
        assertTrue { iter3.numActiveIterators() == minNum }

        val iter4 = storage.scan(
            lower = Bound.Unbounded,
            upper = Bound.Included("%05d".format(0).toComparableByteArray())
        )
        assertTrue { iter4.numActiveIterators() == minNum }

        val iter5 = storage.scan(
            lower = Bound.Included("%05d".format(10001).toComparableByteArray()),
            upper = Bound.Unbounded
        )
        assertTrue { iter5.numActiveIterators() == minNum }

        val iter6 = storage.scan(
            lower = Bound.Included("%05d".format(5000).toComparableByteArray()),
            upper = Bound.Excluded("%05d".format(6000).toComparableByteArray())
        )
        assertTrue { iter6.numActiveIterators() in minNum..<maxNum }
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
}