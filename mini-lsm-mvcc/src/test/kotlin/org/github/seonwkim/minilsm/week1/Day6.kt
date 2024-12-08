package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.iterator.StorageIterator
import org.github.seonwkim.lsm.LsmStorageInner
import org.github.seonwkim.lsm.LsmStorageOptions
import org.github.seonwkim.lsm.MiniLsm
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day6 {

    @Test
    fun `test task1 storage scan`() {
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemTableLimit = 50
            )
        )
        storage.put("0".toTimestampedByteArray(), "2333333".toTimestampedByteArray())
        storage.put("00".toTimestampedByteArray(), "2333333".toTimestampedByteArray())
        storage.put("4".toTimestampedByteArray(), "23".toTimestampedByteArray())
        sync(storage)

        storage.delete("4".toTimestampedByteArray())
        sync(storage)

        storage.put("1".toTimestampedByteArray(), "233".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.put("00".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.put("3".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        storage.delete("1".toTimestampedByteArray())

        assertEquals(storage.getL0SstablesSize(), 2)
        assertEquals(storage.getImmutableMemTablesSize(), 2)

        checkIterator(
            storage.scan(Unbounded, Unbounded),
            listOf(
                Pair("0".toTimestampedByteArray(), "2333333".toTimestampedByteArray()),
                Pair("00".toTimestampedByteArray(), "2333".toTimestampedByteArray()),
                Pair("2".toTimestampedByteArray(), "2333".toTimestampedByteArray()),
                Pair("3".toTimestampedByteArray(), "23333".toTimestampedByteArray()),
            )
        )

        checkIterator(
            storage.scan(
                Included("1".toTimestampedByteArray()),
                Included("2".toTimestampedByteArray())
            ),
            listOf(
                Pair("2".toTimestampedByteArray(), "2333".toTimestampedByteArray()),
            )
        )

        checkIterator(
            storage.scan(
                Excluded("1".toTimestampedByteArray()),
                Excluded("3".toTimestampedByteArray())
            ),
            listOf(
                Pair("2".toTimestampedByteArray(), "2333".toTimestampedByteArray()),
            )
        )
    }

    @Test
    fun `test task1 storage get`() {
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemTableLimit = 50
            )
        )
        storage.put("0".toTimestampedByteArray(), "2333333".toTimestampedByteArray())
        storage.put("00".toTimestampedByteArray(), "2333333".toTimestampedByteArray())
        storage.put("4".toTimestampedByteArray(), "23".toTimestampedByteArray())
        sync(storage)

        storage.delete("4".toTimestampedByteArray())
        sync(storage)

        storage.put("1".toTimestampedByteArray(), "233".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.put("00".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.put("3".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        storage.delete("1".toTimestampedByteArray())

        assertEquals(storage.getL0SstablesSize(), 2)
        assertEquals(storage.getImmutableMemTablesSize(), 2)

        assertEquals(storage.get("0".toTimestampedByteArray()), "2333333".toTimestampedByteArray())
        assertEquals(storage.get("00".toTimestampedByteArray()), "2333".toTimestampedByteArray())
        assertEquals(storage.get("2".toTimestampedByteArray()), "2333".toTimestampedByteArray())
        assertEquals(storage.get("3".toTimestampedByteArray()), "23333".toTimestampedByteArray())
        assertEquals(storage.get("4".toTimestampedByteArray()), null)
        assertEquals(storage.get("--".toTimestampedByteArray()), null)
        assertEquals(storage.get("555".toTimestampedByteArray()), null)
    }

    @Test
    fun `test task2 auto flush`() {
        val dir = createTempDirectory()
        val storage = MiniLsm.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemTableLimit = 2
            )
        )

        val oneKBData = "1".repeat(1024).toTimestampedByteArray()

        // approximately 6MB
        repeat(6_000) {
            storage.put("$it".toTimestampedByteArray(), oneKBData)
        }

        Thread.sleep(500)
        assertTrue { storage.inner.getL0SstablesSize() > 0 }
    }

    @Test
    fun `test task3 sst filter`() {
        val dir = createTempDirectory()
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
            storage.put("%05d".format(i).toTimestampedByteArray(), "2333333".toTimestampedByteArray())
        }

        val iter1 = storage.scan(Unbounded, Unbounded)
        assertTrue { iter1.numActiveIterators() >= 10 }

        val maxNum = iter1.numActiveIterators()
        val iter2 = storage.scan(
            lower = Excluded("%05d".format(10000).toTimestampedByteArray()),
            upper = Unbounded
        )
        assertTrue { iter2.numActiveIterators() < maxNum }

        val minNum = iter2.numActiveIterators()
        val iter3 = storage.scan(
            lower = Unbounded,
            upper = Excluded("%05d".format(1).toTimestampedByteArray())
        )
        assertTrue { iter3.numActiveIterators() == minNum }

        val iter4 = storage.scan(
            lower = Unbounded,
            upper = Included("%05d".format(0).toTimestampedByteArray())
        )
        assertTrue { iter4.numActiveIterators() == minNum }

        val iter5 = storage.scan(
            lower = Included("%05d".format(10001).toTimestampedByteArray()),
            upper = Unbounded
        )
        assertTrue { iter5.numActiveIterators() == minNum }

        val iter6 = storage.scan(
            lower = Included("%05d".format(5000).toTimestampedByteArray()),
            upper = Excluded("%05d".format(6000).toTimestampedByteArray())
        )
        assertTrue { iter6.numActiveIterators() in minNum..<maxNum }
    }

    private fun sync(storage: LsmStorageInner) {
        storage.forceFreezeMemTableWithLock()
        storage.forceFlushNextImmMemTable()
    }

    private fun checkIterator(
        actual: StorageIterator,
        expected: List<Pair<TimestampedByteArray, TimestampedByteArray>>
    ) {
        for (e in expected) {
            assertTrue { actual.isValid() }
            assertEquals(actual.key(), e.first)
            assertEquals(actual.value(), e.second)
            actual.next()
        }
    }
}
