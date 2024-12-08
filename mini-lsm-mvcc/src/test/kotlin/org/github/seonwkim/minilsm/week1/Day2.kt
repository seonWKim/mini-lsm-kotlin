package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.LsmStorageInner
import org.github.seonwkim.lsm.LsmStorageOptions
import org.junit.jupiter.api.assertThrows
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Day2 {

    @Test
    fun `test task1 memTable iter`() {
        val memTable = MemTable.create(0)
        memTable.forTestingPutSlice("key1".toTimestampedByteArray(), "value1".toTimestampedByteArray())
        memTable.forTestingPutSlice("key2".toTimestampedByteArray(), "value2".toTimestampedByteArray())
        memTable.forTestingPutSlice("key3".toTimestampedByteArray(), "value3".toTimestampedByteArray())

        memTable.forTestingScanSlice(Unbounded, Unbounded).run {
            assertTrue(isValid())
            assertEquals("key1".toTimestampedByteArray(), key())
            assertEquals("value1".toTimestampedByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key2".toTimestampedByteArray(), key())
            assertEquals("value2".toTimestampedByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key3".toTimestampedByteArray(), key())
            assertEquals("value3".toTimestampedByteArray(), value())
            next()

            assertFalse(isValid())
        }

        memTable.forTestingScanSlice(
            Included("key1".toTimestampedByteArray()),
            Included("key2".toTimestampedByteArray())
        ).run {
            assertTrue(isValid())
            assertEquals("key1".toTimestampedByteArray(), key())
            assertEquals("value1".toTimestampedByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key2".toTimestampedByteArray(), key())
            assertEquals("value2".toTimestampedByteArray(), value())
            next()

            assertFalse(isValid())
        }

        memTable.forTestingScanSlice(
            Excluded("key1".toTimestampedByteArray()),
            Excluded("key3".toTimestampedByteArray())
        ).run {
            assertTrue(isValid())
            assertEquals("key2".toTimestampedByteArray(), key())
            assertEquals("value2".toTimestampedByteArray(), value())
            next()

            assertFalse(isValid())
        }
    }

    @Test
    fun `test task1 empty memtable iter`() {
        val memtable = MemTable.create(0)

        memtable.forTestingScanSlice(
            Excluded("key1".toTimestampedByteArray()),
            Excluded("key3".toTimestampedByteArray())
        ).run {
            assertFalse { isValid() }
        }

        memtable.forTestingScanSlice(
            Included("key1".toTimestampedByteArray()),
            Included("key2".toTimestampedByteArray())
        ).run {
            assertFalse { isValid() }
        }

        memtable.forTestingScanSlice(
            Unbounded,
            Unbounded
        ).run {
            assertFalse { isValid() }
        }
    }

    @Test
    fun `test task2 merge 1`() {
        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toTimestampedByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toTimestampedByteArray()),
                MockIteratorData("e".toTimestampedByteArray(), "".toTimestampedByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.2".toTimestampedByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.2".toTimestampedByteArray()),
                MockIteratorData("d".toTimestampedByteArray(), "4.2".toTimestampedByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                MockIteratorData("b".toTimestampedByteArray(), "2.3".toTimestampedByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.3".toTimestampedByteArray()),
                MockIteratorData("d".toTimestampedByteArray(), "4.3".toTimestampedByteArray()),
            )
        )

        val mergeIter = MergeIterator(listOf(iter1, iter2, iter3))
        assertIterator(
            mergeIter,
            listOf(
                Pair("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
                Pair("b".toTimestampedByteArray(), "2.1".toTimestampedByteArray()),
                Pair("c".toTimestampedByteArray(), "3.1".toTimestampedByteArray()),
                Pair("d".toTimestampedByteArray(), "4.2".toTimestampedByteArray()),
                Pair("e".toTimestampedByteArray(), "".toTimestampedByteArray()),
            )
        )
    }

    @Test
    fun `test task2 merge 2`() {
        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toTimestampedByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toTimestampedByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("d".toTimestampedByteArray(), "1.2".toTimestampedByteArray()),
                MockIteratorData("e".toTimestampedByteArray(), "2.2".toTimestampedByteArray()),
                MockIteratorData("f".toTimestampedByteArray(), "3.2".toTimestampedByteArray()),
                MockIteratorData("g".toTimestampedByteArray(), "4.2".toTimestampedByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                MockIteratorData("h".toTimestampedByteArray(), "1.3".toTimestampedByteArray()),
                MockIteratorData("i".toTimestampedByteArray(), "2.3".toTimestampedByteArray()),
                MockIteratorData("j".toTimestampedByteArray(), "3.3".toTimestampedByteArray()),
                MockIteratorData("k".toTimestampedByteArray(), "4.3".toTimestampedByteArray()),
            )
        )
        val iter4 = MockIterator(listOf())
        val result = listOf(
            Pair("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
            Pair("b".toTimestampedByteArray(), "2.1".toTimestampedByteArray()),
            Pair("c".toTimestampedByteArray(), "3.1".toTimestampedByteArray()),
            Pair("d".toTimestampedByteArray(), "1.2".toTimestampedByteArray()),
            Pair("e".toTimestampedByteArray(), "2.2".toTimestampedByteArray()),
            Pair("f".toTimestampedByteArray(), "3.2".toTimestampedByteArray()),
            Pair("g".toTimestampedByteArray(), "4.2".toTimestampedByteArray()),
            Pair("h".toTimestampedByteArray(), "1.3".toTimestampedByteArray()),
            Pair("i".toTimestampedByteArray(), "2.3".toTimestampedByteArray()),
            Pair("j".toTimestampedByteArray(), "3.3".toTimestampedByteArray()),
            Pair("k".toTimestampedByteArray(), "4.3".toTimestampedByteArray()),
        )

        val mergedIter1 = MergeIterator(listOf(iter1.copy(), iter2.copy(), iter3.copy(), iter4.copy()))
        assertIterator(mergedIter1, result)

        val mergedIter2 = MergeIterator(listOf(iter2.copy(), iter4.copy(), iter3.copy(), iter1.copy()))
        assertIterator(mergedIter2, result)

        val mergedIter3 = MergeIterator(listOf(iter4.copy(), iter3.copy(), iter2.copy(), iter1.copy()))
        assertIterator(mergedIter3, result)
    }

    @Test
    fun `test task2 merge empty`() {
        val iter = MergeIterator(listOf(MockIterator(emptyList())))
        assertIterator(iter, emptyList())

        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toTimestampedByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toTimestampedByteArray()),
            )
        )
        val iter2 = MockIterator(listOf())
        val mergeIterator = MergeIterator(listOf(iter1, iter2))
        assertIterator(
            mergeIterator, listOf(
                Pair("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
                Pair("b".toTimestampedByteArray(), "2.1".toTimestampedByteArray()),
                Pair("c".toTimestampedByteArray(), "3.1".toTimestampedByteArray()),
            )
        )
    }

    @Test
    fun `test task2 merge error`() {
        val iter = MergeIterator(listOf(MockIterator(emptyList())))
        assertIterator(iter, emptyList())

        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toTimestampedByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toTimestampedByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toTimestampedByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toTimestampedByteArray()),
            ),
            throwErrorOnIdx = 1
        )
        val mergeIterator = MergeIterator(listOf(iter1.copy(), iter1, iter2))
        assertIteratorThrows<IllegalStateException>(mergeIterator)
    }

    @Test
    fun `test task3 fused iterator`() {
        val iter1 = MockIterator(emptyList())
        val fusedIter1 = FusedIterator(iter1)
        assertFalse { fusedIter1.isValid() }
        fusedIter1.next()
        fusedIter1.next()
        fusedIter1.next()
        assertFalse { fusedIter1.isValid() }

        val iter2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toTimestampedByteArray()),
            ),
            throwErrorOnIdx = 1
        )
        val fusedIter2 = FusedIterator(iter2)
        assertTrue { fusedIter2.isValid() }
        assertThrows<IllegalStateException> { fusedIter2.next() }
        assertFalse { fusedIter2.isValid() }
        assertThrows<Error> { fusedIter2.next() }
        assertThrows<Error> { fusedIter2.next() }
    }

    @Test
    fun `test task4 integration`() {
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        storage.put("1".toTimestampedByteArray(), "233".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.delete("1".toTimestampedByteArray())
        storage.delete("2".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.put("4".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.put("1".toTimestampedByteArray(), "233333".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "233333".toTimestampedByteArray())

        storage.scan(Unbounded, Unbounded).let { iter ->
            assertIterator(
                iter = iter,
                values = listOf(
                    Pair("1".toTimestampedByteArray(), "233333".toTimestampedByteArray()),
                    Pair("3".toTimestampedByteArray(), "233333".toTimestampedByteArray()),
                    Pair("4".toTimestampedByteArray(), "23333".toTimestampedByteArray()),
                )
            )
            assertFalse { iter.isValid() }
            iter.next()
            iter.next()
            iter.next()
            assertFalse { iter.isValid() }
        }

        storage.scan(
            Included("2".toTimestampedByteArray()),
            Included("3".toTimestampedByteArray())
        ).let { iter ->
            assertIterator(
                iter = iter,
                listOf(Pair("3".toTimestampedByteArray(), "233333".toTimestampedByteArray()))
            )
            assertFalse { iter.isValid() }
            iter.next()
            iter.next()
            iter.next()
            assertFalse { iter.isValid() }
        }
    }

    private fun assertIterator(
        iter: StorageIterator,
        values: List<Pair<TimestampedByteArray, TimestampedByteArray>>
    ) {
        for (value in values) {
            val (expectedKey, expectedValue) = value
            assertEquals(expectedKey, iter.key(), "$expectedKey != iter_key(${iter.key()})")
            assertEquals(expectedValue, iter.value(), "$expectedValue != iter_value(${iter.value()})")
            iter.next()
        }
    }

    private inline fun <reified T : Throwable> assertIteratorThrows(
        iter: StorageIterator
    ) {
        assertThrows<T> {
            while (true) {
                iter.next()
            }
        }
    }

    private fun lsmStorageOptionForTest(): LsmStorageOptions = LsmStorageOptions(
        blockSize = 4096,
        targetSstSize = 2 shl 20,
        numMemTableLimit = 10
    )
}
