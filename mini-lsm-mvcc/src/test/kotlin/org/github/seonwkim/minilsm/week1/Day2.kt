package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.LsmStorageInner
import org.github.seonwkim.lsm.LsmStorageOptions
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemTable
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
        memTable.forTestingPutSlice("key1".toTimestampedByteArray(), "value1".toComparableByteArray())
        memTable.forTestingPutSlice("key2".toTimestampedByteArray(), "value2".toComparableByteArray())
        memTable.forTestingPutSlice("key3".toTimestampedByteArray(), "value3".toComparableByteArray())

        memTable.forTestingScanSlice(Unbounded, Unbounded).run {
            assertTrue(isValid())
            assertEquals("key1".toTimestampedByteArray(), key())
            assertEquals("value1".toComparableByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key2".toTimestampedByteArray(), key())
            assertEquals("value2".toComparableByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key3".toTimestampedByteArray(), key())
            assertEquals("value3".toComparableByteArray(), value())
            next()

            assertFalse(isValid())
        }

        memTable.forTestingScanSlice(
            Included("key1".toTimestampedByteArray()),
            Included("key2".toTimestampedByteArray())
        ).run {
            assertTrue(isValid())
            assertEquals("key1".toTimestampedByteArray(), key())
            assertEquals("value1".toComparableByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key2".toTimestampedByteArray(), key())
            assertEquals("value2".toComparableByteArray(), value())
            next()

            assertFalse(isValid())
        }

        memTable.forTestingScanSlice(
            Excluded("key1".toTimestampedByteArray()),
            Excluded("key3".toTimestampedByteArray())
        ).run {
            assertTrue(isValid())
            assertEquals("key2".toTimestampedByteArray(), key())
            assertEquals("value2".toComparableByteArray(), value())
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
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toComparableByteArray()),
                MockIteratorData("e".toTimestampedByteArray(), "".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toTimestampedByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                MockIteratorData("b".toTimestampedByteArray(), "2.3".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.3".toComparableByteArray()),
                MockIteratorData("d".toTimestampedByteArray(), "4.3".toComparableByteArray()),
            )
        )

        val mergeIter = MergeIterator(listOf(iter1, iter2, iter3))
        assertIterator(
            mergeIter,
            listOf(
                Pair("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toTimestampedByteArray(), "2.1".toComparableByteArray()),
                Pair("c".toTimestampedByteArray(), "3.1".toComparableByteArray()),
                Pair("d".toTimestampedByteArray(), "4.2".toComparableByteArray()),
                Pair("e".toTimestampedByteArray(), "".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task2 merge 2`() {
        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("d".toTimestampedByteArray(), "1.2".toComparableByteArray()),
                MockIteratorData("e".toTimestampedByteArray(), "2.2".toComparableByteArray()),
                MockIteratorData("f".toTimestampedByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("g".toTimestampedByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                MockIteratorData("h".toTimestampedByteArray(), "1.3".toComparableByteArray()),
                MockIteratorData("i".toTimestampedByteArray(), "2.3".toComparableByteArray()),
                MockIteratorData("j".toTimestampedByteArray(), "3.3".toComparableByteArray()),
                MockIteratorData("k".toTimestampedByteArray(), "4.3".toComparableByteArray()),
            )
        )
        val iter4 = MockIterator(listOf())
        val result = listOf(
            Pair("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
            Pair("b".toTimestampedByteArray(), "2.1".toComparableByteArray()),
            Pair("c".toTimestampedByteArray(), "3.1".toComparableByteArray()),
            Pair("d".toTimestampedByteArray(), "1.2".toComparableByteArray()),
            Pair("e".toTimestampedByteArray(), "2.2".toComparableByteArray()),
            Pair("f".toTimestampedByteArray(), "3.2".toComparableByteArray()),
            Pair("g".toTimestampedByteArray(), "4.2".toComparableByteArray()),
            Pair("h".toTimestampedByteArray(), "1.3".toComparableByteArray()),
            Pair("i".toTimestampedByteArray(), "2.3".toComparableByteArray()),
            Pair("j".toTimestampedByteArray(), "3.3".toComparableByteArray()),
            Pair("k".toTimestampedByteArray(), "4.3".toComparableByteArray()),
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
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(listOf())
        val mergeIterator = MergeIterator(listOf(iter1, iter2))
        assertIterator(
            mergeIterator, listOf(
                Pair("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toTimestampedByteArray(), "2.1".toComparableByteArray()),
                Pair("c".toTimestampedByteArray(), "3.1".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task2 merge error`() {
        val iter = MergeIterator(listOf(MockIterator(emptyList())))
        assertIterator(iter, emptyList())

        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArray(), "3.1".toComparableByteArray()),
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
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("a".toTimestampedByteArray(), "1.1".toComparableByteArray()),
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
        storage.put("1".toTimestampedByteArray(), "233".toComparableByteArray())
        storage.put("2".toTimestampedByteArray(), "2333".toComparableByteArray())
        storage.put("3".toTimestampedByteArray(), "23333".toComparableByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.delete("1".toTimestampedByteArray())
        storage.delete("2".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "2333".toComparableByteArray())
        storage.put("4".toTimestampedByteArray(), "23333".toComparableByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.put("1".toTimestampedByteArray(), "233333".toComparableByteArray())
        storage.put("3".toTimestampedByteArray(), "233333".toComparableByteArray())

        storage.scan(Unbounded, Unbounded).let { iter ->
            assertIterator(
                iter = iter,
                values = listOf(
                    Pair("1".toTimestampedByteArray(), "233333".toComparableByteArray()),
                    Pair("3".toTimestampedByteArray(), "233333".toComparableByteArray()),
                    Pair("4".toTimestampedByteArray(), "23333".toComparableByteArray()),
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
                listOf(Pair("3".toTimestampedByteArray(), "233333".toComparableByteArray()))
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
        values: List<Pair<TimestampedByteArray, ComparableByteArray>>
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
