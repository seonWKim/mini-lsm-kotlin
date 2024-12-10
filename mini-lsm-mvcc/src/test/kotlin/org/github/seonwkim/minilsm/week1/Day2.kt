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
        memTable.forTestingPutSlice("key1".toTimestampedByteArrayWithoutTs(), "value1".toComparableByteArray())
        memTable.forTestingPutSlice("key2".toTimestampedByteArrayWithoutTs(), "value2".toComparableByteArray())
        memTable.forTestingPutSlice("key3".toTimestampedByteArrayWithoutTs(), "value3".toComparableByteArray())

        memTable.forTestingScanSlice(Unbounded, Unbounded).run {
            assertTrue(isValid())
            assertEquals("key1".toTimestampedByteArrayWithoutTs(), key())
            assertEquals("value1".toComparableByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key2".toTimestampedByteArrayWithoutTs(), key())
            assertEquals("value2".toComparableByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key3".toTimestampedByteArrayWithoutTs(), key())
            assertEquals("value3".toComparableByteArray(), value())
            next()

            assertFalse(isValid())
        }

        memTable.forTestingScanSlice(
            Included("key1".toTimestampedByteArrayWithoutTs()),
            Included("key2".toTimestampedByteArrayWithoutTs())
        ).run {
            assertTrue(isValid())
            assertEquals("key1".toTimestampedByteArrayWithoutTs(), key())
            assertEquals("value1".toComparableByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key2".toTimestampedByteArrayWithoutTs(), key())
            assertEquals("value2".toComparableByteArray(), value())
            next()

            assertFalse(isValid())
        }

        memTable.forTestingScanSlice(
            Excluded("key1".toTimestampedByteArrayWithoutTs()),
            Excluded("key3".toTimestampedByteArrayWithoutTs())
        ).run {
            assertTrue(isValid())
            assertEquals("key2".toTimestampedByteArrayWithoutTs(), key())
            assertEquals("value2".toComparableByteArray(), value())
            next()

            assertFalse(isValid())
        }
    }

    @Test
    fun `test task1 empty memtable iter`() {
        val memtable = MemTable.create(0)

        memtable.forTestingScanSlice(
            Excluded("key1".toTimestampedByteArrayWithoutTs()),
            Excluded("key3".toTimestampedByteArrayWithoutTs())
        ).run {
            assertFalse { isValid() }
        }

        memtable.forTestingScanSlice(
            Included("key1".toTimestampedByteArrayWithoutTs()),
            Included("key2".toTimestampedByteArrayWithoutTs())
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
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
                MockIteratorData("e".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.3".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.3".toComparableByteArray()),
                MockIteratorData("d".toTimestampedByteArrayWithoutTs(), "4.3".toComparableByteArray()),
            )
        )

        val mergeIter = MergeIterator(listOf(iter1, iter2, iter3))
        assertIterator(
            mergeIter,
            listOf(
                Pair("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                Pair("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                Pair("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
                Pair("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
                Pair("e".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task2 merge 2`() {
        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("d".toTimestampedByteArrayWithoutTs(), "1.2".toComparableByteArray()),
                MockIteratorData("e".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                MockIteratorData("f".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                MockIteratorData("g".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                MockIteratorData("h".toTimestampedByteArrayWithoutTs(), "1.3".toComparableByteArray()),
                MockIteratorData("i".toTimestampedByteArrayWithoutTs(), "2.3".toComparableByteArray()),
                MockIteratorData("j".toTimestampedByteArrayWithoutTs(), "3.3".toComparableByteArray()),
                MockIteratorData("k".toTimestampedByteArrayWithoutTs(), "4.3".toComparableByteArray()),
            )
        )
        val iter4 = MockIterator(listOf())
        val result = listOf(
            Pair("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
            Pair("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
            Pair("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
            Pair("d".toTimestampedByteArrayWithoutTs(), "1.2".toComparableByteArray()),
            Pair("e".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
            Pair("f".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
            Pair("g".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            Pair("h".toTimestampedByteArrayWithoutTs(), "1.3".toComparableByteArray()),
            Pair("i".toTimestampedByteArrayWithoutTs(), "2.3".toComparableByteArray()),
            Pair("j".toTimestampedByteArrayWithoutTs(), "3.3".toComparableByteArray()),
            Pair("k".toTimestampedByteArrayWithoutTs(), "4.3".toComparableByteArray()),
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
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(listOf())
        val mergeIterator = MergeIterator(listOf(iter1, iter2))
        assertIterator(
            mergeIterator, listOf(
                Pair("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                Pair("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                Pair("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task2 merge error`() {
        val iter = MergeIterator(listOf(MockIterator(emptyList())))
        assertIterator(iter, emptyList())

        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
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
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
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
        storage.put("1".toTimestampedByteArrayWithoutTs(), "233".toComparableByteArray())
        storage.put("2".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
        storage.put("3".toTimestampedByteArrayWithoutTs(), "23333".toComparableByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.delete("1".toTimestampedByteArrayWithoutTs())
        storage.delete("2".toTimestampedByteArrayWithoutTs())
        storage.put("3".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
        storage.put("4".toTimestampedByteArrayWithoutTs(), "23333".toComparableByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.put("1".toTimestampedByteArrayWithoutTs(), "233333".toComparableByteArray())
        storage.put("3".toTimestampedByteArrayWithoutTs(), "233333".toComparableByteArray())

        storage.scan(Unbounded, Unbounded).let { iter ->
            assertIterator(
                iter = iter,
                values = listOf(
                    Pair("1".toTimestampedByteArrayWithoutTs(), "233333".toComparableByteArray()),
                    Pair("3".toTimestampedByteArrayWithoutTs(), "233333".toComparableByteArray()),
                    Pair("4".toTimestampedByteArrayWithoutTs(), "23333".toComparableByteArray()),
                )
            )
            assertFalse { iter.isValid() }
            iter.next()
            iter.next()
            iter.next()
            assertFalse { iter.isValid() }
        }

        storage.scan(
            Included("2".toTimestampedByteArrayWithoutTs()),
            Included("3".toTimestampedByteArrayWithoutTs())
        ).let { iter ->
            assertIterator(
                iter = iter,
                listOf(Pair("3".toTimestampedByteArrayWithoutTs(), "233333".toComparableByteArray()))
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
            assertByteArrayEquals(expectedKey, iter.key(), "$expectedKey != iter_key(${iter.key()})")
            assertByteArrayEquals(expectedValue, iter.value(), "$expectedValue != iter_value(${iter.value()})")
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
