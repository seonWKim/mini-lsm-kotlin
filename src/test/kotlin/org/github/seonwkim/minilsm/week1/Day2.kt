package org.github.seonwkim.minilsm.week1

import org.junit.jupiter.api.assertThrows
import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.BoundFlag
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.storage.LsmStorageInner
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.iterator.util.lsmStorageOptionForTest
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Day2 {

    @Test
    fun `test task1 memTable iter`() {
        val memTable = MemTable.create(0)
        memTable.forTestingPutSlice("key1".toComparableByteArray(), "value1".toComparableByteArray())
        memTable.forTestingPutSlice("key2".toComparableByteArray(), "value2".toComparableByteArray())
        memTable.forTestingPutSlice("key3".toComparableByteArray(), "value3".toComparableByteArray())

        memTable.forTestingScanSlice(Bound.unbounded(), Bound.unbounded()).run {
            assertTrue(isValid())
            assertEquals("key1".toComparableByteArray(), key())
            assertEquals("value1".toComparableByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key2".toComparableByteArray(), key())
            assertEquals("value2".toComparableByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key3".toComparableByteArray(), key())
            assertEquals("value3".toComparableByteArray(), value())
            next()

            assertFalse(isValid())
        }

        memTable.forTestingScanSlice(
            Bound("key1".toComparableByteArray(), BoundFlag.INCLUDED),
            Bound("key2".toComparableByteArray(), BoundFlag.INCLUDED)
        ).run {
            assertTrue(isValid())
            assertEquals("key1".toComparableByteArray(), key())
            assertEquals("value1".toComparableByteArray(), value())
            next()

            assertTrue(isValid())
            assertEquals("key2".toComparableByteArray(), key())
            assertEquals("value2".toComparableByteArray(), value())
            next()

            assertFalse(isValid())
        }

        memTable.forTestingScanSlice(
            Bound("key1".toComparableByteArray(), BoundFlag.EXCLUDED),
            Bound("key3".toComparableByteArray(), BoundFlag.EXCLUDED)
        ).run {
            assertTrue(isValid())
            assertEquals("key2".toComparableByteArray(), key())
            assertEquals("value2".toComparableByteArray(), value())
            next()

            assertFalse(isValid())
        }
    }

    @Test
    fun `test task1 empty memtable iter`() {
        val memtable = MemTable.create(0)

        memtable.forTestingScanSlice(
            Bound("key1".toComparableByteArray(), BoundFlag.EXCLUDED),
            Bound("key3".toComparableByteArray(), BoundFlag.EXCLUDED)
        ).run {
            assertFalse { isValid() }
        }

        memtable.forTestingScanSlice(
            Bound("key1".toComparableByteArray(), BoundFlag.INCLUDED),
            Bound("key2".toComparableByteArray(), BoundFlag.INCLUDED)
        ).run {
            assertFalse { isValid() }
        }

        memtable.forTestingScanSlice(
            Bound.unbounded(),
            Bound.unbounded()
        ).run {
            assertFalse { isValid() }
        }
    }

    @Test
    fun `test task2 merge 1`() {
        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.1".toComparableByteArray()),
                MockIteratorData("e".toComparableByteArray(), "".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                MockIteratorData("b".toComparableByteArray(), "2.3".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.3".toComparableByteArray()),
                MockIteratorData("d".toComparableByteArray(), "4.3".toComparableByteArray()),
            )
        )

        val mergeIter = MergeIterator(listOf(iter1, iter2, iter3))
        assertIterator(
            mergeIter,
            listOf(
                Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.1".toComparableByteArray()),
                Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
                Pair("e".toComparableByteArray(), "".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task2 merge 2`() {
        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("d".toComparableByteArray(), "1.2".toComparableByteArray()),
                MockIteratorData("e".toComparableByteArray(), "2.2".toComparableByteArray()),
                MockIteratorData("f".toComparableByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("g".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                MockIteratorData("h".toComparableByteArray(), "1.3".toComparableByteArray()),
                MockIteratorData("i".toComparableByteArray(), "2.3".toComparableByteArray()),
                MockIteratorData("j".toComparableByteArray(), "3.3".toComparableByteArray()),
                MockIteratorData("k".toComparableByteArray(), "4.3".toComparableByteArray()),
            )
        )
        val iter4 = MockIterator(listOf())
        val result = listOf(
            Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
            Pair("b".toComparableByteArray(), "2.1".toComparableByteArray()),
            Pair("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            Pair("d".toComparableByteArray(), "1.2".toComparableByteArray()),
            Pair("e".toComparableByteArray(), "2.2".toComparableByteArray()),
            Pair("f".toComparableByteArray(), "3.2".toComparableByteArray()),
            Pair("g".toComparableByteArray(), "4.2".toComparableByteArray()),
            Pair("h".toComparableByteArray(), "1.3".toComparableByteArray()),
            Pair("i".toComparableByteArray(), "2.3".toComparableByteArray()),
            Pair("j".toComparableByteArray(), "3.3".toComparableByteArray()),
            Pair("k".toComparableByteArray(), "4.3".toComparableByteArray()),
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
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(listOf())
        val mergeIterator = MergeIterator(listOf(iter1, iter2))
        assertIterator(
            mergeIterator, listOf(
                Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task2 merge error`() {
        val iter = MergeIterator(listOf(MockIterator(emptyList())))
        assertIterator(iter, emptyList())

        val iter1 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.1".toComparableByteArray()),
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
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
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
        val dir = createTempDirectory("test_task4_integration")
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        storage.state.withWriteLock { storage.forceFreezeMemTable(it) }

        storage.delete("1".toComparableByteArray())
        storage.delete("2".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("4".toComparableByteArray(), "23333".toComparableByteArray())
        storage.state.withWriteLock { storage.forceFreezeMemTable(it) }

        storage.put("1".toComparableByteArray(), "233333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "233333".toComparableByteArray())

        storage.scan(Bound.unbounded(), Bound.unbounded()).let { iter ->
            assertIterator(
                iter = iter,
                values = listOf(
                    Pair("1".toComparableByteArray(), "233333".toComparableByteArray()),
                    Pair("3".toComparableByteArray(), "233333".toComparableByteArray()),
                    Pair("4".toComparableByteArray(), "23333".toComparableByteArray()),
                )
            )
            assertFalse { iter.isValid() }
            iter.next()
            iter.next()
            iter.next()
            assertFalse { iter.isValid() }
        }

        storage.scan(
            Bound("2".toComparableByteArray(), BoundFlag.INCLUDED),
            Bound("3".toComparableByteArray(), BoundFlag.INCLUDED)
        ).let { iter ->
            assertIterator(
                iter = iter,
                listOf(Pair("3".toComparableByteArray(), "233333".toComparableByteArray()))
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
        values: List<Pair<ComparableByteArray, ComparableByteArray>>
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
            while (iter.isValid()) {
                iter.next()
            }
        }
    }
}
