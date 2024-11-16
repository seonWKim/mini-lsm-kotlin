package week1

import org.example.common.Bound
import org.example.common.BoundFlag
import org.example.common.ComparableByteArray
import org.example.common.toComparableByteArray
import org.example.lsm.memtable.MemTable
import org.example.lsm.memtable.iterator.MergeIterator
import org.example.lsm.memtable.iterator.MockIterator
import org.example.lsm.memtable.iterator.StorageIterator
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
                Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.1".toComparableByteArray()),
                Pair("e".toComparableByteArray(), "".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                Pair("b".toComparableByteArray(), "2.3".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.3".toComparableByteArray()),
                Pair("d".toComparableByteArray(), "4.3".toComparableByteArray()),
            )
        )

        val iter = MergeIterator(listOf(iter1, iter2, iter3))
        assertIterator(
            iter,
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
                Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(
            listOf(
                Pair("d".toComparableByteArray(), "1.2".toComparableByteArray()),
                Pair("e".toComparableByteArray(), "2.2".toComparableByteArray()),
                Pair("f".toComparableByteArray(), "3.2".toComparableByteArray()),
                Pair("g".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter3 = MockIterator(
            listOf(
                Pair("h".toComparableByteArray(), "1.3".toComparableByteArray()),
                Pair("i".toComparableByteArray(), "2.3".toComparableByteArray()),
                Pair("j".toComparableByteArray(), "3.3".toComparableByteArray()),
                Pair("k".toComparableByteArray(), "4.3".toComparableByteArray()),
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
                Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val iter2 = MockIterator(listOf())
        val mergeIterator = MergeIterator(listOf(iter1, iter2))
        assertIterator(mergeIterator, listOf(
            Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
            Pair("b".toComparableByteArray(), "2.1".toComparableByteArray()),
            Pair("c".toComparableByteArray(), "3.1".toComparableByteArray()),
        ))
    }

    private fun assertIterator(
        iter: StorageIterator,
        values: List<Pair<ComparableByteArray, ComparableByteArray>>
    ) {
        var idx = 0
        while (iter.isValid()) {
            val (expectedKey, expectedValue) = values[idx]
            assertEquals(expectedKey, iter.key(), "$expectedKey != iter_key(${iter.key()})")
            assertEquals(expectedValue, iter.value(), "$expectedValue != iter_value(${iter.value()})")
            idx++
            iter.next()
        }
    }
}
