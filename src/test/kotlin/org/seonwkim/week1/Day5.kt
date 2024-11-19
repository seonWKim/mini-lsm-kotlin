package org.seonwkim.week1

import org.seonwkim.common.ComparableByteArray
import org.seonwkim.common.toComparableByteArray
import org.seonwkim.lsm.iterator.MockIterator
import org.seonwkim.lsm.iterator.MockIteratorData
import org.seonwkim.lsm.iterator.StorageIterator
import org.seonwkim.lsm.iterator.TwoMergeIterator
import kotlin.test.Test
import kotlin.test.assertEquals

class Day5 {

    @Test
    fun `test task1 merge 1`() {
        val i1 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val i2 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter = TwoMergeIterator.create(i1, i2)
        checkIterator(iter, listOf(
            Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
            Pair("b".toComparableByteArray(), "2.1".toComparableByteArray()),
            Pair("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
        ))
    }

    @Test
    fun `test task1 merge 2`() {
        val i2 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val i1 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.2".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter = TwoMergeIterator.create(i1, i2)
        checkIterator(iter, listOf(
            Pair("a".toComparableByteArray(), "1.2".toComparableByteArray()),
            Pair("b".toComparableByteArray(), "2.2".toComparableByteArray()),
            Pair("c".toComparableByteArray(), "3.2".toComparableByteArray()),
            Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
        ))
    }

    @Test
    fun `test task1 merge 3`() {
        val i2 = MockIterator(
            listOf(
                MockIteratorData("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.1".toComparableByteArray()),
            )
        )
        val i1 = MockIterator(
            listOf(
                MockIteratorData("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter = TwoMergeIterator.create(i1, i2)
        checkIterator(iter, listOf(
            Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
            Pair("b".toComparableByteArray(), "2.2".toComparableByteArray()),
            Pair("c".toComparableByteArray(), "3.2".toComparableByteArray()),
            Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
        ))
    }

    @Test
    fun `test task1 merge 4`() {
        val i2 = MockIterator(emptyList())
        val i1 = MockIterator(
            listOf(
                MockIteratorData("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter1 = TwoMergeIterator.create(i1, i2)
        checkIterator(iter1, listOf(
            Pair("b".toComparableByteArray(), "2.2".toComparableByteArray()),
            Pair("c".toComparableByteArray(), "3.2".toComparableByteArray()),
            Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
        ))

        val i3 = MockIterator(emptyList())
        val i4 = MockIterator(
            listOf(
                MockIteratorData("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter2 = TwoMergeIterator.create(i3, i4)
        checkIterator(iter2, listOf(
            Pair("b".toComparableByteArray(), "2.2".toComparableByteArray()),
            Pair("c".toComparableByteArray(), "3.2".toComparableByteArray()),
            Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
        ))
    }

    @Test
    fun `test task1 merge 5`() {
        val i2 = MockIterator(emptyList())
        val i1 = MockIterator(emptyList())
        val iter = TwoMergeIterator.create(i1, i2)
        checkIterator(iter, emptyList())
    }

    private fun checkIterator(
        actual: StorageIterator,
        expected: List<Pair<ComparableByteArray, ComparableByteArray>>
    ) {
        var i = 0
        while (actual.isValid()) {
            assertEquals(actual.key(), expected[i].first)
            assertEquals(actual.value(), expected[i].second)
            i++
            actual.next()
        }

        assertEquals(i, expected.size)
    }
}
