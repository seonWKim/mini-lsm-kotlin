package org.seonwkim.week1

import org.seonwkim.common.Bound
import org.seonwkim.common.BoundFlag
import org.seonwkim.common.ComparableByteArray
import org.seonwkim.common.toComparableByteArray
import org.seonwkim.lsm.LsmStorageInner
import org.seonwkim.lsm.LsmStorageOptions
import org.seonwkim.lsm.iterator.MockIterator
import org.seonwkim.lsm.iterator.MockIteratorData
import org.seonwkim.lsm.iterator.StorageIterator
import org.seonwkim.lsm.iterator.TwoMergeIterator
import org.seonwkim.util.generateSst
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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
        checkIterator(
            iter, listOf(
                Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toComparableByteArray(), "2.1".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.1".toComparableByteArray()),
                Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
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
        checkIterator(
            iter, listOf(
                Pair("a".toComparableByteArray(), "1.2".toComparableByteArray()),
                Pair("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
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
        checkIterator(
            iter, listOf(
                Pair("a".toComparableByteArray(), "1.1".toComparableByteArray()),
                Pair("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
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
        checkIterator(
            iter1, listOf(
                Pair("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )

        val i3 = MockIterator(emptyList())
        val i4 = MockIterator(
            listOf(
                MockIteratorData("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
        val iter2 = TwoMergeIterator.create(i3, i4)
        checkIterator(
            iter2, listOf(
                Pair("b".toComparableByteArray(), "2.2".toComparableByteArray()),
                Pair("c".toComparableByteArray(), "3.2".toComparableByteArray()),
                Pair("d".toComparableByteArray(), "4.2".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task1 merge 5`() {
        val i2 = MockIterator(emptyList())
        val i1 = MockIterator(emptyList())
        val iter = TwoMergeIterator.create(i1, i2)
        checkIterator(iter, emptyList())
    }

    @Test
    fun `test task2 storage scan`() {
        val dir = createTempDirectory("test_task2_storage_scan")
        val storage = LsmStorageInner.open(dir, LsmStorageOptions.defaultForWeek1Test())
        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("00".toComparableByteArray(), "2333".toComparableByteArray())
        storage.forceFreezeMemtable()
        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        storage.delete("1".toComparableByteArray())
        val sst1 = generateSst(
            id = 10,
            path = dir.resolve("10.sst"),
            data = listOf(
                Pair("0".toComparableByteArray(), "2333333".toComparableByteArray()),
                Pair("00".toComparableByteArray(), "2333333".toComparableByteArray()),
                Pair("4".toComparableByteArray(), "23".toComparableByteArray()),
            ),
            blockCache = storage.blockCache.copy()
        )

        val sst2 = generateSst(
            id = 11,
            path = dir.resolve("11.sst"),
            data = listOf(
                Pair("4".toComparableByteArray(), "".toComparableByteArray())
            ),
            blockCache = storage.blockCache.copy()
        )

        val snapshot = storage.state
        snapshot.l0SsTables.add(sst2.id)
        snapshot.l0SsTables.add(sst1.id)
        snapshot.ssTables[sst2.id] = sst2
        snapshot.ssTables[sst1.id] = sst1

        checkIterator(
            storage.scan(Bound.unbounded(), Bound.unbounded()),
            listOf(
                Pair("0".toComparableByteArray(), "2333333".toComparableByteArray()),
                Pair("00".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("3".toComparableByteArray(), "23333".toComparableByteArray()),
                Pair("4".toComparableByteArray(), "".toComparableByteArray()), // because we push sst2 first
            )
        )

        checkIterator(
            storage.scan(
                Bound("1".toComparableByteArray(), BoundFlag.INCLUDED),
                Bound("2".toComparableByteArray(), BoundFlag.INCLUDED)
            ),
            listOf(
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray())
            )
        )

        checkIterator(
            storage.scan(
                Bound("1".toComparableByteArray(), BoundFlag.INCLUDED),
                Bound("3".toComparableByteArray(), BoundFlag.INCLUDED)
            ),
            listOf(
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray())
            )
        )

        checkIterator(
            storage.scan(
                Bound("0".toComparableByteArray(), BoundFlag.INCLUDED),
                Bound("1".toComparableByteArray(), BoundFlag.INCLUDED)
            ),
            listOf(
                Pair("0".toComparableByteArray(), "2333333".toComparableByteArray()),
                Pair("00".toComparableByteArray(), "2333".toComparableByteArray())
            )
        )

        checkIterator(
            storage.scan(
                Bound("0".toComparableByteArray(), BoundFlag.EXCLUDED),
                Bound("1".toComparableByteArray(), BoundFlag.INCLUDED)
            ),
            listOf(
                Pair("00".toComparableByteArray(), "2333".toComparableByteArray())
            )
        )
    }

    @Test
    fun `test task3 storage get`() {

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
