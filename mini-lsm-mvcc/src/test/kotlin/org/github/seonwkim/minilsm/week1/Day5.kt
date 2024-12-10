package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.LsmStorageInner
import org.github.seonwkim.lsm.LsmStorageOptions
import org.github.seonwkim.lsm.iterator.MockIterator
import org.github.seonwkim.lsm.iterator.MockIteratorData
import org.github.seonwkim.lsm.iterator.StorageIterator
import org.github.seonwkim.lsm.iterator.TwoMergeIterator
import org.github.seonwkim.lsm.iterator.util.generateSst
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertTrue

class Day5 {

    @Test
    fun `test task1 merge 1`() {
        val i1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
            )
        )
        val i2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
        val iter = TwoMergeIterator.create(i1, i2)
        checkIterator(
            iter, listOf(
                Pair("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                Pair("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                Pair("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
                Pair("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task1 merge 2`() {
        val i2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
            )
        )
        val i1 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.2".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
        val iter = TwoMergeIterator.create(i1, i2)
        checkIterator(
            iter, listOf(
                Pair("a".toTimestampedByteArrayWithoutTs(), "1.2".toComparableByteArray()),
                Pair("b".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                Pair("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                Pair("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task1 merge 3`() {
        val i2 = MockIterator(
            listOf(
                MockIteratorData("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.1".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.1".toComparableByteArray()),
            )
        )
        val i1 = MockIterator(
            listOf(
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
        val iter = TwoMergeIterator.create(i1, i2)
        checkIterator(
            iter, listOf(
                Pair("a".toTimestampedByteArrayWithoutTs(), "1.1".toComparableByteArray()),
                Pair("b".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                Pair("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                Pair("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
    }

    @Test
    fun `test task1 merge 4`() {
        val i2 = MockIterator(emptyList())
        val i1 = MockIterator(
            listOf(
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
        val iter1 = TwoMergeIterator.create(i1, i2)
        checkIterator(
            iter1, listOf(
                Pair("b".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                Pair("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                Pair("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )

        val i3 = MockIterator(emptyList())
        val i4 = MockIterator(
            listOf(
                MockIteratorData("b".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                MockIteratorData("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                MockIteratorData("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
            )
        )
        val iter2 = TwoMergeIterator.create(i3, i4)
        checkIterator(
            iter2, listOf(
                Pair("b".toTimestampedByteArrayWithoutTs(), "2.2".toComparableByteArray()),
                Pair("c".toTimestampedByteArrayWithoutTs(), "3.2".toComparableByteArray()),
                Pair("d".toTimestampedByteArrayWithoutTs(), "4.2".toComparableByteArray()),
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
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        storage.put("1".toTimestampedByteArrayWithoutTs(), "233".toComparableByteArray())
        storage.put("2".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
        storage.put("00".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
        storage.forceFreezeMemTableWithLock()
        storage.put("3".toTimestampedByteArrayWithoutTs(), "23333".toComparableByteArray())
        storage.delete("1".toTimestampedByteArrayWithoutTs())
        val sst1 = generateSst(
            id = 10,
            path = dir.resolve("10.sst"),
            data = listOf(
                Pair("0".toTimestampedByteArrayWithoutTs(), "2333333".toComparableByteArray()),
                Pair("00".toTimestampedByteArrayWithoutTs(), "2333333".toComparableByteArray()),
                Pair("4".toTimestampedByteArrayWithoutTs(), "23".toComparableByteArray()),
            ),
            blockCache = storage.blockCache.copy()
        )

        val sst2 = generateSst(
            id = 11,
            path = dir.resolve("11.sst"),
            data = listOf(
                Pair("4".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()) // This is treated as deleted
            ),
            blockCache = storage.blockCache.copy()
        )

        storage.addL0Sstable(sst1.id)
        storage.addL0Sstable(sst2.id)
        storage.addSstable(sst1)
        storage.addSstable(sst2)

        checkIterator(
            storage.scan(Unbounded, Unbounded),
            listOf(
                Pair("0".toTimestampedByteArrayWithoutTs(), "2333333".toComparableByteArray()),
                Pair("00".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray()),
                Pair("2".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray()),
                Pair("3".toTimestampedByteArrayWithoutTs(), "23333".toComparableByteArray()),
            )
        )

        checkIterator(
            storage.scan(
                Included("1".toTimestampedByteArrayWithoutTs()),
                Included("2".toTimestampedByteArrayWithoutTs())
            ),
            listOf(
                Pair("2".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
            )
        )

        checkIterator(
            storage.scan(
                Excluded("1".toTimestampedByteArrayWithoutTs()),
                Excluded("3".toTimestampedByteArrayWithoutTs())
            ),
            listOf(
                Pair("2".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
            )
        )

        checkIterator(
            storage.scan(
                Included("0".toTimestampedByteArrayWithoutTs()),
                Included("1".toTimestampedByteArrayWithoutTs())
            ),
            listOf(
                Pair("0".toTimestampedByteArrayWithoutTs(), "2333333".toComparableByteArray()),
                Pair("00".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
            )
        )

        checkIterator(
            storage.scan(
                Excluded("0".toTimestampedByteArrayWithoutTs()),
                Included("1".toTimestampedByteArrayWithoutTs())
            ),
            listOf(
                Pair("00".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
            )
        )
    }

    @Test
    fun `test task3 storage get`() {
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(
            path = dir,
            options = lsmStorageOptionForTest()
        )
        storage.put("1".toTimestampedByteArrayWithoutTs(), "233".toComparableByteArray())
        storage.put("2".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
        storage.put("00".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
        storage.forceFreezeMemTableWithLock()

        storage.put("3".toTimestampedByteArrayWithoutTs(), "23333".toComparableByteArray())
        storage.delete("1".toTimestampedByteArrayWithoutTs())
        val sst1 = generateSst(
            id = 10,
            path = dir.resolve("10.sst"),
            data = listOf(
                Pair("0".toTimestampedByteArrayWithoutTs(), "2333333".toComparableByteArray()),
                Pair("00".toTimestampedByteArrayWithoutTs(), "2333333".toComparableByteArray()),
                Pair("4".toTimestampedByteArrayWithoutTs(), "23".toComparableByteArray()),
            ),
            blockCache = storage.blockCache.copy()
        )
        val sst2 = generateSst(
            id = 11,
            path = dir.resolve("11.sst"),
            data = listOf(
                Pair("4".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
            ),
            blockCache = storage.blockCache.copy()
        )

        storage.addL0Sstable(sst1.id)
        storage.addL0Sstable(sst2.id)
        storage.addSstable(sst1)
        storage.addSstable(sst2)

        assertByteArrayEquals(storage.get("0".toTimestampedByteArrayWithoutTs()), "2333333".toComparableByteArray())
        assertByteArrayEquals(storage.get("00".toTimestampedByteArrayWithoutTs()), "2333".toComparableByteArray())
        assertByteArrayEquals(storage.get("2".toTimestampedByteArrayWithoutTs()), "2333".toComparableByteArray())
        assertByteArrayEquals(storage.get("3".toTimestampedByteArrayWithoutTs()), "23333".toComparableByteArray())
        assertByteArrayEquals(storage.get("4".toTimestampedByteArrayWithoutTs()), null)
        assertByteArrayEquals(storage.get("--".toTimestampedByteArrayWithoutTs()), null)
        assertByteArrayEquals(storage.get("555".toTimestampedByteArrayWithoutTs()), null)
    }

    private fun checkIterator(
        actual: StorageIterator,
        expected: List<Pair<TimestampedByteArray, ComparableByteArray>>
    ) {
        for (e in expected) {
            assertTrue { actual.isValid() }
            assertByteArrayEquals(actual.key(), e.first)
            assertByteArrayEquals(actual.value(), e.second)
            actual.next()
        }
        assertTrue { !actual.isValid() }
    }

    private fun lsmStorageOptionForTest(): LsmStorageOptions = LsmStorageOptions(
        blockSize = 4096,
        targetSstSize = 2 shl 20,
        numMemTableLimit = 10
    )
}
