package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.BoundFlag
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.LsmStorageInner
import org.github.seonwkim.lsm.iterator.StorageIterator
import org.github.seonwkim.util.lsmStorageOptionForTest
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
            options = lsmStorageOptionForTest()
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
            storage.scan(Bound.unbounded(), Bound.unbounded()),
            listOf(
                Pair("0".toComparableByteArray(), "2333333".toComparableByteArray()),
                Pair("00".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("3".toComparableByteArray(), "23333".toComparableByteArray()),
            )
        )

        checkIterator(
            storage.scan(
                Bound("1".toComparableByteArray(), BoundFlag.INCLUDED),
                Bound("2".toComparableByteArray(), BoundFlag.INCLUDED)
            ),
            listOf(
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
            )
        )

        checkIterator(
            storage.scan(
                Bound("1".toComparableByteArray(), BoundFlag.EXCLUDED),
                Bound("3".toComparableByteArray(), BoundFlag.EXCLUDED)
            ),
            listOf(
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
            )
        )
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
