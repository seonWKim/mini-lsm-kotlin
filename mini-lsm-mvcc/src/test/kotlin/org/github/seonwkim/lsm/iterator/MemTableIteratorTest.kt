package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class MemTableIteratorTest {

    @Test
    fun `test UNBOUNDED`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArrayWithoutTs(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArrayWithoutTs(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArrayWithoutTs(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Unbounded,
            upper = Unbounded
        )

        assertByteArrayEquals(memTableIterator.key(), "a".toTimestampedByteArrayWithoutTs())
        memTableIterator.next()
        assertByteArrayEquals(memTableIterator.key(), "b".toTimestampedByteArrayWithoutTs())
        memTableIterator.next()
        assertByteArrayEquals(memTableIterator.key(), "c".toTimestampedByteArrayWithoutTs())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure lower and test 1`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArrayWithoutTs(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArrayWithoutTs(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArrayWithoutTs(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Included("b".toTimestampedByteArrayWithoutTs()),
            upper = Unbounded
        )

        assertByteArrayEquals(memTableIterator.key(), "b".toTimestampedByteArrayWithoutTs())
        memTableIterator.next()
        assertByteArrayEquals(memTableIterator.key(), "c".toTimestampedByteArrayWithoutTs())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure lower and test 2`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArrayWithoutTs(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArrayWithoutTs(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArrayWithoutTs(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Excluded("b".toTimestampedByteArrayWithoutTs()),
            upper = Unbounded
        )

        assertByteArrayEquals(memTableIterator.key(), "c".toTimestampedByteArrayWithoutTs())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure lower and test 3`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArrayWithoutTs(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArrayWithoutTs(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArrayWithoutTs(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Excluded("d".toTimestampedByteArrayWithoutTs()),
            upper = Unbounded
        )

        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure upper and test 1`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArrayWithoutTs(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArrayWithoutTs(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArrayWithoutTs(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Unbounded,
            upper = Included("b".toTimestampedByteArrayWithoutTs())
        )

        assertByteArrayEquals(memTableIterator.key(), "a".toTimestampedByteArrayWithoutTs())
        memTableIterator.next()
        assertByteArrayEquals(memTableIterator.key(), "b".toTimestampedByteArrayWithoutTs())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure upper and test 2`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArrayWithoutTs(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArrayWithoutTs(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArrayWithoutTs(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Unbounded,
            upper = Excluded("b".toTimestampedByteArrayWithoutTs())
        )

        assertByteArrayEquals(memTableIterator.key(), "a".toTimestampedByteArrayWithoutTs())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }
}
