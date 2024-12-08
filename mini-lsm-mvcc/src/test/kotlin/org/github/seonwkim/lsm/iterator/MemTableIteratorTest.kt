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
        memTable.put("a".toTimestampedByteArray(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArray(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArray(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Unbounded,
            upper = Unbounded
        )

        assertEquals(memTableIterator.key(), "a".toTimestampedByteArray())
        memTableIterator.next()
        assertEquals(memTableIterator.key(), "b".toTimestampedByteArray())
        memTableIterator.next()
        assertEquals(memTableIterator.key(), "c".toTimestampedByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure lower and test 1`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArray(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArray(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArray(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Included("b".toTimestampedByteArray()),
            upper = Unbounded
        )

        assertEquals(memTableIterator.key(), "b".toTimestampedByteArray())
        memTableIterator.next()
        assertEquals(memTableIterator.key(), "c".toTimestampedByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure lower and test 2`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArray(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArray(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArray(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Excluded("b".toTimestampedByteArray()),
            upper = Unbounded
        )

        assertEquals(memTableIterator.key(), "c".toTimestampedByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure lower and test 3`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArray(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArray(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArray(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Excluded("d".toTimestampedByteArray()),
            upper = Unbounded
        )

        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure upper and test 1`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArray(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArray(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArray(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Unbounded,
            upper = Included("b".toTimestampedByteArray())
        )

        assertEquals(memTableIterator.key(), "a".toTimestampedByteArray())
        memTableIterator.next()
        assertEquals(memTableIterator.key(), "b".toTimestampedByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure upper and test 2`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toTimestampedByteArray(), "a".toComparableByteArray())
        memTable.put("b".toTimestampedByteArray(), "b".toComparableByteArray())
        memTable.put("c".toTimestampedByteArray(), "c".toComparableByteArray())

        val memTableIterator = MemTableIterator.create(
            memTable = memTable,
            lower = Unbounded,
            upper = Excluded("b".toTimestampedByteArray())
        )

        assertEquals(memTableIterator.key(), "a".toTimestampedByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }
}
