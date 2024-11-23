package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class MemTableIteratorTest {

    @Test
    fun `test UNBOUNDED`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toComparableByteArray(), MemtableValue("a".toComparableByteArray()))
        memTable.put("b".toComparableByteArray(), MemtableValue("b".toComparableByteArray()))
        memTable.put("c".toComparableByteArray(), MemtableValue("c".toComparableByteArray()))

        val memTableIterator = MemTableIterator(
            memTable = memTable,
            lower = Bound.Unbounded,
            upper = Bound.Unbounded
        )

        assertEquals(memTableIterator.key(), "a".toComparableByteArray())
        memTableIterator.next()
        assertEquals(memTableIterator.key(), "b".toComparableByteArray())
        memTableIterator.next()
        assertEquals(memTableIterator.key(), "c".toComparableByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure lower and test 1`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toComparableByteArray(), MemtableValue("a".toComparableByteArray()))
        memTable.put("b".toComparableByteArray(), MemtableValue("b".toComparableByteArray()))
        memTable.put("c".toComparableByteArray(), MemtableValue("c".toComparableByteArray()))

        val memTableIterator = MemTableIterator(
            memTable = memTable,
            lower = Bound.Included("b".toComparableByteArray()),
            upper = Bound.Unbounded
        )

        assertEquals(memTableIterator.key(), "b".toComparableByteArray())
        memTableIterator.next()
        assertEquals(memTableIterator.key(), "c".toComparableByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure lower and test 2`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toComparableByteArray(), MemtableValue("a".toComparableByteArray()))
        memTable.put("b".toComparableByteArray(), MemtableValue("b".toComparableByteArray()))
        memTable.put("c".toComparableByteArray(), MemtableValue("c".toComparableByteArray()))

        val memTableIterator = MemTableIterator(
            memTable = memTable,
            lower = Bound.Excluded("b".toComparableByteArray()),
            upper = Bound.Unbounded
        )

        assertEquals(memTableIterator.key(), "c".toComparableByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure lower and test 3`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toComparableByteArray(), MemtableValue("a".toComparableByteArray()))
        memTable.put("b".toComparableByteArray(), MemtableValue("b".toComparableByteArray()))
        memTable.put("c".toComparableByteArray(), MemtableValue("c".toComparableByteArray()))

        val memTableIterator = MemTableIterator(
            memTable = memTable,
            lower = Bound.Excluded("d".toComparableByteArray()),
            upper = Bound.Unbounded
        )

        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure upper and test 1`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toComparableByteArray(), MemtableValue("a".toComparableByteArray()))
        memTable.put("b".toComparableByteArray(), MemtableValue("b".toComparableByteArray()))
        memTable.put("c".toComparableByteArray(), MemtableValue("c".toComparableByteArray()))

        val memTableIterator = MemTableIterator(
            memTable = memTable,
            lower = Bound.Unbounded,
            upper = Bound.Included("b".toComparableByteArray())
        )

        assertEquals(memTableIterator.key(), "a".toComparableByteArray())
        memTableIterator.next()
        assertEquals(memTableIterator.key(), "b".toComparableByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }

    @Test
    fun `configure upper and test 2`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toComparableByteArray(), MemtableValue("a".toComparableByteArray()))
        memTable.put("b".toComparableByteArray(), MemtableValue("b".toComparableByteArray()))
        memTable.put("c".toComparableByteArray(), MemtableValue("c".toComparableByteArray()))

        val memTableIterator = MemTableIterator(
            memTable = memTable,
            lower = Bound.Unbounded,
            upper = Bound.Excluded("b".toComparableByteArray())
        )

        assertEquals(memTableIterator.key(), "a".toComparableByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }
}
