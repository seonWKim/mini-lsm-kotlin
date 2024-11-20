package org.github.seonwkim.iterator

import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.BoundFlag
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.iterator.MemTableIterator
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
            lower = Bound.unbounded(),
            upper = Bound.unbounded()
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
            lower = Bound("b".toComparableByteArray(), BoundFlag.INCLUDED),
            upper = Bound.unbounded()
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
            lower = Bound("b".toComparableByteArray(), BoundFlag.EXCLUDED),
            upper = Bound.unbounded()
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
            lower = Bound("d".toComparableByteArray()),
            upper = Bound.unbounded()
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
            lower = Bound.unbounded(),
            upper = Bound("b".toComparableByteArray(), BoundFlag.INCLUDED)
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
            lower = Bound.unbounded(),
            upper = Bound("b".toComparableByteArray(), BoundFlag.EXCLUDED)
        )

        assertEquals(memTableIterator.key(), "a".toComparableByteArray())
        memTableIterator.next()
        assertFalse { memTableIterator.isValid() }
    }
}
