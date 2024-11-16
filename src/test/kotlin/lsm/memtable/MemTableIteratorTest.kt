package lsm.memtable

import org.example.common.Bound
import org.example.common.toComparableByteArray
import org.example.lsm.memtable.MemTable
import org.example.lsm.memtable.MemTableIterator
import org.example.lsm.memtable.MemtableValue
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
            lower = Bound.UNBOUNDED,
            upper = Bound.UNBOUNDED
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
    fun `configure lower and test`() {
        val memTable = MemTable.create(0)
        memTable.put("a".toComparableByteArray(), MemtableValue("a".toComparableByteArray()))
        memTable.put("b".toComparableByteArray(), MemtableValue("b".toComparableByteArray()))
        memTable.put("c".toComparableByteArray(), MemtableValue("c".toComparableByteArray()))

        val memTableIterator = MemTableIterator(
            memTable = memTable,
            lower = Bound("b".toComparableByteArray()),
            upper = Bound.UNBOUNDED
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
            lower = Bound("d".toComparableByteArray()),
            upper = Bound.UNBOUNDED
        )

        assertFalse { memTableIterator.isValid() }
    }
}
