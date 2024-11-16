import org.example.common.Bound
import org.example.common.BoundFlag
import org.example.common.toComparableByteArray
import org.example.lsm.memtable.MemTable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Week1Day2 {

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
}
