import org.example.common.Bound
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

        memTable.forTestingScanSlice(Bound.UNBOUNDED, Bound.UNBOUNDED).run {
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
    }
}
