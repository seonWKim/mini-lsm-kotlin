package org.github.seonwkim.lsm.memtable

import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.common.toTimestampedByteArray
import kotlin.test.Test
import kotlin.test.assertEquals

class MemTableTest {
    @Test
    fun `memTable simple put and get test`() {
        val memTable = MemTable.create(0)
        memTable.put("0".toTimestampedByteArray(100), "v1".toComparableByteArray())
        memTable.put("0".toTimestampedByteArray(200), "v2".toComparableByteArray())

        assertEquals(memTable.get("0".toTimestampedByteArray(50)), null)
        assertEquals(memTable.get("0".toTimestampedByteArray(100)), "v1".toComparableByteArray())
        assertEquals(memTable.get("0".toTimestampedByteArray(150)), "v1".toComparableByteArray())
        assertEquals(memTable.get("0".toTimestampedByteArray(200)), "v2".toComparableByteArray())
        assertEquals(memTable.get("0".toTimestampedByteArray(250)), "v2".toComparableByteArray())
    }
}
