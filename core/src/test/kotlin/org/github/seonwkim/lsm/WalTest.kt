package org.github.seonwkim.lsm

import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.ConcurrentSkipListMap
import kotlin.io.path.createTempDirectory
import kotlin.test.Test

class WalTest {

    @Test
    fun `test WAL put and recover`() {
        val tempDir = createTempDirectory("wal_test")
        val walPath = tempDir.resolve("test.wal")

        // Create WAL and put keys
        val wal = Wal.create(walPath)
        val key1 = TimestampedKey("key1".toComparableByteArray())
        val value1 = MemtableValue("value1".toComparableByteArray())
        wal.put(key1, value1)

        val key2 = TimestampedKey("key2".toComparableByteArray())
        val value2 = MemtableValue("value2".toComparableByteArray())
        wal.put(key2, value2)

        // Retrieve keys from WAL
        val map = ConcurrentSkipListMap<TimestampedKey, MemtableValue>()
        Wal.recover(walPath, map)

        assertEquals(2, map.size)
        assertEquals(value1, map[key1])
        assertEquals(value2, map[key2])
    }
}
