package org.github.seonwkim.lsm

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedByteArray
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.common.toTimestampedByteArray

import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.ConcurrentSkipListMap
import kotlin.io.path.createTempDirectory
import kotlin.test.Test

class WalTest {

    @Test
    fun `test WAL put and recover with timestamp`() {
        val tempDir = createTempDirectory()
        val walPath = tempDir.resolve("test.wal")

        // Create WAL and put keys
        val wal = Wal.create(walPath)
        val key1 = "key1".toTimestampedByteArray(timestamp = 5)
        val value1 = "value1".toComparableByteArray()
        wal.put(key1, value1)

        val key2 = "key2".toTimestampedByteArray(timestamp = 4)
        val value2 = "value2".toComparableByteArray()
        wal.put(key2, value2)

        // Retrieve keys from WAL
        val map = ConcurrentSkipListMap<TimestampedByteArray, ComparableByteArray>()
        Wal.recover(walPath, map)

        assertEquals(2, map.size)
        assertEquals(value1, map[key1])
        assertEquals(value2, map[key2])
    }
}
