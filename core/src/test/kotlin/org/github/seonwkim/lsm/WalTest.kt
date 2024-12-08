package org.github.seonwkim.lsm

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.common.toComparableByteArray

import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.ConcurrentSkipListMap
import kotlin.io.path.createTempDirectory
import kotlin.test.Test

class WalTest {

    @Test
    fun `test WAL put and recover`() {
        val tempDir = createTempDirectory()
        val walPath = tempDir.resolve("test.wal")

        // Create WAL and put keys
        val wal = Wal.create(walPath)
        val key1 = "key1".toComparableByteArray()
        val value1 = "value1".toComparableByteArray()
        wal.put(key1, value1)

        val key2 = "key2".toComparableByteArray()
        val value2 = "value2".toComparableByteArray()
        wal.put(key2, value2)

        // Retrieve keys from WAL
        val map = ConcurrentSkipListMap<ComparableByteArray, ComparableByteArray>()
        Wal.recover(walPath, map)

        assertEquals(2, map.size)
        assertEquals(value1, map[key1])
        assertEquals(value2, map[key2])
    }
}
