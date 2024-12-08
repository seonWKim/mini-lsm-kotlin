package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.toTimestampedByteArray
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.LsmStorageInner
import org.github.seonwkim.lsm.LsmStorageOptions
import org.junit.jupiter.api.assertDoesNotThrow
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day1 {

    @Test
    fun `test task1 memTable get`() {
        val memTable = MemTable.create(0)
        memTable.forTestingPutSlice("key1".toTimestampedByteArray(), "value1".toTimestampedByteArray())
        memTable.forTestingPutSlice("key2".toTimestampedByteArray(), "value2".toTimestampedByteArray())
        memTable.forTestingPutSlice("key3".toTimestampedByteArray(), "value3".toTimestampedByteArray())
        assertEquals(
            memTable.forTestingGetSlice("key1".toTimestampedByteArray()),
            "value1".toTimestampedByteArray()
        )
        assertEquals(
            memTable.forTestingGetSlice("key2".toTimestampedByteArray()),
            "value2".toTimestampedByteArray()
        )
        assertEquals(
            memTable.forTestingGetSlice("key3".toTimestampedByteArray()),
            "value3".toTimestampedByteArray()
        )
    }

    @Test
    fun `test task1 memTable overwrite`() {
        val memTable = MemTable.create(0)
        memTable.forTestingPutSlice("key1".toTimestampedByteArray(), "value1".toTimestampedByteArray())
        memTable.forTestingPutSlice("key2".toTimestampedByteArray(), "value2".toTimestampedByteArray())
        memTable.forTestingPutSlice("key3".toTimestampedByteArray(), "value3".toTimestampedByteArray())
        memTable.forTestingPutSlice("key1".toTimestampedByteArray(), "value11".toTimestampedByteArray())
        memTable.forTestingPutSlice("key2".toTimestampedByteArray(), "value22".toTimestampedByteArray())
        memTable.forTestingPutSlice("key3".toTimestampedByteArray(), "value33".toTimestampedByteArray())
        assertEquals(
            memTable.forTestingGetSlice("key1".toTimestampedByteArray()),
            "value11".toTimestampedByteArray()
        )
        assertEquals(
            memTable.forTestingGetSlice("key2".toTimestampedByteArray()),
            "value22".toTimestampedByteArray()
        )
        assertEquals(
            memTable.forTestingGetSlice("key3".toTimestampedByteArray()),
            "value33".toTimestampedByteArray()
        )
    }

    @Test
    fun `test task2 storage integration`() {
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        assertEquals(storage.get("0".toTimestampedByteArray()), null)
        storage.put("1".toTimestampedByteArray(), "233".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        assertEquals(storage.get("1".toTimestampedByteArray()), "233".toTimestampedByteArray())
        assertEquals(storage.get("2".toTimestampedByteArray()), "2333".toTimestampedByteArray())
        assertEquals(storage.get("3".toTimestampedByteArray()), "23333".toTimestampedByteArray())
        storage.delete("2".toTimestampedByteArray())
        assertEquals(storage.get("2".toTimestampedByteArray()), null)
        assertDoesNotThrow { storage.delete("0".toTimestampedByteArray()) }
    }

    @Test
    fun `test task3 storage integration`() {
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        storage.put("1".toTimestampedByteArray(), "233".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()
        assertEquals(storage.getImmutableMemTablesSize(), 1)
        val previousApproximateSize = storage.getImmutableMemTableApproximateSize(0)
        assertTrue("previousApproximate size should be greater than or equal to 15") { previousApproximateSize >= 15 }
        storage.put("1".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "233333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()
        assertEquals(storage.getImmutableMemTablesSize(), 2)
        assertTrue("Wrong order of memtables?") { storage.getImmutableMemTableApproximateSize(1) == previousApproximateSize }
        assertTrue { storage.getImmutableMemTableApproximateSize(0) > previousApproximateSize }
    }

    @Test
    fun `test task3 freeze on capacity`() {
        val dir = createTempDirectory()
        val option = LsmStorageOptions(
            blockSize = 4096,
            targetSstSize = 1024,
            numMemTableLimit = 1000
        )
        val storage = LsmStorageInner.open(dir, option)
        for (_unused in 0..1000) {
            storage.put("1".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        }
        val numImmMemtables = storage.getImmutableMemTablesSize()
        assertTrue("No memtables frozen?") { numImmMemtables >= 1 }
        for (_unused in 0..1000) {
            storage.delete("1".toTimestampedByteArray())
        }
        assertTrue("No more memTable frozen?") { storage.getImmutableMemTablesSize() > numImmMemtables }
    }

    @Test
    fun `test task4 storage integration`() {
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        assertEquals(storage.get("0".toTimestampedByteArray()), null)
        storage.put("1".toTimestampedByteArray(), "233".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()
        storage.delete("1".toTimestampedByteArray())
        storage.delete("2".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.put("4".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        storage.forceFreezeMemTableWithLock()
        storage.put("1".toTimestampedByteArray(), "233333".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "233333".toTimestampedByteArray())
        assertEquals(storage.getImmutableMemTablesSize(), 2)
        assertEquals(storage.get("1".toTimestampedByteArray()), "233333".toTimestampedByteArray())
        assertEquals(storage.get("2".toTimestampedByteArray()), null)
        assertEquals(storage.get("3".toTimestampedByteArray()), "233333".toTimestampedByteArray())
        assertEquals(storage.get("4".toTimestampedByteArray()), "23333".toTimestampedByteArray())
    }

    private fun lsmStorageOptionForTest(): LsmStorageOptions = LsmStorageOptions(
        blockSize = 4096,
        targetSstSize = 2 shl 20,
        numMemTableLimit = 10
    )
}
