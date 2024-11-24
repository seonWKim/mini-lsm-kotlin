package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.storage.LsmStorageInner
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.junit.jupiter.api.assertDoesNotThrow
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day1 {

    @Test
    fun `test task1 memTable get`() {
        val memTable = MemTable.create(0)
        memTable.forTestingPutSlice("key1".toComparableByteArray(), "value1".toComparableByteArray())
        memTable.forTestingPutSlice("key2".toComparableByteArray(), "value2".toComparableByteArray())
        memTable.forTestingPutSlice("key3".toComparableByteArray(), "value3".toComparableByteArray())
        assertEquals(
            memTable.forTestingGetSlice("key1".toComparableByteArray())?.value,
            "value1".toComparableByteArray()
        )
        assertEquals(
            memTable.forTestingGetSlice("key2".toComparableByteArray())?.value,
            "value2".toComparableByteArray()
        )
        assertEquals(
            memTable.forTestingGetSlice("key3".toComparableByteArray())?.value,
            "value3".toComparableByteArray()
        )
    }

    @Test
    fun `test task1 memTable overwrite`() {
        val memTable = MemTable.create(0)
        memTable.forTestingPutSlice("key1".toComparableByteArray(), "value1".toComparableByteArray())
        memTable.forTestingPutSlice("key2".toComparableByteArray(), "value2".toComparableByteArray())
        memTable.forTestingPutSlice("key3".toComparableByteArray(), "value3".toComparableByteArray())
        memTable.forTestingPutSlice("key1".toComparableByteArray(), "value11".toComparableByteArray())
        memTable.forTestingPutSlice("key2".toComparableByteArray(), "value22".toComparableByteArray())
        memTable.forTestingPutSlice("key3".toComparableByteArray(), "value33".toComparableByteArray())
        assertEquals(
            memTable.forTestingGetSlice("key1".toComparableByteArray())?.value,
            "value11".toComparableByteArray()
        )
        assertEquals(
            memTable.forTestingGetSlice("key2".toComparableByteArray())?.value,
            "value22".toComparableByteArray()
        )
        assertEquals(
            memTable.forTestingGetSlice("key3".toComparableByteArray())?.value,
            "value33".toComparableByteArray()
        )
    }

    @Test
    fun `test task2 storage integration`() {
        val dir = createTempDirectory("test_task2_storage_integration")
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        assertEquals(storage.get("0".toComparableByteArray()), null)
        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        assertEquals(storage.get("1".toComparableByteArray()), "233".toComparableByteArray())
        assertEquals(storage.get("2".toComparableByteArray()), "2333".toComparableByteArray())
        assertEquals(storage.get("3".toComparableByteArray()), "23333".toComparableByteArray())
        storage.delete("2".toComparableByteArray())
        assertEquals(storage.get("2".toComparableByteArray()), null)
        assertDoesNotThrow { storage.delete("0".toComparableByteArray()) }
    }

    @Test
    fun `test task3 storage integration`() {
        val dir = createTempDirectory("test_task3_storage_integration")
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        storage.forceFreezeMemTable()
        assertEquals(storage.snapshot().getImmutableMemTablesSize(), 1)
        val previousApproximateSize = storage.snapshot().getImmutableMemTableApproximateSize(0)
        assertTrue("previousApproximate size should be greater than or equal to 15") { previousApproximateSize >= 15 }
        storage.put("1".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "23333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "233333".toComparableByteArray())
        storage.forceFreezeMemTable()
        assertEquals(storage.snapshot().getImmutableMemTablesSize(), 2)
        assertTrue("Wrong order of memtables?") { storage.snapshot().getImmutableMemTableApproximateSize(1) == previousApproximateSize }
        assertTrue { storage.snapshot().getImmutableMemTableApproximateSize(0) > previousApproximateSize }
    }

    @Test
    fun `test task3 freeze on capacity`() {
        val dir = createTempDirectory("test_task3_freeze_on_capacity")
        val option = LsmStorageOptions(
            blockSize = 4096,
            targetSstSize = 1024,
            numMemTableLimit = 1000
        )
        val storage = LsmStorageInner.open(dir, option)
        for (_unused in 0..1000) {
            storage.put("1".toComparableByteArray(), "2333".toComparableByteArray())
        }
        val numImmMemtables = storage.snapshot().getImmutableMemTablesSize()
        assertTrue("No memtables frozen?") { numImmMemtables >= 1 }
        for (_unused in 0..1000) {
            storage.delete("1".toComparableByteArray())
        }
        assertTrue("No more memTable frozen?") { storage.snapshot().getImmutableMemTablesSize() > numImmMemtables }
    }

    @Test
    fun `test task4 storage integration`() {
        val dir = createTempDirectory("test_task4_storage_integration")
        val storage = LsmStorageInner.open(dir, lsmStorageOptionForTest())
        assertEquals(storage.get("0".toComparableByteArray()), null)
        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        storage.forceFreezeMemTable()
        storage.delete("1".toComparableByteArray())
        storage.delete("2".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("4".toComparableByteArray(), "23333".toComparableByteArray())
        storage.forceFreezeMemTable()
        storage.put("1".toComparableByteArray(), "233333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "233333".toComparableByteArray())
        assertEquals(storage.snapshot().getImmutableMemTablesSize(), 2)
        assertEquals(storage.get("1".toComparableByteArray()), "233333".toComparableByteArray())
        assertEquals(storage.get("2".toComparableByteArray()), null)
        assertEquals(storage.get("3".toComparableByteArray()), "233333".toComparableByteArray())
        assertEquals(storage.get("4".toComparableByteArray()), "23333".toComparableByteArray())
    }

    private fun lsmStorageOptionForTest(): LsmStorageOptions = LsmStorageOptions(
        blockSize = 4096,
        targetSstSize = 2 shl 20,
        numMemTableLimit = 10
    )
}
