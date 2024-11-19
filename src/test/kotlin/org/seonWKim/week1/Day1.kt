package org.seonWKim.week1

import org.seonWKim.lsm.LsmStorageInner
import org.seonWKim.lsm.LsmStorageOptions
import org.seonWKim.lsm.memtable.MemTable
import org.seonWKim.common.toComparableByteArray
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
        assertEquals(memTable.forTestingGetSlice("key1".toComparableByteArray())?.value, "value1".toComparableByteArray())
        assertEquals(memTable.forTestingGetSlice("key2".toComparableByteArray())?.value, "value2".toComparableByteArray())
        assertEquals(memTable.forTestingGetSlice("key3".toComparableByteArray())?.value, "value3".toComparableByteArray())
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
        assertEquals(memTable.forTestingGetSlice("key1".toComparableByteArray())?.value, "value11".toComparableByteArray())
        assertEquals(memTable.forTestingGetSlice("key2".toComparableByteArray())?.value, "value22".toComparableByteArray())
        assertEquals(memTable.forTestingGetSlice("key3".toComparableByteArray())?.value, "value33".toComparableByteArray())
    }

    @Test
    fun `test task2 storage integration`() {
        val dir = createTempDirectory("test_task2_storage_integration")
        val storage = LsmStorageInner.open(dir, LsmStorageOptions.defaultForWeek1Test())
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
        val storage = LsmStorageInner.open(dir, LsmStorageOptions.defaultForWeek1Test())
        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        storage.forceFreezeMemtable()
        assertEquals(storage.state.immutableMemtables.size, 1)
        val previousApproximateSize = storage.state.immutableMemtables[0].approximateSize()
        assertTrue ("previousApproximate size should be greater than or equal to 15") { previousApproximateSize >= 15 }
        storage.put("1".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "23333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "233333".toComparableByteArray())
        storage.forceFreezeMemtable()
        assertEquals(storage.state.immutableMemtables.size, 2)
        assertTrue("Wrong order of memtables?") { storage.state.immutableMemtables[1].approximateSize() == previousApproximateSize }
        assertTrue { storage.state.immutableMemtables[0].approximateSize() > previousApproximateSize }
    }

    @Test
    fun `test task3 freeze on capacity`() {
        val dir = createTempDirectory("test_task3_freeze_on_capacity")
        val option = LsmStorageOptions.defaultForWeek1Test()
        option.setTargetSstSize(1024)
        option.setNumMemtableLimit(1000)
        val storage = LsmStorageInner.open(dir, option)
        for (_unused in 0..1000) {
            storage.put("1".toComparableByteArray(), "2333".toComparableByteArray())
        }
        val numImmMemtables = storage.state.immutableMemtables.size
        assertTrue("No memtables frozen?") { numImmMemtables >= 1 }
        for (_unused in 0..1000) {
            storage.delete("1".toComparableByteArray())
        }
        assertTrue("No more memTable frozen?") { storage.state.immutableMemtables.size > numImmMemtables }
    }

    @Test
    fun `test task4 storage integration`() {
        val dir = createTempDirectory("test_task4_storage_integration")
        val storage = LsmStorageInner.open(dir, LsmStorageOptions.defaultForWeek1Test())
        assertEquals(storage.get("0".toComparableByteArray()), null)
        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        storage.forceFreezeMemtable()
        storage.delete("1".toComparableByteArray())
        storage.delete("2".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("4".toComparableByteArray(), "23333".toComparableByteArray())
        storage.forceFreezeMemtable()
        storage.put("1".toComparableByteArray(), "233333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "233333".toComparableByteArray())
        assertEquals(storage.state.immutableMemtables.size, 2)
        assertEquals(storage.get("1".toComparableByteArray()), "233333".toComparableByteArray())
        assertEquals(storage.get("2".toComparableByteArray()), null)
        assertEquals(storage.get("3".toComparableByteArray()), "233333".toComparableByteArray())
        assertEquals(storage.get("4".toComparableByteArray()), "23333".toComparableByteArray())
    }
}
