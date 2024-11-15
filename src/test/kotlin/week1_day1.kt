import org.example.LsmStorageInner
import org.example.LsmStorageOptions
import org.example.MemTable
import org.junit.jupiter.api.assertDoesNotThrow
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class week1_day1 {

    @Test
    fun `test task1 memtable get`() {
        val memtable = MemTable.create(0)
        memtable.forTestingPutSlice("key1".toByteArray(), "value1".toByteArray())
        memtable.forTestingPutSlice("key2".toByteArray(), "value2".toByteArray())
        memtable.forTestingPutSlice("key3".toByteArray(), "value3".toByteArray())
        assertEquals(memtable.forTestingGetSlice("key1".toByteArray()), "value1".toByteArray())
        assertEquals(memtable.forTestingGetSlice("key2".toByteArray()), "value2".toByteArray())
        assertEquals(memtable.forTestingGetSlice("key3".toByteArray()), "value3".toByteArray())
    }

    @Test
    fun `test task1 memtable overwrite`() {
        val memtable = MemTable.create(0)
        memtable.forTestingPutSlice("key1".toByteArray(), "value1".toByteArray())
        memtable.forTestingPutSlice("key2".toByteArray(), "value2".toByteArray())
        memtable.forTestingPutSlice("key3".toByteArray(), "value3".toByteArray())
        memtable.forTestingPutSlice("key1".toByteArray(), "value11".toByteArray())
        memtable.forTestingPutSlice("key2".toByteArray(), "value22".toByteArray())
        memtable.forTestingPutSlice("key3".toByteArray(), "value33".toByteArray())
        assertEquals(memtable.forTestingGetSlice("key1".toByteArray()), "value11".toByteArray())
        assertEquals(memtable.forTestingGetSlice("key2".toByteArray()), "value22".toByteArray())
        assertEquals(memtable.forTestingGetSlice("key3".toByteArray()), "value33".toByteArray())
    }

    @Test
    fun `test task2 storage integration`() {
        val dir = createTempDirectory("test_task2_storage_integration")
        val storage = LsmStorageInner.open(dir, LsmStorageOptions.default_for_week1_test())
        assertEquals(storage.get("0".toByteArray()), null)
        storage.put("1".toByteArray(), "233".toByteArray())
        storage.put("2".toByteArray(), "2333".toByteArray())
        storage.put("3".toByteArray(), "23333".toByteArray())
        assertEquals(storage.get("1".toByteArray()), "233".toByteArray())
        assertEquals(storage.get("2".toByteArray()), "2333".toByteArray())
        assertEquals(storage.get("3".toByteArray()), "23333".toByteArray())
        storage.delete("2".toByteArray())
        assertEquals(storage.get("2".toByteArray()), null)
        assertDoesNotThrow { storage.delete("0".toByteArray()) }
    }

    @Test
    fun `test task3 storage integration`() {
        val dir = createTempDirectory("test_task3_storage_integration")
        val storage = LsmStorageInner.open(dir, LsmStorageOptions.default_for_week1_test())
        storage.put("1".toByteArray(), "233".toByteArray())
        storage.put("2".toByteArray(), "2333".toByteArray())
        storage.put("3".toByteArray(), "23333".toByteArray())
        storage.forceFreezeMemtable() // TODO
        assertEquals(storage.state.immutableMemtables.size, 1)
        val previousApproximateSize = storage.state.immutableMemtables[0].approximateSize()
        assertFalse("previousApproximate size should not be greater than or equal to 15") { previousApproximateSize >= 15 }
        storage.put("1".toByteArray(), "2333".toByteArray())
        storage.put("2".toByteArray(), "23333".toByteArray())
        storage.put("3".toByteArray(), "233333".toByteArray())
        storage.forceFreezeMemtable()
        assertEquals(storage.state.immutableMemtables.size, 2)
        assertTrue("Wrong order of memtables?") { storage.state.immutableMemtables[1].approximateSize() == previousApproximateSize }
        assertTrue { storage.state.immutableMemtables[0].approximateSize() > previousApproximateSize }
    }

    @Test
    fun `test task3 freeze on capacity`() {
        val dir = createTempDirectory("test_task3_freeze_on_capacity")
        val option = LsmStorageOptions.default_for_week1_test()
        option.setTargetSstSize(1024)
        option.setNumMemtableLimit(1000)
        val storage = LsmStorageInner.open(dir, option)
        for (_unused in 0..1000) {
            storage.put("1".toByteArray(), "2333".toByteArray())
        }
        val numImmMemtables = storage.state.immutableMemtables.size
        assertTrue("No memtables frozen?") { numImmMemtables >= 1 }
        for (_unused in 0..1000) {
            storage.delete("1".toByteArray())
        }
        assertTrue("No more memtable frozen?") { storage.state.immutableMemtables.size > numImmMemtables }
    }

    @Test
    fun `test task4 storage integration`() {
        val dir = createTempDirectory("test_task4_storage_integration")
        val storage = LsmStorageInner.open(dir, LsmStorageOptions.default_for_week1_test())
        assertEquals(storage.get("0".toByteArray()), null)
        storage.put("1".toByteArray(), "233".toByteArray())
        storage.put("2".toByteArray(), "2333".toByteArray())
        storage.put("3".toByteArray(), "23333".toByteArray())
        storage.forceFreezeMemtable()
        storage.delete("1".toByteArray())
        storage.delete("2".toByteArray())
        storage.put("3".toByteArray(), "2333".toByteArray())
        storage.put("4".toByteArray(), "23333".toByteArray())
        storage.forceFreezeMemtable()
        storage.put("1".toByteArray(), "233333".toByteArray())
        storage.put("3".toByteArray(), "233333".toByteArray())
        assertEquals(storage.state.immutableMemtables.size, 2)
        assertEquals(storage.get("1".toByteArray()), "233333".toByteArray())
        assertEquals(storage.get("2".toByteArray()), null)
        assertEquals(storage.get("3".toByteArray()), "233333".toByteArray())
        assertEquals(storage.get("4".toByteArray()), "23333".toByteArray())
    }
}
