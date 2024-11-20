package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.LsmStorageInner
import org.github.seonwkim.util.lsmStorageOptionForTest
import kotlin.io.path.createTempDirectory
import kotlin.test.Test

class Day6 {

    @Test
    fun `test task1 storage scan`() {
        val dir = createTempDirectory("test_task1_storage_scan")
        val storage = LsmStorageInner.open(
            path = dir,
            options = lsmStorageOptionForTest()
        )
        storage.put("0".toComparableByteArray(), "2333333".toComparableByteArray())
        storage.put("00".toComparableByteArray(), "2333333".toComparableByteArray())
        storage.put("4".toComparableByteArray(), "23".toComparableByteArray())
        sync(storage)
    }

    private fun sync(storage: LsmStorageInner) {
        storage.state.withWriteLock {
            storage.forceFreezeMemTable(it)
        }
        storage.forceFlushNextImmMemTable()
    }
}
