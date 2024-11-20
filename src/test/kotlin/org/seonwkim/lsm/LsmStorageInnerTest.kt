package org.seonwkim.lsm

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.seonwkim.common.SimulatedRwLock
import org.seonwkim.common.toComparableByteArray
import org.seonwkim.lsm.memtable.MemTable
import org.seonwkim.lsm.sstable.BlockCache
import java.util.concurrent.Executors
import kotlin.io.path.createTempDirectory

class LsmStorageInnerTest {

    @Test
    fun `no concurrent running threads`() {
        val options = LsmStorageOptions(
            blockSize = 2,
            targetSstSize = 2,
            numMemTableLimit = 10
        )

        val storage = LsmStorageInner(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
            state = LsmStorageState(MemTable.create(0)),
            blockCache = BlockCache(),
            lock = SimulatedRwLock(
                readBehavior = {
                    println("Acquired read lock from ${Thread.currentThread()}")
                },
                writeBehavior = {
                    println("Acquired write lock from ${Thread.currentThread()}")
                }
            )
        )

        storage.put("a".toComparableByteArray(), "aa".toComparableByteArray())
        // freeze and then 
        storage.put("b".toComparableByteArray(), "bb".toComparableByteArray())
        // freeze and then  
        storage.put("c".toComparableByteArray(), "cc".toComparableByteArray())
        // freeze and then  
        storage.put("d".toComparableByteArray(), "dd".toComparableByteArray())
        // freeze and then  
        storage.put("e".toComparableByteArray(), "ee".toComparableByteArray())
        // freeze and then  
        storage.put("f".toComparableByteArray(), "ff".toComparableByteArray())
        // freeze and then  
        storage.put("g".toComparableByteArray(), "gg".toComparableByteArray())
        // freeze and then
        storage.put("h".toComparableByteArray(), "hh".toComparableByteArray())
        // freeze and then
        storage.put("i".toComparableByteArray(), "ii".toComparableByteArray())

        assertEquals(8, storage.state.immutableMemTables.size)
    }

    @Test
    fun `concurrent running threads`() {
        val options = LsmStorageOptions(
            blockSize = 2,
            targetSstSize = 2,
            numMemTableLimit = 10
        )

        val storage = LsmStorageInner(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
            state = LsmStorageState(MemTable.create(0)),
            blockCache = BlockCache(),
            lock = SimulatedRwLock(
                readBehavior = {
                },
                writeBehavior = {
                    Thread.sleep(1000)
                }
            )
        )

        val executor1 = Executors.newVirtualThreadPerTaskExecutor()
        val executor2 = Executors.newVirtualThreadPerTaskExecutor()
        val executor3 = Executors.newVirtualThreadPerTaskExecutor()

        val tasks = ('a'..'c').flatMap { c ->
            Thread.sleep(100)
            listOf(
                executor1.submit {
                    val char = c
                    storage.put(char.toString().toComparableByteArray(), "$char$char".toComparableByteArray())
                },
                executor2.submit {
                    val char = c + 3
                    storage.put(char.toString().toComparableByteArray(), "$char$char".toComparableByteArray())
                },
                executor3.submit {
                    val char = c + 6
                    storage.put(char.toString().toComparableByteArray(), "$char$char".toComparableByteArray())
                }
            )
        }

        tasks.forEach { it.get() } // Wait for all tasks to complete

        executor1.shutdown()
        assertEquals(8, storage.state.immutableMemTables.size)
    }
}
