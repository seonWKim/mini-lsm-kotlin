package org.github.seonwkim.lsm.iterator.storage

import mu.KotlinLogging
import org.github.seonwkim.common.SimulatedRwLock
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.sstable.BlockCache
import org.github.seonwkim.lsm.storage.LsmStorageInner
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.LsmStorageState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors
import kotlin.io.path.createTempDirectory

class LsmStorageInnerTest {

    private val log = KotlinLogging.logger { }

    @Test
    fun `appropriate number of immutableMemTables should be created even in single threaded environment`() {
        val options = LsmStorageOptions(
            blockSize = 2,
            targetSstSize = 2,
            numMemTableLimit = 10
        )

        val storage = LsmStorageInner.open(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
            blockCache = BlockCache(),
            state = SimulatedRwLock(
                value = LsmStorageState(MemTable.create(0)),
                afterReadLockAcquireBehavior = {
                    log.info { "Trying to acquire read lock from ${Thread.currentThread()}" }
                },
                afterWriteLockAcquireBehavior = {
                    log.info { "Trying to acquire write lock from ${Thread.currentThread()}" }
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

        assertEquals(8, storage.state.read().immutableMemTables.size)
    }

    @Test
    fun `appropriate number of immutableMemTables should be created even in concurrent environment`() {
        val options = LsmStorageOptions(
            blockSize = 2,
            targetSstSize = 2,
            numMemTableLimit = 10
        )

        val storage = LsmStorageInner.open(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
            state = SimulatedRwLock(
                value = LsmStorageState(MemTable.create(0)),
                afterReadLockAcquireBehavior = {},
                afterWriteLockAcquireBehavior = { Thread.sleep(100) }
            ),
            blockCache = BlockCache(),
        )

        val executors = List(3) { Executors.newVirtualThreadPerTaskExecutor() }

        val tasks = ('a'..'c').flatMapIndexed { index, c ->
            executors.map { executor ->
                executor.submit {
                    val char = c + index * 3
                    storage.put(char.toString().toComparableByteArray(), "$char$char".toComparableByteArray())
                }
            }
        }

        tasks.forEach { it.get() } // Wait for all tasks to complete

        executors.forEach { it.shutdown() }
        assertEquals(8, storage.state.read().immutableMemTables.size)
    }

    @Test
    fun `the last immutableMemTable should be flushed only once even when multiple threads call forceFlushNextImmMemTable`() {
        val options = LsmStorageOptions(
            blockSize = 2,
            targetSstSize = 2,
            numMemTableLimit = 10
        )

        val storage = LsmStorageInner.open(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
            blockCache = BlockCache(),
            state = SimulatedRwLock(
                value = LsmStorageState(MemTable.create(0)),
                afterWriteLockAcquireBehavior = {
                    Thread.sleep(500)
                    log.info { "Trying to acquire write lock from ${Thread.currentThread()}" }
                }
            )
        )

        // flush memTable into immutableMemTables
        storage.put("a".toComparableByteArray(), "aa".toComparableByteArray())
        storage.put("b".toComparableByteArray(), "bb".toComparableByteArray())
        storage.state.withWriteLock {
            storage.forceFreezeMemTable(it)
        }
        assertTrue { storage.state.read().immutableMemTables.size == 2 }

        // call forceFlushNextImmMemTable in parallel, this will only flush single, the oldest immutableMemTable
        val executors = List(5) { Executors.newSingleThreadExecutor() }
        val tasks = executors.map { executor -> executor.submit { storage.forceFlushNextImmMemTable() } }
        tasks.forEach { it.get() }
        assertEquals(1, storage.state.read().immutableMemTables.size)
        assertEquals(1, storage.state.read().l0SsTables.size)

        // this will flush remaining immutableMemTable
        storage.forceFlushNextImmMemTable()
        assertEquals(0, storage.state.read().immutableMemTables.size)
        assertEquals(2, storage.state.read().l0SsTables.size)

        executors.forEach { it.shutdown() }
    }
}
