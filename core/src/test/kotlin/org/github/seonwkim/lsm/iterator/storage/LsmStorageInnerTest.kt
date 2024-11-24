package org.github.seonwkim.lsm.iterator.storage

import mu.KotlinLogging
import org.github.seonwkim.common.SimulatedRwLock
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.storage.LsmStorageInner
import org.github.seonwkim.lsm.storage.LsmStorageOptions
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
            numMemTableLimit = 10,
        )

        val storage = LsmStorageInner.open(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
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

        assertEquals(8, storage.stateManager.snapshot().getImmutableMemTablesSize())
    }

    @Test
    fun `appropriate number of immutableMemTables should be created even in concurrent environment`() {
        val options = LsmStorageOptions(
            blockSize = 2,
            targetSstSize = 2,
            numMemTableLimit = 10,
            customizableMemTableLock = SimulatedRwLock(
                value = Unit,
                afterWriteLockAcquireBehavior = { Thread.sleep(100) }
            )
        )

        val storage = LsmStorageInner.open(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
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
        assertEquals(8, storage.stateManager.snapshot().getImmutableMemTablesSize())
    }

    @Test
    fun `the last immutableMemTable should be flushed only once even when multiple threads call forceFlushNextImmMemTable`() {
        val options = LsmStorageOptions(
            blockSize = 2,
            targetSstSize = 2,
            numMemTableLimit = 10,
            customizableMemTableLock = SimulatedRwLock(
                value = Unit ,
                afterWriteLockAcquireBehavior = {
                    Thread.sleep(500)
                    log.info { "Trying to acquire write lock from ${Thread.currentThread()}" }
                }
            )
        )

        val storage = LsmStorageInner.open(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
        )

        // flush memTable into immutableMemTables
        storage.put("a".toComparableByteArray(), "aa".toComparableByteArray())
        storage.put("b".toComparableByteArray(), "bb".toComparableByteArray())
        storage.stateManager.forceFreezeMemTable()
        assertTrue { storage.stateManager.snapshot().getImmutableMemTablesSize() == 2 }

        // call forceFlushNextImmMemTable in parallel, this will only flush single, the oldest immutableMemTable
        val executors = List(5) { Executors.newSingleThreadExecutor() }
        val tasks = executors.map { executor -> executor.submit { storage.forceFlushNextImmMemTable() } }
        tasks.forEach { it.get() }
        assertEquals(1, storage.stateManager.snapshot().getImmutableMemTablesSize())
        assertEquals(1, storage.stateManager.snapshot().getL0SstablesSize())

        // this will flush remaining immutableMemTable
        storage.forceFlushNextImmMemTable()
        assertEquals(0, storage.stateManager.snapshot().getImmutableMemTablesSize())
        assertEquals(2, storage.stateManager.snapshot().getL0SstablesSize())

        executors.forEach { it.shutdown() }
    }
}
