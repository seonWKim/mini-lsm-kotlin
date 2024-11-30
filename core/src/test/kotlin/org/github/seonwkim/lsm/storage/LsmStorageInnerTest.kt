package org.github.seonwkim.lsm.storage

import mu.KotlinLogging
import org.github.seonwkim.common.lock.SimulatedRwLock
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.memtable.MemTable
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.io.path.createTempDirectory

class LsmStorageInnerTest {

    private val log = KotlinLogging.logger { }

    @Test
    fun `memtables and immutable memTables IDs must have no gaps and maintain uniqueness`() {
        val options = LsmStorageOptions(
            blockSize = 2 shl 5,
            targetSstSize = 2 shl 10,
            numMemTableLimit = 10,
            enableFlush = false,
            enableCompaction = false
        )
        val storage = LsmStorageInner.open(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
        )
        val availableProcessors = maxOf(Runtime.getRuntime().availableProcessors(), 5)
        val executors = (1..availableProcessors).map {
            Executors.newSingleThreadExecutor()
        }

        val genKey = { it: Int -> "key_%10d".format(it) }
        val genValue = { it: Int -> "value_%100d".format(it) }
        val futures = (1..<100_000).map {
            val key = genKey(it)
            val value = genValue(it)

            executors[it % availableProcessors].submit {
                storage.put(
                    key.toComparableByteArray(),
                    value.toComparableByteArray()
                )
            }
        }
        futures.forEach { it.get() }

        val memTableId = storage.getMemTableId()
        val immutableMemTableIds = storage.getImmutableMemTableIds().sorted()
        val immutableMemTableIdsSet = immutableMemTableIds.toSet()

        // check whether gaps exist
        for (i in 0..<immutableMemTableIds.lastIndex) {
            assertEquals(immutableMemTableIds[i] + 1, immutableMemTableIds[i + 1])
        }

        // check uniqueness of IDs
        assertTrue { immutableMemTableIds.size == immutableMemTableIdsSet.size }
        assertTrue { !immutableMemTableIdsSet.contains(memTableId) }
    }

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

        assertEquals(8, storage.getImmutableMemTablesSize())
    }

    @Test
    @Disabled("Is there a way to freeze memtables even when acquiring write lock takes long?")
    fun `appropriate number of immutableMemTables should be created when acquiring memTable lock takes too long`() {
        val options = LsmStorageOptions(
            blockSize = 2,
            targetSstSize = 2,
            numMemTableLimit = 10,
        )

        val storage = LsmStorageInner.openWithCustomState(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
            customState = LsmStorageState.createWithCustomLock(
                memTable = SimulatedRwLock(value = MemTable.create(0), beforeWriteLockAcquireBehavior = {
                    log.info { "Trying to acquire write lock from ${Thread.currentThread()}" }
                    Thread.sleep(200)
                }),
                immutableMemTables = SimulatedRwLock(value = LinkedList()),
                l0Sstables = SimulatedRwLock(value = LinkedList()),
                levels = SimulatedRwLock(value = LinkedList()),
                sstables = ConcurrentHashMap()
            )
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
        assertEquals(8, storage.getImmutableMemTablesSize())
    }

    @Test
    fun `the last immutableMemTable should be flushed only once even when multiple threads call forceFlushNextImmMemTable`() {
        val options = LsmStorageOptions(
            blockSize = 2,
            targetSstSize = 2,
            numMemTableLimit = 10,
        )

        val storage = LsmStorageInner.openWithCustomState(
            path = createTempDirectory("test_concurrency").resolve("1.sst"),
            options = options,
            customState = LsmStorageState.createWithCustomLock(
                memTable = SimulatedRwLock(value = MemTable.create(0)),
                immutableMemTables = SimulatedRwLock(value = LinkedList(), beforeWriteLockAcquireBehavior = {
                    log.info { "Trying to acquire write lock from ${Thread.currentThread()}" }
                    Thread.sleep(200)
                }),
                l0Sstables = SimulatedRwLock(value = LinkedList()),
                levels = SimulatedRwLock(value = LinkedList()),
                sstables = ConcurrentHashMap()
            )
        )

        // flush memTable into immutableMemTables
        storage.put("a".toComparableByteArray(), "aa".toComparableByteArray())
        storage.put("b".toComparableByteArray(), "bb".toComparableByteArray())
        storage.forceFreezeMemTable()
        assertTrue { storage.getImmutableMemTablesSize() == 2 }

        // call forceFlushNextImmMemTable in parallel, this will only flush single, the oldest immutableMemTable
        val executors = List(5) { Executors.newSingleThreadExecutor() }
        val tasks = executors.map { executor -> executor.submit { storage.forceFlushNextImmMemTable() } }
        tasks.forEach { it.get() }
        assertEquals(1, storage.getImmutableMemTablesSize())
        assertEquals(1, storage.getL0SstablesSize())

        // this will flush remaining immutableMemTable
        storage.forceFlushNextImmMemTable()
        assertEquals(0, storage.getImmutableMemTablesSize())
        assertEquals(2, storage.getL0SstablesSize())

        executors.forEach { it.shutdown() }
    }
}
