package org.github.seonwkim.lsm.storage

import mu.KotlinLogging
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.RwLock
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.sstable.BlockCache
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import org.github.seonwkim.lsm.sstable.Sstable
import org.github.seonwkim.lsm.sstable.sstPath
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

class LsmStorageStateConcurrencyManager(
    private val path: Path,
    private val state: LsmStorageState,
    private val options: LsmStorageStateOptions,
    private val nextSstId: AtomicInteger,
    val blockCache: BlockCache,
    private var manifest: Manifest? = null,
    private val memTableLock: RwLock<Unit> = options.customizableMemTableLock,
    private val immutableMemTableLock: RwLock<Unit> = options.customizableMemTableLock
) {

    private val log = KotlinLogging.logger { }

    /**
     * Retrieves the current MemTable ID.
     *
     * Note: MemTable IDs may not be strictly sequential because a read lock on [memTableLock] is not acquired.
     *
     * @return The ID of the current MemTable.
     */
    fun memTableId(): Int {
        return state.memTable.get().id
    }

    /**
     * Adds a new SSTable ID to the list of L0 SSTables.
     *
     * @param sstableId The ID of the SSTable to be added.
     */
    fun addNewL0SstableId(sstableId: Int) {
        state.l0Sstables.addFirst(sstableId)
    }

    /**
     * Adds a new SSTable level to the list of levels.
     *
     * @param sstLevel The SSTable level to be added.
     */
    fun addNewSstLevel(sstLevel: SstLevel) {
        state.levels.addFirst(sstLevel)
    }

    /**
     * Retrieve all sstable ids from l0ssTables and levels.
     */
    fun getTotalSstableIds(): List<Int> {
        return state.l0Sstables + state.levels.flatMap { it.sstIds }
    }

    /**
     * Sets a new MemTable in the state.
     *
     * This method updates the current MemTable to the provided new MemTable instance.
     * The update is performed atomically to ensure thread safety.
     *
     * @param memTable The new MemTable instance to be set.
     */
    fun setMemTable(memTable: MemTable) {
        state.memTable.set(memTable)
    }

    /**
     * Sets the given `Sstable` with the specified `sstableId` in the state.
     *
     * @param sstableId The ID of the `Sstable` to be set.
     * @param sst The `Sstable` instance to be set in the state.
     */
    fun setSstable(sstableId: Int, sst: Sstable) {
        state.sstables[sstableId] = sst
    }

    /**
     * Sorts the SSTables within each level based on their first keys.
     *
     * This method iterates through all levels and sorts the SSTables in each level
     * by comparing their first keys.
     */
    fun sortLevels() {
        val sstables = state.sstables
        for ((_, ssts) in state.levels) {
            ssts.sortWith { t1, t2 ->
                sstables[t1]!!.firstKey.compareTo(sstables[t2]!!.firstKey)
            }
        }
    }

    fun snapshot(timestamp: Long = 0L): LsmStateSnapshot {
        return LsmStateSnapshot(
            state = state,
            blockCache = blockCache,
            timestamp = timestamp
        )
    }

    fun nextSstId(): Int {
        return nextSstId.get()
    }

    fun setNextSstId(id: Int) {
        return nextSstId.set(id)
    }

    fun incrementNextSstId() {
        nextSstId.incrementAndGet()
    }

    /**
     * Inserts a key-value pair into the current memTable.
     *
     * If the current memTable exceeds the target size, it will be frozen and a new memTable will be created.
     * This method ensures that the memTable freezing operation is synchronized to prevent race conditions.
     * However, if acquiring the write lock takes too long, memTables that exceed the limit might be created.
     *
     * @param key The key to be inserted. It must be a `ComparableByteArray` to ensure proper ordering.
     * @param value The value to be associated with the key. It must be a `MemtableValue` which can represent the actual data or a tombstone for deletions.
     */
    fun put(key: ComparableByteArray, value: MemtableValue) {
        if (shouldFreezeMemTable()) {
            memTableLock.withWriteLock {
                if (shouldFreezeMemTable()) {
                    forceFreezeMemTable()
                }
            }
        }

        state.memTable.get().put(key, value)
    }

    /**
     * Freezes the current memTable and returns the ID of the previous memTable.
     *
     * This method increments the next SSTable ID and creates a new memTable.
     * The current memTable is added to the head of immutable memTables.
     * If Write-Ahead Logging (WAL) is enabled, the method will need to be implemented to support WAL.
     *
     * @return The ID of the previous memTable.
     */
    fun forceFreezeMemTable(): Int {
        val memtableId = nextSstId.getAndIncrement()
        val memTable = if (options.enableWal) {
            TODO("should be implemented after WAL is supported")
        } else {
            MemTable.create(memtableId)
        }

        state.immutableMemTables.addFirst(state.memTable.get())
        state.memTable.set(memTable)

        return memtableId
    }

    private fun shouldFreezeMemTable(): Boolean {
        return memTableLock.withReadLock {
            state.memTable.get().approximateSize() > options.targetSstSize
        }
    }

    fun shouldFlush(): Boolean {
        return state.immutableMemTables.size >= options.numMemTableLimit
    }

    /**
     * Force flushes the earliest created immutable memTable to disk and returns the flushed memTable ID.
     *
     * @return The ID of the flushed memTable.
     * @throws IllegalStateException if no immutable memTables exist.
     */
    fun forceFlushNextImmutableMemTable(): Int {
        lateinit var flushMemTable: MemTable
        immutableMemTableLock.withReadLock {
            flushMemTable = state.immutableMemTables.lastOrNull()
                ?: throw IllegalStateException("No immutable memTables exist!")
        }

        val builder = SsTableBuilder(options.blockSize)
        flushMemTable.flush(builder)
        val flushedMemTableId = flushMemTable.id
        val sst = builder.build(
            id = flushedMemTableId,
            blockCache = blockCache.copy(),
            path = sstPath(path, flushedMemTableId)
        )

        immutableMemTableLock.withWriteLock {
            val immutableMemTable: MemTable? = state.immutableMemTables.peekLast()
            if (immutableMemTable?.id != flushedMemTableId) {
                // in case where forceFlushNextImmMemTable() is called concurrently, it can enter this block
                log.info { "Sst id($flushedMemTableId) and immutable memTable id(${immutableMemTable?.id}) mismatch. There might be concurrent calls to flush immMemTable" }
                return@withWriteLock
            } else {
                // we can safely remove the oldest immutableMemTable
                state.immutableMemTables.pollLast()
            }

            // TODO: this is only for no compaction strategy, will require additional logic when we support for diverse compaction algorithms
            state.l0Sstables.addFirst(flushedMemTableId)
            log.info { "Flushed $flushedMemTableId.sst with size ${sst.file.size}" }
            state.sstables[flushedMemTableId] = sst
        }

        if (options.enableWal) {
            TODO("remove wal file")
        }

        return flushedMemTableId
    }

    fun setManifest(manifest: Manifest?) {
        this.manifest = manifest
    }
}
