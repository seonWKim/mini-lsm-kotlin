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
    private val options: LsmStorageOptions,
    private val nextSstId: AtomicInteger,
    val blockCache: BlockCache,
    private var manifest: Manifest? = null,
    private val memTableLock: RwLock<Unit> = options.customizableMemTableLock,
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

    fun snapshot(timestamp: Long = 0L): LsmStorageStateSnapshot {
        return LsmStorageStateSnapshot(
            state = state,
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

    fun put(key: ComparableByteArray, value: MemtableValue) {
        memTableLock.withWriteLock {
            if (shouldFreezeMemTable()) {
                forceFreezeMemTable()
            }

            state.memTable.get().put(key, value)
        }
    }

    /**
     * Freeze memtable and return the previous memtable id.
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
        return state.memTable.get().approximateSize() > options.targetSstSize
    }

    fun shouldFlush(): Boolean {
        return state.immutableMemTables.size >= options.numMemTableLimit
    }

    /**
     * Flush immutable memTable to disk and return flushed memTable ID.
     */
    fun forceFlushNextImmutableMemTable(): Int {
        lateinit var flushMemTable: MemTable
        memTableLock.withReadLock {
            flushMemTable = state.immutableMemTables.lastOrNull()
                ?: throw IllegalStateException("No immutable memTables exist!")
        }

        val builder = SsTableBuilder(options.blockSize)
        flushMemTable.flush(builder)
        val sstId = flushMemTable.id
        val sst = builder.build(
            id = sstId,
            blockCache = blockCache.copy(),
            path = sstPath(path, sstId)
        )

        memTableLock.withWriteLock {
            val immutableMemTable: MemTable? = state.immutableMemTables.peekLast()
            if (immutableMemTable?.id != sstId) {
                // in case where forceFlushNextImmMemTable() is called concurrently, it can enter this block
                log.info { "Sst id($sstId) and immutable memTable id(${immutableMemTable?.id}) mismatch. There might be concurrent calls to flush immMemTable" }
                return@withWriteLock
            } else {
                // we can safely remove the oldest immutableMemTable
                state.immutableMemTables.pollLast()
            }

            // TODO: this is only for no compaction strategy, will require additional logic when we support for diverse compaction algorithms
            state.l0Sstables.addFirst(sstId)
            log.info { "Flushed $sstId.sst with size ${sst.file.size}" }
            state.sstables[sstId] = sst
        }

        return sstId
    }

    fun setManifest(manifest: Manifest?) {
        this.manifest = manifest
    }
}
