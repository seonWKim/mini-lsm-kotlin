package org.github.seonwkim.lsm.storage

import com.google.common.annotations.VisibleForTesting
import mu.KotlinLogging
import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.memtable.isDeleted
import org.github.seonwkim.lsm.memtable.isValid
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
     * Retrieves a value from the current memTable using the provided key.
     *
     * @param key The key to search for in the memTable.
     * @return The value associated with the key, or null if not found.
     */
    fun getFromMemTable(key: TimestampedKey): MemtableValue? {
        return state.memTable.get().get(key)
    }

    /**
     * Retrieves a value from the immutable memTables using the provided key.
     *
     * @param key The key to search for in the immutable memTables.
     * @return The value associated with the key, or null if not found.
     */
    fun getFromImmutableMemTables(key: TimestampedKey): MemtableValue? {
        val immutableMemTables = state.immutableMemTables
        for (immMemTable in immutableMemTables) {
            val result = immMemTable.get(key)
            if (result.isValid() || result.isDeleted()) {
                return result
            }
        }

        return null
    }

    /**
     * Retrieves a value from the SSTables using the provided key.
     *
     * @param key The key to search for in the SSTables.
     * @return The value associated with the key, or null if not found.
     */
    fun getFromSstables(key: TimestampedKey): ComparableByteArray? {
        val l0Iter = MergeIterator(getL0SsTableIterators(key))
        val levelIters = MergeIterator(getSstConcatIterator(key))
        val totalMergedIter = TwoMergeIterator.create(
            first = l0Iter,
            second = levelIters
        )

        if (totalMergedIter.isValid() &&
            totalMergedIter.key() == key.bytes && // TODO: timestamp comparison between keys
            !totalMergedIter.value().isEmpty()
        ) {
            return totalMergedIter.value()
        }

        return null
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

    fun getMemTableIterator(lower: Bound, upper: Bound): MemTableIterator {
        return state.memTable.get().iterator(lower, upper)
    }

    fun getImmutableMemTablesIterator(lower: Bound, upper: Bound): List<MemTableIterator> {
        return state.immutableMemTables.map { it.iterator(lower, upper) }
    }

    private fun getL0SsTableIterators(key: TimestampedKey): List<SsTableIterator> {
        return state.l0Sstables.mapNotNull { l0SstableIdx ->
            val table = state.sstables[l0SstableIdx]!!
            if (table.firstKey <= key && key <= table.lastKey) {
                SsTableIterator.createAndSeekToKey(table, key)
            } else {
                null
            }
        }
    }

    private fun getSstConcatIterator(key: TimestampedKey): List<SstConcatIterator> {
        return state.levels.map { level ->
            val validLevelSsts = mutableListOf<Sstable>()
            level.sstIds.forEach { levelSstId ->
                val table = state.sstables[levelSstId]!!
                if (keepTable(key, table)) {
                    validLevelSsts.add(table)
                }
            }
            SstConcatIterator.createAndSeekToKey(validLevelSsts, key)
        }
    }

    private fun keepTable(key: TimestampedKey, table: Sstable): Boolean {
        if (table.firstKey <= key && key <= table.lastKey) {
            if (table.bloom?.mayContain(farmHashFingerPrintU32(key.bytes)) == true) {
                return true
            }
        } else {
            return true
        }

        return false
    }

    fun getL0SstablesIterator(lower: Bound, upper: Bound): List<SsTableIterator> {
        return state.l0Sstables.mapNotNull { idx ->
            val table = state.sstables[idx]!!
            if (rangeOverlap(lower, upper, table.firstKey, table.lastKey)) {
                when (lower) {
                    is Included -> SsTableIterator.createAndSeekToKey(table, TimestampedKey(lower.key))
                    is Excluded -> {
                        val iter = SsTableIterator.createAndSeekToKey(table, TimestampedKey(lower.key))
                        if (iter.isValid() && iter.key() == lower.key) {
                            iter.next()
                        }
                        iter
                    }

                    is Unbounded -> SsTableIterator.createAndSeekToFirst(table)
                }
            } else null
        }
    }

    private fun rangeOverlap(
        userBegin: Bound,
        userEnd: Bound,
        tableBegin: TimestampedKey,
        tableEnd: TimestampedKey
    ): Boolean {
        when (userEnd) {
            is Excluded -> if (userEnd.key <= tableBegin.bytes) {
                return false
            }

            is Included -> if (userEnd.key < tableBegin.bytes) {
                return false
            }

            else -> {}
        }

        when (userBegin) {
            is Excluded -> if (userBegin.key >= tableEnd.bytes) {
                return false
            }

            is Included -> if (userBegin.key > tableEnd.bytes) {
                return false
            }

            else -> {}
        }

        return true
    }

    fun getLevelIterators(lower: Bound, upper: Bound): List<SstConcatIterator> {
        return state.levels.map { level ->
            val levelSsts = mutableListOf<Sstable>()
            level.sstIds.forEach {
                val sstable = state.sstables[it]!!
                if (rangeOverlap(
                        userBegin = lower,
                        userEnd = upper,
                        tableBegin = sstable.firstKey,
                        tableEnd = sstable.lastKey
                    )
                ) {
                    levelSsts.add(sstable)
                }
            }

            when (lower) {
                is Included -> {
                    SstConcatIterator.createAndSeekToKey(
                        sstables = levelSsts,
                        key = TimestampedKey(lower.key),
                    )
                }

                is Excluded -> {
                    val iter = SstConcatIterator.createAndSeekToKey(
                        sstables = levelSsts,
                        key = TimestampedKey(lower.key),
                    )
                    while (iter.isValid() && iter.key() == lower.key) {
                        iter.next()
                    }
                    iter
                }

                is Unbounded -> {
                    SstConcatIterator.createAndSeekToFirst(levelSsts)
                }
            }
        }
    }

    fun getImmutableMemTablesSize(): Int {
        return state.immutableMemTables.size
    }

    fun getImmutableMemTableApproximateSize(idx: Int): Int {
        return state.immutableMemTables.get(idx).approximateSize()
    }

    fun getL0SstablesSize(): Int {
        return state.sstables.size
    }

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

    fun nextSstId(): Int {
        return nextSstId.get()
    }

    fun setNextSstId(id: Int) {
        return nextSstId.set(id)
    }

    fun incrementNextSstId() {
        nextSstId.incrementAndGet()
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

    @VisibleForTesting
    fun addL0Sstable(sstId: Int) {
        state.l0Sstables.addFirst(sstId)
    }

    @VisibleForTesting
    fun addSstable(sst: Sstable) {
        state.sstables[sst.id] = sst
    }

    @VisibleForTesting
    fun constructMergeIterator(): MergeIterator<SsTableIterator> {
        val iters = mutableListOf<SsTableIterator>()
        state.l0Sstables.forEach {
            iters.add(SsTableIterator.createAndSeekToFirst(state.sstables[it]!!))
        }

        state.levels.forEach { level ->
            level.sstIds.forEach {
                iters.add(SsTableIterator.createAndSeekToFirst(state.sstables[it]!!))
            }
        }

        return MergeIterator(iters)
    }
}
