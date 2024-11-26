package org.github.seonwkim.lsm.storage

import com.google.common.annotations.VisibleForTesting
import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.memtable.isDeleted
import org.github.seonwkim.lsm.memtable.isValid
import org.github.seonwkim.lsm.sstable.*
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.io.path.exists

class LsmStorageInner private constructor(
    val path: Path,
    private val state: LsmStorageState,
    val options: LsmStorageOptions,
    private val nextSstId: AtomicInteger,
    val blockCache: BlockCache,
    private var manifest: Manifest? = null,
    private val compactionController: CompactionController,
) {
    private var memTableLock: RwLock<Unit> = DefaultRwLock(Unit)
    private var immutableMemTableLock: RwLock<Unit> = DefaultRwLock(Unit)
    private var sstableLock: RwLock<Unit> = DefaultRwLock(Unit)


    companion object {
        private val log = mu.KotlinLogging.logger { }

        fun open(
            path: Path,
            options: LsmStorageOptions
        ): LsmStorageInner {
            if (!path.exists()) {
                log.info { "Creating directories $path" }
                try {
                    Files.createDirectories(path)
                } catch (e: Exception) {
                    log.warn { "Errors creating DB dir: $e" }
                    throw e
                }
            }

            val compactionOptions = options.compactionOptions
            val compactionController: CompactionController = when (compactionOptions) {
                is Leveled -> {
                    LeveledCompactionController(compactionOptions.options)
                }

                is Tiered -> {
                    TieredCompactionController(compactionOptions.options)
                }

                is Simple -> {
                    SimpleCompactionController(compactionOptions.options)
                }

                is NoCompaction -> {
                    NoCompactionController
                }
            }

            val manifestPath = path.resolve("MANIFEST")
            lateinit var manifest: Manifest
            var lastCommitTs = 0L
            val state = LsmStorageState.create(options)
            val blockCache = BlockCache(1 shl 20)
            var nextSstId = 1
            if (!manifestPath.exists()) {
                if (options.enableWal) {
                    // TODO("create with WAL")
                }
                manifest = try {
                    Manifest.create(manifestPath)
                } catch (e: Exception) {
                    throw Error("Failed to create manifest file: $e")
                }
                manifest.addRecord(NewMemTable(state.memTable.get().id))
            } else {
                val (m, records) = Manifest.recover(manifestPath)
                val memTables = mutableSetOf<Int>()
                for (record in records) {
                    when (record) {
                        is Flush -> {
                            val sstId = record.sstId
                            val removed = memTables.remove(sstId)
                            if (!removed) {
                                throw Error("memtable of id $sstId not exists")
                            }
                            if (compactionController.flushTol0()) {
                                state.l0Sstables.addFirst(sstId)
                            } else {
                                state.levels.addFirst(SstLevel(sstId, mutableListOf(sstId)))
                            }

                            nextSstId = maxOf(nextSstId, sstId)
                        }

                        is NewMemTable -> {
                            nextSstId = maxOf(nextSstId, record.memTableId)
                            memTables.add(record.memTableId)
                        }

                        is Compaction -> {
                            TODO()
                        }
                    }
                }

                var sstCount = 0
                val totalSstableIds = state.l0Sstables + state.levels.flatMap { it.sstIds }
                for (sstableId in totalSstableIds) {
                    val sst = Sstable.open(
                        id = sstableId,
                        blockCache = blockCache.copy(),
                        file = SsTableFile.open(sstPath(path, sstableId))
                    )
                    lastCommitTs = maxOf(lastCommitTs, sst.maxTs)
                    state.sstables[sstableId] = sst
                    sstCount += 1
                }
                log.info { "$sstCount SSTs opened" }
                nextSstId++

                // For leveled compaction, sort SSTs on each level
                if (compactionController is LeveledCompactionController) {
                    val sstables = state.sstables
                    for ((_, ssts) in state.levels) {
                        ssts.sortWith { t1, t2 ->
                            sstables[t1]!!.firstKey.compareTo(sstables[t2]!!.firstKey)
                        }
                    }
                }

                // Recover memTables
                if (options.enableWal) {
                    TODO()
                } else {
                    state.memTable.set(MemTable.create(nextSstId))
                }
                m.addRecord(NewMemTable(state.memTable.get().id))
                nextSstId++
                manifest = m
            }

            return LsmStorageInner(
                path = path,
                state = state,
                options = options,
                nextSstId = AtomicInteger(nextSstId),
                blockCache = blockCache,
                compactionController = compactionController,
                manifest = manifest,
            )
        }

        fun openWithCustomLock(
            path: Path,
            options: LsmStorageOptions,
            memTableLock: RwLock<Unit> = DefaultRwLock(Unit),
            immutableMemTableLock: RwLock<Unit> = DefaultRwLock(Unit),
            sstableLock: RwLock<Unit> = DefaultRwLock(Unit)
        ): LsmStorageInner {
            val inner = open(path, options)
            inner.memTableLock = memTableLock
            inner.immutableMemTableLock = immutableMemTableLock
            inner.sstableLock = sstableLock
            return inner
        }
    }

    /**
     * Retrieves the value associated with the specified key from the storage.
     *
     * This method first attempts to retrieve the value from the current memTable.
     * If the key is not found or is marked as deleted, it then checks the immutable memTables.
     * If the key is still not found, it finally checks the SSTables.
     *
     * @param key The key to be retrieved. It must be a `ComparableByteArray`.
     * @return The value associated with the key, or `null` if the key is not found or is marked as deleted.
     */
    fun get(key: ComparableByteArray): ComparableByteArray? {
        return get(TimestampedKey(key))
    }

    /**
     * Retrieves the value associated with the specified timestamped key from the storage.
     *
     * This method first attempts to retrieve the value from the current memTable.
     * If the key is not found or is marked as deleted, it then checks the immutable memTables.
     * If the key is still not found, it finally checks the SSTables.
     *
     * @param key The timestamped key to be retrieved. It must be a `TimestampedKey`.
     * @return The value associated with the key, or `null` if the key is not found or is marked as deleted.
     */
    fun get(key: TimestampedKey): ComparableByteArray? {
        getFromMemTable(key).let {
            when {
                it.isValid() -> return it!!.value
                it.isDeleted() -> return null
                else -> {
                    // find from immutable memTables
                }
            }
        }

        getFromImmutableMemTables(key).let {
            when {
                it.isValid() -> return it!!.value
                it.isDeleted() -> return null
                else -> {
                    // find from l0 sstables
                }
            }
        }

        return getFromSstables(key)
    }

    private fun getFromMemTable(key: TimestampedKey): MemtableValue? {
        return state.memTable.get().get(key)
    }

    private fun getFromImmutableMemTables(key: TimestampedKey): MemtableValue? {
        val immutableMemTables = state.immutableMemTables
        for (immMemTable in immutableMemTables) {
            val result = immMemTable.get(key)
            if (result.isValid() || result.isDeleted()) {
                return result
            }
        }

        return null
    }

    private fun getFromSstables(key: TimestampedKey): ComparableByteArray? {
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

    /**
     * Scans the storage for key-value pairs within the specified bounds.
     *
     * This method creates iterators for the current memTable, immutable memTables, and SSTables at different levels.
     * It then merges these iterators to provide a unified view of the key-value pairs within the specified bounds.
     *
     * @param lower The lower bound of the scan. It must be a `Bound`.
     * @param upper The upper bound of the scan. It must be a `Bound`.
     * @return A `FusedIterator` that iterates over the key-value pairs within the specified bounds.
     */
    fun scan(lower: Bound, upper: Bound): FusedIterator {
        // memTable iterators
        val memTableIters =
            listOf(getMemTableIterator(lower, upper)) + getImmutableMemTablesIterator(lower, upper)
        val memTableMergeIter = MergeIterator(memTableIters)
        val l0MergeIter = MergeIterator(getL0SstablesIterator(lower, upper))
        val levelIters = getLevelIterators(lower, upper)
        val `memTableIter plus l0Iter` = TwoMergeIterator.create(memTableMergeIter, l0MergeIter)
        val `levelIter added iter` =
            TwoMergeIterator.create(`memTableIter plus l0Iter`, MergeIterator(levelIters))

        return FusedIterator(
            iter = LsmIterator.new(
                iter = `levelIter added iter`,
                endBound = upper
            )
        )
    }

    fun getMemTableIterator(lower: Bound, upper: Bound): MemTableIterator {
        return state.memTable.get().iterator(lower, upper)
    }

    fun getImmutableMemTablesIterator(lower: Bound, upper: Bound): List<MemTableIterator> {
        return state.immutableMemTables.map { it.iterator(lower, upper) }
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

    /**
     * Inserts a key-value pair into the current memTable.
     *
     * This method adds the specified key-value pair to the current memTable.
     *
     * @param key The key to be inserted. It must be a `ComparableByteArray` to ensure proper ordering.
     * @param value The value to be associated with the key. It must be a `ComparableByteArray`.
     */
    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        put(key, MemtableValue(value))
    }

    /**
     * Deletes a key from the current memTable.
     *
     * This method marks the specified key as deleted in the current memTable.
     *
     * @param key The key to be deleted. It must be a `ComparableByteArray`.
     */
    fun delete(key: ComparableByteArray) {
        put(key, MemtableValue(ComparableByteArray.new()))
    }

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

    private fun shouldFreezeMemTable(): Boolean {
        return memTableLock.withReadLock {
            state.memTable.get().approximateSize() > options.targetSstSize
        }
    }

    /**
     * Freezes the current memTable and creates a new one.
     *
     * This method forces the current memTable to be frozen and moved to the list of immutable memTables.
     * A new memTable is then created and set as the current memTable.
     */
    fun forceFreezeMemTable() {
        val newMemTableId = nextSstId.getAndIncrement()
        val memTable = if (options.enableWal) {
            TODO("should be implemented after WAL is supported")
        } else {
            MemTable.create(newMemTableId)
        }

        state.immutableMemTables.addFirst(state.memTable.get())
        state.memTable.set(memTable)

        manifest?.addRecord(NewMemTable(newMemTableId))
    }

    /**
     * Force flushes the earliest created immutable memTable to disk.
     *
     * This method flushes the oldest immutable memTable to disk, creating a new SSTable.
     * The method updates the manifest with the flushed memTable ID.
     *
     * @throws IllegalStateException if no immutable memTables exist.
     */
    fun forceFlushNextImmMemTable() {
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

        manifest?.addRecord(Flush(flushedMemTableId))
    }

    /**
     *
     */
    fun forceFullCompaction() {
        if (options.compactionOptions == NoCompaction) {
            throw Error("Compaction is not enabled")
        }

        val snapshot = snapshot()
        val compactionTask = ForceFullCompaction(
            l0Sstables = snapshot.l0Sstables,
            l1Sstables = snapshot.levels[0].sstIds
        )
        log.info { "Force full compaction: $compactionTask" }

        val sstables = compact(compactionTask)
        val ids = mutableListOf<Int>()


    }

    private fun snapshot(): LsmStorageState {
        return memTableLock.withReadLock {
            immutableMemTableLock.withReadLock {
                sstableLock.withReadLock {
                    LsmStorageState(
                        memTable = AtomicReference(state.memTable.get().copy()),
                        immutableMemTables = LinkedList(state.immutableMemTables),
                        l0Sstables = LinkedList(state.l0Sstables),
                        levels = LinkedList(state.levels),
                        sstables = ConcurrentHashMap(state.sstables)
                    )
                }
            }
        }
    }

    private fun compact(task: CompactionTask): List<Sstable> {
        TODO()
    }

    /**
     * Triggers a flush operation when the number of immutable memTables exceeds the configured limit.
     *
     * This method checks if the number of immutable memTables has exceeded the limit set in the configuration.
     * If the limit is exceeded, it forces a flush of the earliest created immutable memTable to disk.
     */
    fun triggerFlush() {
        if (state.immutableMemTables.size >= options.numMemTableLimit) {
            forceFlushNextImmMemTable()
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
