package org.github.seonwkim.lsm.storage

import com.google.common.annotations.VisibleForTesting
import org.github.seonwkim.common.*
import org.github.seonwkim.common.lock.MutexLock
import org.github.seonwkim.common.lock.RwLock
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.memtable.isDeleted
import org.github.seonwkim.lsm.memtable.isValid
import org.github.seonwkim.lsm.sstable.*
import org.github.seonwkim.lsm.storage.compaction.CompactionFilter
import org.github.seonwkim.lsm.storage.compaction.Prefix
import org.github.seonwkim.lsm.storage.compaction.controller.CompactionController
import org.github.seonwkim.lsm.storage.compaction.controller.LeveledCompactionController
import org.github.seonwkim.lsm.storage.compaction.option.NoCompaction
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask
import org.github.seonwkim.lsm.storage.compaction.task.ForceFullCompactionTask
import org.github.seonwkim.lsm.storage.compaction.task.SimpleLeveledCompactionTask
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.exists

class LsmStorageInner private constructor(
    val path: Path,
    val state: LsmStorageState,
    val options: LsmStorageOptions,
    val blockCache: BlockCache,
    private val nextSstId: AtomicInteger,
    private var manifest: Manifest? = null,
    private val compactionController: CompactionController,
    private val compactionFilters: RwLock<CompactionFilter>
) {
    companion object {
        private val log = mu.KotlinLogging.logger { }

        fun open(
            path: Path,
            options: LsmStorageOptions,
            customState: LsmStorageState? = null
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

            val state = customState ?: LsmStorageState.create(options)
            val compactionOptions = options.compactionOptions
            val compactionController = CompactionController.createCompactionController(compactionOptions)

            val manifestPath = path.resolve("MANIFEST")
            lateinit var manifest: Manifest
            var lastCommitTs = 0L
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
                manifest.addRecord(NewMemTable(state.memTable.read().id))
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
                            if (compactionController.flushToL0()) {
                                state.l0Sstables.read().addFirst(sstId)
                            } else {
                                state.levels.read().addFirst(SstLevel(sstId, mutableListOf(sstId)))
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
                val totalSstableIds = state.l0Sstables.read() + state.levels.read().flatMap { it.sstIds }
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
                    for ((_, ssts) in state.levels.read()) {
                        ssts.sortWith { t1, t2 ->
                            sstables[t1]!!.firstKey.compareTo(sstables[t2]!!.firstKey)
                        }
                    }
                }

                // Recover memTables
                if (options.enableWal) {
                    TODO()
                } else {
                    state.setMemTable(MemTable.create(nextSstId))
                }
                m.addRecord(NewMemTable(state.memTable.read().id))
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
                compactionFilters = MutexLock(Prefix(ComparableByteArray.new()))
            )
        }

        fun openWithCustomState(
            path: Path,
            options: LsmStorageOptions,
            customState: LsmStorageState
        ): LsmStorageInner {
            val inner = open(
                path = path,
                options = options,
                customState = customState
            )
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
        return state.memTable.withReadLock { it.get(key) }
    }

    private fun getFromImmutableMemTables(key: TimestampedKey): MemtableValue? {
        return state.immutableMemTables.withReadLock {
            for (immMemTable in it) {
                val result = immMemTable.get(key)
                if (result.isValid() || result.isDeleted()) {
                    return@withReadLock result
                }
            }

            return@withReadLock null
        }
    }

    private fun getFromSstables(key: TimestampedKey): ComparableByteArray? {
        val l0Iter = MergeIterator(getL0SsTableIterators(key))
        val levelIters = MergeIterator(getSstConcatIterator(key))
        val totalMergedIter = TwoMergeIterator.create(
            first = l0Iter, second = levelIters
        )

        if (totalMergedIter.isValid() && totalMergedIter.key() == key.bytes && // TODO: timestamp comparison between keys
            !totalMergedIter.value().isEmpty()
        ) {
            return totalMergedIter.value()
        }

        return null
    }

    private fun getL0SsTableIterators(key: TimestampedKey): List<SsTableIterator> {
        return state.l0Sstables.withReadLock {
            it.mapNotNull { l0SstableIdx ->
                val table = state.sstables[l0SstableIdx]!!
                if (table.firstKey <= key && key <= table.lastKey) {
                    SsTableIterator.createAndSeekToKey(table, key)
                } else {
                    null
                }
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
                iter = `levelIter added iter`, endBound = upper
            )
        )
    }

    private fun getMemTableIterator(lower: Bound, upper: Bound): MemTableIterator {
        return state.memTable.withReadLock { it.iterator(lower, upper) }
    }

    private fun getImmutableMemTablesIterator(lower: Bound, upper: Bound): List<MemTableIterator> {
        return state.immutableMemTables.withReadLock { immutableMemTables ->
            immutableMemTables.map { it.iterator(lower, upper) }
        }
    }

    private fun getL0SstablesIterator(lower: Bound, upper: Bound): List<SsTableIterator> {
        return state.l0Sstables.withReadLock {
            it.mapNotNull { idx ->
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
    }

    private fun rangeOverlap(
        userBegin: Bound, userEnd: Bound, tableBegin: TimestampedKey, tableEnd: TimestampedKey
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
        return state.levels.read().map { level ->
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
        return state.levels.read().map { level ->
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
            if (table.bloom == null) {
                return true
            }

            // we can assure that key doesn't exist when bloom filter doesn't have the key
            return table.bloom.mayContain(farmHashFingerPrintU32(key.bytes))
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
            forceFreezeMemTable(enableCheck = true)
        }

        state.memTable.withReadLock { it.put(key, value) }
    }

    private fun shouldFreezeMemTable(): Boolean {
        return state.memTable.withReadLock {
            it.approximateSize() > options.targetSstSize
        }
    }

    /**
     * Freezes the current memTable and creates a new one.
     *
     * This method forces the current memTable to be frozen and moved to the list of immutable memTables.
     * A new memTable is then created and set as the current memTable.
     */
    fun forceFreezeMemTable(enableCheck: Boolean = false) {
        state.memTable.withWriteLock { prevMemTable ->
            if (enableCheck) {
                if (!shouldFreezeMemTable()) return@withWriteLock
            }

            val newMemTableId = nextSstId.getAndIncrement()
            val newMemTable = if (options.enableWal) {
                TODO("should be implemented after WAL is supported")
            } else {
                MemTable.create(newMemTableId)
            }

            state.immutableMemTables.withWriteLock {
                it.addFirst(prevMemTable)
            }
            state.setMemTable(newMemTable)

            manifest?.addRecord(NewMemTable(newMemTableId))
        }
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
        var flushTargetMemTable: MemTable? = null
        state.immutableMemTables.withReadLock {
            flushTargetMemTable = it.lastOrNull()
        }
        if (flushTargetMemTable == null) {
            throw IllegalStateException("No immutable memTables exist!")
        }

        val builder = SsTableBuilder(options.blockSize)
        flushTargetMemTable!!.flush(builder)
        val flushedMemTableId = flushTargetMemTable!!.id
        val sst = builder.build(
            id = flushedMemTableId, blockCache = blockCache.copy(), path = sstPath(path, flushedMemTableId)
        )

        state.immutableMemTables.withWriteLock {
            val immutableMemTable: MemTable? = it.peekLast()
            if (immutableMemTable?.id != flushedMemTableId) {
                // in case where forceFlushNextImmMemTable() is called concurrently, it can enter this block
                log.info { "Sst id($flushedMemTableId) and immutable memTable id(${immutableMemTable?.id}) mismatch. There might be concurrent calls to flush immMemTable" }
                return@withWriteLock
            } else {
                // we can safely remove the oldest immutableMemTable
                it.pollLast()
            }

            if (compactionController.flushToL0()) {
                state.l0Sstables.withWriteLock { l0SsTables ->
                    l0SsTables.addFirst(flushedMemTableId)
                }
            } else {
                state.levels.withWriteLock { levels ->
                    levels.addFirst(SstLevel(flushedMemTableId, mutableListOf(flushedMemTableId)))
                }
            }
            log.info { "Flushed $flushedMemTableId.sst with size ${sst.file.size}" }
            state.sstables[flushedMemTableId] = sst
        }

        if (options.enableWal) {
            TODO("remove wal file")
        }

        manifest?.addRecord(Flush(flushedMemTableId))
    }

    fun forceFullCompaction() {
        if (options.compactionOptions != NoCompaction) {
            throw Error("Force compaction can only be called with NoCompaction option")
        }

        lateinit var l0SstablesSnapshot: LinkedList<Int>
        lateinit var l1SstablesSnapshot: MutableList<Int>

        // We should run compaction without blocking l0 flush
        state.l0Sstables.withReadLock { l0Sstables ->
            state.levels.withReadLock { levels ->
                l0SstablesSnapshot = l0Sstables
                l1SstablesSnapshot = levels[0].sstIds
            }
        }
        val compactionTask = ForceFullCompactionTask(
            l0Sstables = l0SstablesSnapshot,
            l1Sstables = l1SstablesSnapshot
        )
        log.info { "Force full compaction: $compactionTask" }

        val newSstableIds = mutableListOf<Int>()
        val newSstables = compact(compactionTask)
        state.l0Sstables.withWriteLock { l0Sstables ->
            state.levels.withWriteLock { levels ->
                state
                // Removing the old sstables
                (l0SstablesSnapshot + l1SstablesSnapshot).forEach { sstId ->
                    state.sstables.remove(sstId)
                }

                newSstables.forEach { newSst ->
                    newSstableIds.add(newSst.id)
                    state.sstables[newSst.id] = newSst
                }
                // TODO: add assertions to check whether l1Ss

                // Add newly created sstable IDs to levels
                levels.let {
                    it[0].sstIds.clear()
                    it[0].sstIds.addAll(newSstableIds)
                }

                val compactedL0SstableIds = l0SstablesSnapshot.toHashSet()
                l0Sstables.removeAll { sstId ->
                    val shouldRemove = compactedL0SstableIds.contains(sstId)
                    compactedL0SstableIds.remove(sstId)
                    shouldRemove
                }
                if (compactedL0SstableIds.isNotEmpty()) {
                    throw Error("compactedL0SstableIds($compactedL0SstableIds) should be empty after iterating l0Sstables!!")
                }

                manifest?.addRecord(Compaction(compactionTask, newSstableIds))
            }
        }

        log.info { "Force full compaction done, new SSTs: $newSstableIds" }
    }

    private fun compact(task: CompactionTask): List<Sstable> {
        return when (task) {
            is ForceFullCompactionTask -> {
                val l0Iters = task.l0Sstables.mapNotNull { l0SstableId ->
                    state.sstables[l0SstableId]?.let { SsTableIterator.createAndSeekToFirst(it) }
                }

                val l1Iters = task.l1Sstables.mapNotNull { l1SstableId ->
                    state.sstables[l1SstableId]
                }

                val iter = TwoMergeIterator.create(
                    first = MergeIterator(l0Iters),
                    second = SstConcatIterator.createAndSeekToFirst(l1Iters)
                )
                compactGenerateSstFromIter(iter, task.compactToBottomLevel())
            }

            is SimpleLeveledCompactionTask -> {
                if (task.l0Compaction()) {
                    val upperIters = mutableListOf<SsTableIterator>()
                    for (id in task.upperLevelSstIds) {
                        state.sstables[id]?.let { SsTableIterator.createAndSeekToFirst(it) }
                            ?.let { upperIters.add(it) }
                    }
                    val upperIter = MergeIterator(upperIters)
                    val lowerSsts = mutableListOf<Sstable>()
                    for (id in task.lowerLevelSstIds) {
                        state.sstables[id]?.let { lowerSsts.add(it) }
                    }
                    val lowerIter = SstConcatIterator.createAndSeekToFirst(lowerSsts)
                    compactGenerateSstFromIter(
                        iter = TwoMergeIterator.create(upperIter, lowerIter),
                        compactToBottomLevel = task.compactToBottomLevel()
                    )
                } else {
                    val upperSsts = mutableListOf<Sstable>()
                    for (id in task.upperLevelSstIds) {
                        state.sstables[id]?.let { upperSsts.add(it) }
                    }
                    val upperIter = SstConcatIterator.createAndSeekToFirst(upperSsts)
                    val lowerSsts = mutableListOf<Sstable>()
                    for (id in task.lowerLevelSstIds) {
                        state.sstables[id]?.let { lowerSsts.add(it) }
                    }
                    val lowerIter = SstConcatIterator.createAndSeekToFirst(lowerSsts)
                    compactGenerateSstFromIter(
                        iter = TwoMergeIterator.create(upperIter, lowerIter),
                        compactToBottomLevel = task.compactToBottomLevel()
                    )
                }
            }

            else -> {
                TODO()
            }
        }
    }

    // TODO: Add support for timestamp comparison
    private fun compactGenerateSstFromIter(
        iter: StorageIterator,
        compactToBottomLevel: Boolean
    ): List<Sstable> {
        var builder: SsTableBuilder? = null
        val newSst = mutableListOf<Sstable>()
        while (iter.isValid()) {
            if (builder == null) {
                builder = SsTableBuilder(blockSize = options.blockSize)
            }

            if (compactToBottomLevel) {
                if (!iter.value().isEmpty()) {
                    builder.add(TimestampedKey(iter.key()), iter.value())
                }
            } else {
                builder.add(TimestampedKey(iter.key()), iter.value())
            }
            iter.next()

            if (builder.estimatedSize() >= options.targetSstSize) {
                val sstId = nextSstId.getAndIncrement()
                newSst.add(
                    builder.build(
                        id = sstId,
                        blockCache = blockCache.copy(),
                        sstPath(path = path, id = sstId)
                    )
                )
                builder = null
            }
        }

        if (builder != null) {
            val sstId = nextSstId.getAndIncrement()
            val sst = builder.build(
                id = sstId,
                blockCache = blockCache.copy(),
                path = sstPath(path = path, id = sstId)
            )
            newSst.add(sst)
        }

        return newSst
    }

    /**
     * Triggers a flush operation when the number of immutable memTables exceeds the configured limit.
     *
     * This method checks if the number of immutable memTables has exceeded the limit set in the configuration.
     * If the limit is exceeded, it forces a flush of the earliest created immutable memTable to disk.
     */
    fun triggerFlush() {
        val shouldFlush = state.immutableMemTables.withReadLock {
            it.size >= options.numMemTableLimit
        }

        if (shouldFlush) {
            forceFlushNextImmMemTable()
        }
    }

    /**
     * Ignore concurrent cases because this will be executed by single threaded scheduler. If not, we should
     * rewrite the code to consider concurrent operations.
     */
    fun triggerCompaction() {
        val snapshotForCompaction = state.diskSnapshot()
        val task = compactionController.generateCompactionTask(snapshotForCompaction) ?: return
        dumpStructure(snapshotForCompaction)

        log.info { "Running compaction task: $task" }
        val newSstables = compact(task)
        val newSstIds = mutableListOf<Int>()
        newSstables.forEach { newSstable ->
            newSstIds.add(newSstable.id)
            state.sstables[newSstable.id] = newSstable
        }

        // changes are applied to the snapshot
        val (snapshot, sstIdsToRemove) =
            compactionController.applyCompactionResult(snapshotForCompaction, task, newSstIds, false)
        log.info { "Compaction applied snapshot: $snapshot" }
        val sstablesToRemove = mutableListOf<Sstable>()
        for (sstId in sstIdsToRemove) {
            val sstable = state.sstables.remove(sstId) ?: throw Error("Sstable(id: $sstId) doesn't exist!!")
            sstablesToRemove.add(sstable)
        }

        state.l0Sstables.withWriteLock { l0sstables ->
            while (l0sstables.isNotEmpty()) {
                if (!sstIdsToRemove.contains(l0sstables.last())) {
                    break
                }
                l0sstables.removeLast()
            }
        }
        state.levels.withWriteLock { levels ->
            levels.clear()
            levels.addAll(snapshot.levels)
        }
        manifest?.addRecord(Compaction(task = task, output = newSstIds))
        log.info { "Compaction finished: ${sstIdsToRemove.size} files removed, ${newSstIds.size} files added, newSstIds=${newSstIds}, oldSstIDs=${sstIdsToRemove}" }
        sstablesToRemove.forEach { sst ->
            Files.delete(sstPath(path = path, id = sst.id))
        }
    }

    fun getImmutableMemTablesSize(): Int {
        return state.immutableMemTables.withReadLock { it.size }
    }

    fun getImmutableMemTableApproximateSize(idx: Int): Int {
        return state.immutableMemTables.withReadLock { it[idx].approximateSize() }
    }

    fun getL0SstablesSize(): Int {
        return state.sstables.size
    }

    @VisibleForTesting
    fun addL0Sstable(sstId: Int) {
        state.l0Sstables.read().addFirst(sstId)
    }

    @VisibleForTesting
    fun addSstable(sst: Sstable) {
        state.sstables[sst.id] = sst
    }

    @VisibleForTesting
    fun constructMergeIterator(): MergeIterator<SsTableIterator> {
        val iters = mutableListOf<SsTableIterator>()
        state.l0Sstables.read().forEach {
            iters.add(SsTableIterator.createAndSeekToFirst(state.sstables[it]!!))
        }

        state.levels.read().forEach { level ->
            level.sstIds.forEach {
                iters.add(SsTableIterator.createAndSeekToFirst(state.sstables[it]!!))
            }
        }

        return MergeIterator(iters)
    }

    fun dumpStructure() {
        dumpStructure(state.diskSnapshot())
    }

    private fun dumpStructure(snapshot: LsmStorageSstableSnapshot) {
        log.debug { "Dumping l0sstables and levels" }
        if (snapshot.l0Sstables.isNotEmpty()) {
            log.info { "L0 (${state.l0Sstables.read().size}): ${state.l0Sstables.read()}" }
        }

        for ((level, files) in snapshot.levels) {
            log.info { "L${level} (${files.size}): $files" }
        }
    }
}
