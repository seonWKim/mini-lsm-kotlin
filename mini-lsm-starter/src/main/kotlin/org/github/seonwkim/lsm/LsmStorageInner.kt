package org.github.seonwkim.lsm

import com.google.common.annotations.VisibleForTesting
import org.github.seonwkim.common.*
import org.github.seonwkim.common.lock.MutexLock
import org.github.seonwkim.common.lock.RwLock
import org.github.seonwkim.lsm.compaction.CompactionFilter
import org.github.seonwkim.lsm.compaction.Prefix
import org.github.seonwkim.lsm.compaction.controller.CompactionController
import org.github.seonwkim.lsm.compaction.controller.LeveledCompactionController
import org.github.seonwkim.lsm.compaction.option.NoCompaction
import org.github.seonwkim.lsm.compaction.task.*
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.manifest.CompactionRecord
import org.github.seonwkim.lsm.manifest.FlushRecord
import org.github.seonwkim.lsm.manifest.Manifest
import org.github.seonwkim.lsm.manifest.NewMemTableRecord
import org.github.seonwkim.lsm.memtable.MemTable

import org.github.seonwkim.lsm.sstable.*
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
                log.debug { "Creating directories $path" }
                try {
                    Files.createDirectories(path)
                } catch (e: Exception) {
                    log.warn { "Errors creating DB dir: $e" }
                    throw e
                }
            }

            var state = customState ?: LsmStorageState.create(options)
            val compactionOptions = options.compactionOptions
            val compactionController =
                CompactionController.createCompactionController(
                    compactionOptions
                )

            val manifestPath = path.resolve("MANIFEST")
            lateinit var manifest: Manifest
            var lastCommitTs = 0L
            val blockCache = BlockCache(1 shl 20)
            var nextSstId = 0
            if (!manifestPath.exists()) {
                if (options.enableWal) {
                    val walAppliedMemTable = MemTable.createWithWal(
                        id = state.memTable.readValue().id(),
                        path = walPath(path, state.memTable.readValue().id())
                    )
                    state.memTable.replace(walAppliedMemTable)
                }
                manifest = try {
                    Manifest.create(manifestPath)
                } catch (e: Exception) {
                    throw Error("Failed to create manifest file: $e")
                }
                manifest.addRecord(NewMemTableRecord(state.memTable.readValue().id()))
            } else {
                val (m, records) = Manifest.recover(manifestPath)
                val memTables = mutableSetOf<Int>()
                for (record in records) {
                    when (record) {
                        is FlushRecord -> {
                            val sstId = record.sstId
                            val removed = memTables.remove(sstId)
                            if (!removed) {
                                throw Error("memtable of id $sstId not exists")
                            }
                            if (compactionController.flushToL0()) {
                                state.l0Sstables.readValue().addFirst(sstId)
                            } else {
                                state.levels.readValue().addFirst(SstLevel(sstId, mutableListOf(sstId)))
                            }

                            nextSstId = maxOf(nextSstId, sstId)
                        }

                        is NewMemTableRecord -> {
                            nextSstId = maxOf(nextSstId, record.memTableId)
                            memTables.add(record.memTableId)
                        }

                        is CompactionRecord -> {
                            val (snapshot, _) = compactionController.applyCompaction(
                                snapshot = state.diskSnapshot(),
                                sstables = state.sstables, // we don't actually need sstables in recovery mode
                                task = record.task,
                                newSstIds = record.output,
                                inRecovery = true
                            )
                            state = LsmStorageState.create(
                                memTable = state.memTable.readValue(),
                                immutableMemTables = state.immutableMemTables.readValue(),
                                l0Sstables = LinkedList(snapshot.l0SstableIds),
                                levels = LinkedList(snapshot.levels),
                                sstables = state.sstables
                            )
                            nextSstId = maxOf(nextSstId, record.output.maxOrNull() ?: 1)
                        }
                    }
                }

                var sstCount = 0
                val totalSstableIds =
                    state.l0Sstables.readValue() + state.levels.readValue().flatMap { it.sstIds }
                for (sstableId in totalSstableIds) {
                    val sst = Sstable.open(
                        id = sstableId,
                        blockCache = blockCache.copy(),
                        file = SsTableFile.open(sstPath(path, sstableId))
                    )
                    // lastCommitTs = maxOf(lastCommitTs, sst.maxTs)
                    state.sstables[sstableId] = sst
                    sstCount += 1
                }
                log.debug { "$sstCount SSTs opened" }
                nextSstId++

                // For leveled compaction, sort SSTs on each level
                if (compactionController is LeveledCompactionController) {
                    val sstables = state.sstables
                    for ((_, ssts) in state.levels.readValue()) {
                        ssts.sortWith { t1, t2 ->
                            sstables[t1]!!.firstKey.compareTo(sstables[t2]!!.firstKey)
                        }
                    }
                }

                // Recover memTables
                if (options.enableWal) {
                    var walCount = 0
                    for (id in memTables) {
                        val memTable = MemTable.recoverFromWal(id, walPath(path, id))
                        if (!memTable.isEmpty()) {
                            state.immutableMemTables.readValue().addFirst(memTable)
                            walCount++
                        }
                    }

                    log.info { "$walCount WALs recovered" }
                    state.memTable.replace(MemTable.createWithWal(nextSstId, walPath(path, nextSstId)))
                } else {
                    state.memTable.replace(MemTable.create(nextSstId))
                }
                m.addRecord(NewMemTableRecord(state.memTable.readValue().id()))
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
     * @param key The key to be retrieved.
     * @return The value associated with the key, or `null` if the key is not found or is marked as deleted.
     */
    fun get(key: ComparableByteArray): ComparableByteArray? {
        getFromMemTable(key).let {
            when {
                it.isValid() -> return it!!
                it.isDeleted() -> return null
                else -> {
                    // find from immutable memTables
                }
            }
        }

        getFromImmutableMemTables(key).let {
            when {
                it.isValid() -> return it!!
                it.isDeleted() -> return null
                else -> {
                    // find from l0 sstables
                }
            }
        }

        return getFromSstables(key)
    }

    private fun getFromMemTable(key: ComparableByteArray): ComparableByteArray? {
        return state.memTable.withReadLock { it.get(key) }
    }

    private fun getFromImmutableMemTables(key: ComparableByteArray): ComparableByteArray? {
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

    private fun getFromSstables(key: ComparableByteArray): ComparableByteArray? {
        val l0Iter = MergeIterator(getL0SsTableIterators(key))
        val levelIters = MergeIterator(getSstConcatIterator(key))
        val totalMergedIter = TwoMergeIterator.create(
            first = l0Iter, second = levelIters
        )

        if (totalMergedIter.isValid() &&
            totalMergedIter.key() == key &&
            !totalMergedIter.value().isEmpty()
        ) {
            return totalMergedIter.value()
        }

        return null
    }

    private fun getL0SsTableIterators(key: ComparableByteArray): List<SsTableIterator> {
        return state.l0Sstables.withReadLock {
            it.mapNotNull { l0SstableIdx ->
                val table = state.sstables[l0SstableIdx] ?: return@mapNotNull null
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
                        is Included -> SsTableIterator.createAndSeekToKey(table, lower.key)
                        is Excluded -> {
                            val iter = SsTableIterator.createAndSeekToKey(table, lower.key)
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
        userBegin: Bound,
        userEnd: Bound,
        tableBegin: ComparableByteArray,
        tableEnd: ComparableByteArray
    ): Boolean {
        when (userEnd) {
            is Excluded -> if (userEnd.key <= tableBegin) {
                return false
            }

            is Included -> if (userEnd.key < tableBegin) {
                return false
            }

            else -> {}
        }

        when (userBegin) {
            is Excluded -> if (userBegin.key >= tableEnd) {
                return false
            }

            is Included -> if (userBegin.key > tableEnd) {
                return false
            }

            else -> {}
        }

        return true
    }

    fun getLevelIterators(lower: Bound, upper: Bound): List<SstConcatIterator> {
        return state.levels.readValue().map { level ->
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
                        key = lower.key,
                    )
                }

                is Excluded -> {
                    val iter = SstConcatIterator.createAndSeekToKey(
                        sstables = levelSsts,
                        key = lower.key,
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

    private fun getSstConcatIterator(key: ComparableByteArray): List<SstConcatIterator> {
        return state.levels.readValue().map { level ->
            val validLevelSsts = mutableListOf<Sstable>()
            level.sstIds.forEach { levelSstId ->
                state.sstables[levelSstId]?.let {
                    if (keepTable(key, it)) {
                        validLevelSsts.add(it)
                    }
                }
            }
            SstConcatIterator.createAndSeekToKey(validLevelSsts, key)
        }
    }

    private fun keepTable(key: ComparableByteArray, table: Sstable): Boolean {
        if (table.firstKey <= key && key <= table.lastKey) {
            if (table.bloom == null) {
                return true
            }

            // we can assure that key doesn't exist when bloom filter doesn't have the key
            return table.bloom.mayContain(farmHashFingerPrintU32(key))
        }

        return false
    }

    /**
     * Inserts a key-value pair into the current memTable.
     *
     * This method adds the specified key-value pair to the current memTable.
     *
     * @param key The key to be inserted. It must be a [ComparableByteArray] to ensure proper ordering.
     * @param value The value to be associated with the key. It must be a [ComparableByteArray].
     */
    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        writeBatch(listOf(Put(key, value)))
    }

    /**
     * Deletes a key from the current memTable.
     *
     * This method marks the specified key as deleted in the current memTable.
     *
     * @param key The key to be deleted. It must be a [ComparableByteArray].
     */
    fun delete(key: ComparableByteArray) {
        writeBatch(listOf(Delete(key)))
    }

    fun writeBatch(batch: List<WriteBatchRecord>) {
        for (record in batch) {
            when (record) {
                is Delete -> {
                    val key = record.key
                    if (key.isEmpty()) {
                        throw IllegalArgumentException("key cannot be empty")
                    }

                    var shouldFreezeMemTable = false
                    state.memTable.withReadLock { memTable ->
                        memTable.put(key, "".toComparableByteArray())
                        shouldFreezeMemTable = memTable.approximateSize() >= options.targetSstSize
                    }

                    if (shouldFreezeMemTable) {
                        forceFreezeMemTableWithLock(true)
                    }
                }

                is Put -> {
                    val key = record.key
                    val value = record.value
                    if (key.isEmpty()) {
                        throw IllegalArgumentException("key cannot be empty")
                    }
                    if (value.isEmpty()) {
                        throw IllegalArgumentException("value cannot be empty")
                    }

                    var shouldFreezeMemTable = false
                    state.memTable.withReadLock { memTable ->
                        memTable.put(key, value)
                        shouldFreezeMemTable = memTable.approximateSize() >= options.targetSstSize
                    }

                    if (shouldFreezeMemTable) {
                        forceFreezeMemTableWithLock(true)
                    }
                }
            }
        }
    }

    /**
     * Freezes the current memTable and creates a new one by acquiring write lock.
     *
     * This method forces the current memTable to be frozen and moved to the list of immutable memTables.
     * A new memTable is then created and set as the current memTable.
     */
    fun forceFreezeMemTableWithLock(enableCheck: Boolean = false) {
        state.memTable.withWriteLock {
            if (enableCheck) {
                if (it.approximateSize() < options.targetSstSize) return@withWriteLock
            }

            forceFreezeMemTable()
        }
    }

    private fun forceFreezeMemTable() {
        val newMemTableId = getNextSstId()
        val newMemTable = if (options.enableWal) {
            MemTable.createWithWal(
                id = newMemTableId,
                path = walPath(path = path, id = newMemTableId)
            )
        } else {
            MemTable.create(newMemTableId)
        }

        switchMemTable(newMemTable)

        manifest?.addRecord(NewMemTableRecord(newMemTableId))
    }

    fun switchMemTable(newMemTable: MemTable) {
        val oldMemTable = state.memTable.replace(newMemTable)
        state.immutableMemTables.withWriteLock {
            it.addFirst(oldMemTable)
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
        val flushTargetMemTable: MemTable? = state.immutableMemTables.withReadLock {
            it.lastOrNull()
        }
        if (flushTargetMemTable == null) {
            log.debug { "No immutable memTables to flush" }
            return
        }

        val builder = SsTableBuilder(options.blockSize)
        flushTargetMemTable.flush(builder)
        val flushedMemTableId = flushTargetMemTable.id()
        val sst = builder.build(
            id = flushedMemTableId, blockCache = blockCache.copy(), path = sstPath(path, flushedMemTableId)
        )

        state.immutableMemTables.withWriteLock {
            val immutableMemTable = it.peekLast()
            if (immutableMemTable?.id() != flushedMemTableId) {
                // in case where forceFlushNextImmMemTable() is called concurrently, it can enter this block
                log.debug { "Sst id($flushedMemTableId) and immutable memTable id(${immutableMemTable?.id()}) mismatch. There might be concurrent calls to flush immMemTable" }
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
            state.sstables[flushedMemTableId] = sst
            log.debug { "Flushed $flushedMemTableId.sst with size ${sst.file.size}" }

            if (options.enableWal) {
                Files.delete(walPath(path = path, id = sst.id))
            }

            manifest?.addRecord(FlushRecord(flushedMemTableId))
        }
    }

    fun forceFullCompaction() {
        if (options.compactionOptions != NoCompaction) {
            throw Error("Force compaction can only be called with NoCompaction option")
        }

        val (l0SstablesSnapshot, lNSstablesSnapshot) = state.diskSnapshot()
        val l1SstablesSnapshot = lNSstablesSnapshot[0].sstIds

        // We should run compaction without blocking l0 flush
        val compactionTask = ForceFullCompactionTask(
            l0Sstables = l0SstablesSnapshot,
            l1Sstables = l1SstablesSnapshot
        )
        log.debug { "Force full compaction: $compactionTask" }

        val newSstableIds = mutableListOf<Int>()
        val newSstables = compact(compactionTask)
        state.l0Sstables.withWriteLock { l0Sstables ->
            state.levels.withWriteLock { levels ->
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

                manifest?.addRecord(CompactionRecord(compactionTask, newSstableIds))
            }
        }

        log.debug { "Force full compaction done, new SSTs: $newSstableIds" }
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

            is TieredCompactionTask -> {
                val iters = mutableListOf<SstConcatIterator>()
                for ((_, tierSstIds) in task.tiers) {
                    val ssts = tierSstIds.mapNotNull { state.sstables[it] }
                    iters.add(SstConcatIterator.createAndSeekToFirst(ssts))
                }
                compactGenerateSstFromIter(
                    iter = MergeIterator(iters),
                    compactToBottomLevel = task.compactToBottomLevel()
                )
            }

            is LeveledCompactionTask -> {
                if (task.upperLevel != null) {
                    val upperSsts = mutableListOf<Sstable>()
                    for (id in task.upperLevelSstIds) {
                        upperSsts.add(state.sstables[id]!!)
                    }
                    val upperIter = SstConcatIterator.createAndSeekToFirst(upperSsts)
                    val lowerSsts = mutableListOf<Sstable>()
                    for (id in task.lowerLevelSstIds) {
                        lowerSsts.add(state.sstables[id]!!)
                    }
                    val lowerIter = SstConcatIterator.createAndSeekToFirst(lowerSsts)
                    compactGenerateSstFromIter(
                        iter = TwoMergeIterator.create(upperIter, lowerIter),
                        compactToBottomLevel = task.compactToBottomLevel()
                    )
                } else {
                    val upperIters = mutableListOf<SsTableIterator>()
                    for (id in task.upperLevelSstIds) {
                        upperIters.add(SsTableIterator.createAndSeekToFirst(state.sstables[id]!!))
                    }
                    val upperIter = MergeIterator(upperIters)

                    val lowerSsts = mutableListOf<Sstable>()
                    for (id in task.lowerLevelSstIds) {
                        lowerSsts.add(state.sstables[id]!!)
                    }
                    val lowerIter = SstConcatIterator.createAndSeekToFirst(lowerSsts)
                    compactGenerateSstFromIter(
                        iter = TwoMergeIterator.create(upperIter, lowerIter),
                        compactToBottomLevel = task.compactToBottomLevel()
                    )
                }
            }
        }
    }

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
                    builder.add(iter.key(), iter.value())
                }
            } else {
                builder.add(iter.key(), iter.value())
            }
            iter.next()

            if (builder.estimatedSize() >= options.targetSstSize) {
                val sstId = getNextSstId()
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
            val sstId = getNextSstId()
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
        if (!options.enableFlush) {
            return
        }

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
        if (!options.enableCompaction) {
            return
        }

        val snapshotForCompaction = state.diskSnapshot()
        val task = compactionController.generateCompactionTask(
            snapshot = snapshotForCompaction,
            sstables = state.sstables
        ) ?: return
        if (log.isDebugEnabled) {
            dumpStructure(snapshotForCompaction)
        }

        log.debug { "Running compaction task: $task" }
        val newSstables = compact(task)
        val newSstIds = mutableListOf<Int>()
        newSstables.forEach { newSstable ->
            newSstIds.add(newSstable.id)
            state.sstables[newSstable.id] = newSstable
        }

        // changes are applied to the snapshot
        val (compactionAppliedSnapshot, sstIdsToRemove, l0Compacted) =
            compactionController.applyCompaction(
                snapshot = snapshotForCompaction,
                sstables = state.sstables,
                task = task,
                newSstIds = newSstIds,
                inRecovery = false
            )
        log.debug { "Compaction applied snapshot: $compactionAppliedSnapshot" }
        val sstablesToRemove = hashSetOf<Sstable>()
        for (sstId in sstIdsToRemove) {
            val sstable = state.sstables.remove(sstId) ?: throw Error("Sstable(id: $sstId) doesn't exist!!")
            sstablesToRemove.add(sstable)
        }

        // TODO: should we extract it to a method such as compactionController.updateL0Sstables() ?

        if (l0Compacted) {
            state.l0Sstables.withWriteLock { l0sstables ->
                state.levels.withWriteLock { levels ->
                    l0sstables.removeIf { sstIdsToRemove.contains(it) }
                    compactionController.updateLevels(
                        levels = levels,
                        task = task,
                        snapshot = compactionAppliedSnapshot
                    )
                }
            }
        } else {
            state.levels.withWriteLock { levels ->
                compactionController.updateLevels(
                    levels = levels,
                    task = task,
                    snapshot = compactionAppliedSnapshot
                )
            }
        }

        manifest?.addRecord(CompactionRecord(task = task, output = newSstIds))
        log.debug { "Compaction finished: ${sstIdsToRemove.size} files removed, ${newSstIds.size} files added, newSstIds=${newSstIds}, oldSstIds=${sstIdsToRemove}" }
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

    fun getNextSstId(): Int {
        return nextSstId.incrementAndGet()
    }

    fun sync() {
        state.memTable.withWriteLock { it.syncWal() }
    }

    fun dumpStructure() {
        dumpStructure(state.diskSnapshot())
    }

    private fun dumpStructure(snapshot: LsmStorageSstableSnapshot) {
        if (snapshot.l0SstableIds.isNotEmpty()) {
            log.debug { "L0 (${state.l0Sstables.readValue().size}): ${state.l0Sstables.readValue()}" }
        }

        for ((level, files) in snapshot.levels) {
            log.debug { "L${level} (${files.size}): $files" }
        }
    }

    @VisibleForTesting
    fun addL0Sstable(sstId: Int) {
        state.l0Sstables.readValue().addFirst(sstId)
    }

    @VisibleForTesting
    fun addSstable(sst: Sstable) {
        state.sstables[sst.id] = sst
    }

    @VisibleForTesting
    fun constructMergeIterator(): MergeIterator<SsTableIterator> {
        val l0Iterators = state.l0Sstables.readValue().map {
            SsTableIterator.createAndSeekToFirst(state.sstables[it]!!)
        }
        val levelIterators = state.levels.readValue().flatMap { level ->
            level.sstIds.map {
                SsTableIterator.createAndSeekToFirst(state.sstables[it]!!)
            }
        }

        return MergeIterator(l0Iterators + levelIterators)
    }

    @VisibleForTesting
    fun getMemTableId(): Int {
        return state.memTable.readValue().id()
    }

    @VisibleForTesting
    fun getImmutableMemTableIds(): List<Int> {
        return state.immutableMemTables.readValue().map { it.id() }
    }
}
