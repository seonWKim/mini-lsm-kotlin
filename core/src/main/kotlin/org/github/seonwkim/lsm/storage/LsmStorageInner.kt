package org.github.seonwkim.lsm.storage

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.memtable.isDeleted
import org.github.seonwkim.lsm.memtable.isValid
import org.github.seonwkim.lsm.sstable.*
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.exists

class LsmStorageInner private constructor(
    val path: Path,
    val options: LsmStorageOptions,
    val blockCache: BlockCache,
    val state: RwLock<LsmStorageState>,
    // val stateLock: RwLock<Unit> = MutexLock(Unit) TODO: do we need this?
    private val nextSstId: AtomicInteger = AtomicInteger(1),
    private val compactionController: CompactionController,
    private val manifest: Manifest?,
) {

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
            val state = DefaultRwLock(LsmStorageState.create(options))
            val blockCache = BlockCache(1 shl 20)

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
                manifest.addRecord(NewMemTable(state.read().memTable.id))
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
                                state.read().l0SsTables.addFirst(sstId)
                            } else {
                                state.read().levels.addFirst(SstLevel(sstId, mutableListOf(sstId)))
                            }
                            nextSstId = maxOf(nextSstId, sstId)
                        }

                        is NewMemTable -> {
                            nextSstId = maxOf(nextSstId, record.memTableId)
                            memTables.add(nextSstId)
                        }

                        is Compaction -> {
                            TODO()
                        }
                    }
                }

                var sstCount = 0
                val tableIds = state.read().l0SsTables + state.read().levels.flatMap { it.tables }
                for (tableId in tableIds) {
                    val sst = SsTable.open(
                        id = tableId,
                        blockCache = blockCache.copy(),
                        file = SsTableFile.open(sstPath(path, tableId))
                    )
                    lastCommitTs = maxOf(lastCommitTs, sst.maxTs)
                    state.read().ssTables[tableId] = sst
                    sstCount += 1
                }
                log.info { "$sstCount SSTs opened" }

                nextSstId += 1

                // For leveled compaction, sort SSTs on each level
                if (compactionController is LeveledCompactionController) {
                    val ssTables = state.read().ssTables
                    for ((_, ssts) in state.read().levels) {
                        ssts.sortWith { t1, t2 ->
                            ssTables[t1]!!.firstKey.compareTo(ssTables[t2]!!.firstKey)
                        }
                    }
                }

                // Recover memTables
                if (options.enableWal) {
                    TODO()
                } else {
                    state.read().memTable = MemTable.create(nextSstId)
                }
                m.addRecord(NewMemTable(state.read().memTable.id))
                nextSstId += 1
                manifest = m
            }

            return LsmStorageInner(
                path = path,
                options = options,
                blockCache = blockCache,
                state = state,
                nextSstId = AtomicInteger(nextSstId),
                compactionController = compactionController,
                manifest = manifest,
            )
        }

        fun open(
            path: Path,
            options: LsmStorageOptions,
            blockCache: BlockCache,
            state: RwLock<LsmStorageState>,
        ): LsmStorageInner {
            if (!path.exists()) {
                log.info { "Creating directories $path" }
                Files.createDirectories(path)
            }

            return LsmStorageInner(
                path = path,
                options = options,
                blockCache = blockCache,
                state = state,
                nextSstId = AtomicInteger(1),
                compactionController = NoCompactionController,
                manifest = null
            )
        }
    }

    fun get(key: ComparableByteArray): ComparableByteArray? {
        val snapshot = state.read()

        val memTableResult = snapshot.memTable.get(key)
        if (memTableResult.isValid()) {
            return memTableResult!!.value
        }

        if (memTableResult.isDeleted()) {
            return null
        }

        for (immMemTable in snapshot.immutableMemTables) {
            val immMemTableResult = immMemTable.get(key)
            if (immMemTableResult.isValid()) {
                return immMemTableResult!!.value
            }

            if (immMemTableResult.isDeleted()) {
                return null
            }
        }

        val ssTableIters = mutableListOf<StorageIterator>()
        for (tableIdx in snapshot.l0SsTables) {
            val table = snapshot.ssTables[tableIdx]!!
            // TODO: for now, we do not consider timestamp, so we set to 0
            val timestampedKey = TimestampedKey(key, 0)
            if (table.firstKey <= timestampedKey && timestampedKey <= table.lastKey) {
                ssTableIters.add(SsTableIterator.createAndSeekToKey(table, timestampedKey))
            }
        }
        val mergedIter = MergeIterator(ssTableIters)
        if (!mergedIter.isValid()) return null
        if (mergedIter.key() == key) {
            return if (mergedIter.isDeleted()) null else mergedIter.value()
        }
        return null
    }

    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        updateMemTable {
            it.memTable.put(
                key = key,
                value = MemtableValue(value)
            )
        }
    }

    fun delete(key: ComparableByteArray) {
        updateMemTable {
            it.memTable.put(
                key = key,
                value = MemtableValue(ComparableByteArray.new())
            )
        }
    }

    private fun updateMemTable(action: (state: LsmStorageState) -> Unit) {
        state.withWriteLock {
            if (shouldFreezeMemTable()) {
                forceFreezeMemTable(it)
            }
            action(it)
        }
    }

    private fun shouldFreezeMemTable(): Boolean {
        return state.read().memTable.approximateSize() > options.targetSstSize
    }

    /**
     * Force freeze the current memTable to an immutable memTable.
     * Requires external locking.
     */
    fun forceFreezeMemTable(state: LsmStorageState) {
        val memtableId = nextSstId.get()
        val memTable = if (options.enableWal) {
            TODO("should be implemented after WAL is supported")
        } else {
            MemTable.create(memtableId)
        }

        state.immutableMemTables.addFirst(state.memTable)
        state.memTable = memTable
    }

    /**
     * Force flush the earliest created immutable memTable to disk
     */
    fun forceFlushNextImmMemTable() {
        lateinit var flushMemTable: MemTable
        state.withReadLock {
            flushMemTable = it.immutableMemTables.lastOrNull()
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

        state.withWriteLock { snapshot ->
            val immutableMemTable: MemTable? = snapshot.immutableMemTables.peekLast()
            if (immutableMemTable?.id != sstId) {
                // in case where forceFlushNextImmMemTable() is called concurrently, it can enter this block
                log.info { "Sst id($sstId) and immutable memTable id(${immutableMemTable?.id}) mismatch. There might be concurrent calls to flush immMemTable" }
                return@withWriteLock
            } else {
                // we can safely remove the oldest immutableMemTable
                snapshot.immutableMemTables.pollLast()
            }

            // TODO: this is only for no compaction strategy, will require additional logic when we support for diverse compaction algorithms
            snapshot.l0SsTables.addFirst(sstId)
            log.info { "Flushed $sstId.sst with size ${sst.file.size}" }
            snapshot.ssTables[sstId] = sst
        }

        if (options.enableWal) {
            TODO("remove wal file")
        }

        // Is there more sufficient way to sync all OS-internal file content and metadata to disk?
        FileChannel.open(path).use { it.force(true) }
    }

    /**
     * Trigger flush operation when number of immutable memTables exceeds limit.
     */
    fun triggerFlush() {
        val shouldFlush = state.withReadLock {
            it.immutableMemTables.size >= options.numMemTableLimit
        }

        if (shouldFlush) {
            forceFlushNextImmMemTable()
        }
    }

    fun scan(lower: Bound, upper: Bound): FusedIterator {
        // memTable iterators
        val snapshot = state.read()
        val memTableIters = listOf(snapshot.memTable.iterator(lower, upper)) +
                snapshot.immutableMemTables.map { it.iterator(lower, upper) }
        val memTableMergedIter = MergeIterator(memTableIters)

        val tableIters = snapshot.l0SsTables.mapNotNull { idx ->
            val table = snapshot.ssTables[idx]!!
            if (rangeOverlap(lower, upper, table.firstKey, table.lastKey)) {
                when (lower) {
                    is Bound.Included -> SsTableIterator.createAndSeekToKey(table, TimestampedKey(lower.value))
                    is Bound.Excluded -> {
                        val iter = SsTableIterator.createAndSeekToKey(table, TimestampedKey(lower.value))
                        if (iter.isValid() && iter.key() == lower.value) {
                            iter.next()
                        }
                        iter
                    }

                    is Bound.Unbounded -> SsTableIterator.createAndSeekToFirst(table)
                }
            } else null
        }
        val tableMergedIter = MergeIterator(tableIters)

        return FusedIterator(
            iter = TwoMergeIterator.create(first = memTableMergedIter, second = tableMergedIter)
        )
    }

    private fun rangeOverlap(
        userBegin: Bound,
        userEnd: Bound,
        tableBegin: TimestampedKey,
        tableEnd: TimestampedKey
    ): Boolean {
        when (userEnd) {
            is Bound.Excluded -> if (userEnd.value <= tableBegin.bytes) {
                return false
            }

            is Bound.Included -> if (userEnd.value < tableBegin.bytes) {
                return false
            }

            else -> {}
        }

        when (userBegin) {
            is Bound.Excluded -> if (userBegin.value >= tableEnd.bytes) {
                return false
            }

            is Bound.Included -> if (userBegin.value > tableEnd.bytes) {
                return false
            }

            else -> {}
        }

        return true
    }
}
