package org.github.seonwkim.lsm.storage

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.memtable.isDeleted
import org.github.seonwkim.lsm.memtable.isValid
import org.github.seonwkim.lsm.sstable.BlockCache
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import org.github.seonwkim.lsm.sstable.sstPath
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.exists

class LsmStorageInner(
    val path: Path,
    val options: LsmStorageOptions,
    val blockCache: BlockCache,
    val state: RwLock<LsmStorageState>,
    // val stateLock: RwLock<Unit> = MutexLock(Unit) TODO: do we need this?
    private val nextSstId: AtomicInteger = AtomicInteger(1)
) {

    companion object {
        private val log = mu.KotlinLogging.logger { }

        fun open(path: Path, options: LsmStorageOptions): LsmStorageInner {
            val state = DefaultRwLock(LsmStorageState.create(options))
            val blockCache = BlockCache(1 shl 20)
            if (!path.exists()) {
                log.info { "Creating directories $path" }
                Files.createDirectories(path)
            }

            return LsmStorageInner(
                path = path,
                options = options,
                state = state,
                blockCache = blockCache
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
            // flush the oldest memTable first
            val immutableMemTable = snapshot.immutableMemTables.pollLast()
            if (immutableMemTable.id != sstId) {
                throw IllegalStateException("Sst id($sstId) and immutable memTable id(${immutableMemTable.id} mismatch")
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
            println("triggering flush operation")
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
                when (lower.flag) {
                    BoundFlag.INCLUDED -> SsTableIterator.createAndSeekToKey(table, TimestampedKey(lower.value))
                    BoundFlag.EXCLUDED -> {
                        val iter = SsTableIterator.createAndSeekToKey(table, TimestampedKey(lower.value))
                        if (iter.isValid() && iter.key() == lower.value) {
                            iter.next()
                        }
                        iter
                    }

                    BoundFlag.UNBOUNDED -> SsTableIterator.createAndSeekToFirst(table)
                }
            } else null
        }
        val tableMergedIter = MergeIterator(tableIters)

        return FusedIterator(
            iter = TwoMergeIterator.create(first = memTableMergedIter, second = tableMergedIter)
        )
    }

    fun rangeOverlap(
        userBegin: Bound,
        userEnd: Bound,
        tableBegin: TimestampedKey,
        tableEnd: TimestampedKey
    ): Boolean {
        when (userEnd.flag) {
            BoundFlag.EXCLUDED -> if (userEnd.value <= tableBegin.bytes) {
                return false
            }

            BoundFlag.INCLUDED -> if (userEnd.value < tableBegin.bytes) {
                return false
            }

            else -> {}
        }

        when (userBegin.flag) {
            BoundFlag.EXCLUDED -> if (userBegin.value >= tableEnd.bytes) {
                return false
            }

            BoundFlag.INCLUDED -> if (userBegin.value > tableEnd.bytes) {
                return false
            }

            else -> {}
        }

        return true
    }
}
