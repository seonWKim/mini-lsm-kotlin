package org.seonwkim.lsm

import org.seonwkim.common.*
import org.seonwkim.lsm.iterator.*
import org.seonwkim.lsm.memtable.MemTable
import org.seonwkim.lsm.memtable.MemtableValue
import org.seonwkim.lsm.memtable.isDeleted
import org.seonwkim.lsm.memtable.isValid
import org.seonwkim.lsm.sstable.BlockCache
import org.seonwkim.lsm.sstable.SsTableBuilder
import java.nio.file.Path

class LsmStorageInner(
    val path: Path,
    val options: LsmStorageOptions,
    val blockCache: BlockCache,
    val state: RwLock<LsmStorageState>,
) {

    companion object {
        fun open(path: Path, options: LsmStorageOptions): LsmStorageInner {
            return LsmStorageInner(
                path = path,
                options = options,
                state = DefaultRwLock(LsmStorageState(memTable = MemTable.create(0))),
                blockCache = BlockCache()
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

    // requires external locking
    fun forceFreezeMemTable(state: LsmStorageState) {
        state.freezeMemtable()
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
