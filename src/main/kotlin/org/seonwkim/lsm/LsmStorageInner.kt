package org.seonwkim.lsm

import org.seonwkim.common.*
import org.seonwkim.lsm.iterator.FusedIterator
import org.seonwkim.lsm.iterator.MergeIterator
import org.seonwkim.lsm.iterator.SsTableIterator
import org.seonwkim.lsm.iterator.TwoMergeIterator
import org.seonwkim.lsm.memtable.MemTable
import org.seonwkim.lsm.memtable.MemtableValue
import org.seonwkim.lsm.memtable.isDeleted
import org.seonwkim.lsm.memtable.isValid
import org.seonwkim.lsm.sstable.BlockCache
import java.nio.file.Path

class LsmStorageInner(
    val path: Path,
    val options: LsmStorageOptions,
    val blockCache: BlockCache,
    state: LsmStorageState,
    lock: RwLock = DefaultRwLock(),
) {

    val state: EncompassingRwLock<LsmStorageState> = EncompassingRwLock(state)
    private val stateLock: RwLock = lock

    companion object {
        fun open(path: Path, options: LsmStorageOptions): LsmStorageInner {
            return LsmStorageInner(
                path = path,
                options = options,
                state = LsmStorageState(memTable = MemTable.create(0)),
                blockCache = BlockCache()
            )
        }
    }

    fun get(key: ComparableByteArray): ComparableByteArray? {
        val memtableResult = state.read().memTable.get(key)
        if (memtableResult.isValid()) {
            return memtableResult!!.value
        }

        for (immMemtable in state.read().immutableMemTables) {
            val immMemtableResult = immMemtable.get(key)
            if (immMemtableResult.isValid()) {
                return immMemtableResult!!.value
            }

            if (immMemtableResult.isDeleted()) {
                return null
            }
        }

        // TODO("retrieve from sstables stored in disk")
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

    // requires external locking
    fun forceFreezeMemTable(state: LsmStorageState) {
        state.freezeMemtable()
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

    private fun shouldFreezeMemTable(): Boolean {
        return state.read().memTable.approximateSize() > options.targetSstSize
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
