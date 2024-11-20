package org.seonwkim.lsm

import org.seonwkim.common.Bound
import org.seonwkim.common.BoundFlag
import org.seonwkim.common.ComparableByteArray
import org.seonwkim.lsm.block.BlockKey
import org.seonwkim.lsm.iterator.*
import org.seonwkim.lsm.memtable.MemTable
import org.seonwkim.lsm.memtable.MemtableValue
import org.seonwkim.lsm.memtable.isDeleted
import org.seonwkim.lsm.memtable.isValid
import org.seonwkim.lsm.sstable.BlockCache
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantReadWriteLock

class LsmStorageInner(
    val path: Path,
    val options: LsmStorageOptions,
    val state: LsmStorageState,
    val blockCache: BlockCache
) {

    private val stateLock: ReentrantReadWriteLock = ReentrantReadWriteLock()

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
        val memtableResult = state.memTable.get(key)
        if (memtableResult.isValid()) {
            return memtableResult!!.value
        }

        for (immMemtable in state.immutableMemtables) {
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
            state.memTable.put(
                key = key,
                value = MemtableValue(value, IteratorMeta(IteratorFlag.NORMAL))
            )
        }
    }

    fun delete(key: ComparableByteArray) {
        updateMemTable {
            state.memTable.put(
                key = key,
                value = MemtableValue(ComparableByteArray.new(), IteratorMeta(IteratorFlag.DELETED))
            )
        }
    }

    private fun updateMemTable(action: () -> Unit) {
        val readLock = stateLock.readLock()
        readLock.lock()
        var readLockUnlocked = false
        try {
            if (shouldFreezeMemTable()) {
                readLock.unlock()
                readLockUnlocked = true
                val writeLock = stateLock.writeLock()
                writeLock.lock()
                try {
                    if (shouldFreezeMemTable()) {
                        forceFreezeMemtable()
                    }
                } finally {
                    writeLock.unlock()
                }
            }
            action()
        } finally {
            if (!readLockUnlocked) {
                readLock.unlock()
            }
        }
    }

    fun forceFreezeMemtable() {
        val lock = stateLock.writeLock()
        lock.lock()
        try {
            state.freezeMemtable()
        } finally {
            lock.unlock()
        }
    }

    fun scan(lower: Bound, upper: Bound): FusedIterator {
        // memTable iterators
        val memTableIters = listOf(state.memTable.iterator(lower, upper)) +
                state.immutableMemtables.map { it.iterator(lower, upper) }
        val memTableMergedIter = MergeIterator(memTableIters)

        val tableIters = state.l0SsTables.mapNotNull { idx ->
            val table = state.ssTables[idx]!!
            if (rangeOverlap(lower, upper, table.firstKey, table.lastKey)) {
                when (lower.flag) {
                    BoundFlag.INCLUDED -> SsTableIterator.createAndSeekToKey(table, BlockKey(lower.value))
                    BoundFlag.EXCLUDED -> {
                        val iter = SsTableIterator.createAndSeekToKey(table, BlockKey(lower.value))
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
        val lock = stateLock.readLock()
        lock.lock()
        try {
            if (state.memTable.approximateSize() > options.targetSstSize) {
                return true
            }

            if (state.memTable.countEntries() > options.numMemtableLimit) {
                return true
            }
        } finally {
            lock.unlock()
        }

        return false
    }
}

fun rangeOverlap(userBegin: Bound, userEnd: Bound, tableBegin: BlockKey, tableEnd: BlockKey): Boolean {
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
