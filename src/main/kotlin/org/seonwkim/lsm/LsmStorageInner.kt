package org.seonwkim.lsm

import org.seonwkim.common.*
import org.seonwkim.lsm.iterator.*
import org.seonwkim.lsm.memtable.MemTable
import org.seonwkim.lsm.memtable.MemtableValue
import org.seonwkim.lsm.memtable.isDeleted
import org.seonwkim.lsm.memtable.isValid
import org.seonwkim.lsm.sstable.BlockCache
import java.nio.file.Path

class LsmStorageInner(
    val path: Path,
    val options: LsmStorageOptions,
    val state: LsmStorageState,
    val blockCache: BlockCache,
    lock: RwLock = DefaultRwLock()
) {

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
        val memtableResult = state.memTable.get(key)
        if (memtableResult.isValid()) {
            return memtableResult!!.value
        }

        for (immMemtable in state.immutableMemTables) {
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

    /**
     * | Thread1                                | Thread2                                |
     * |----------------------------------------|----------------------------------------|
     * | [checkShouldFreezeMemTableWithLock]    | [checkShouldFreezeMemTableWithLock]    |
     * | -> acquire/release read lock           | -> acquire/release read lock           |
     * | [stateLock.writeLock]                  | [stateLock.writeLock]                  |
     * | -> acquire write lock                  |                                        |
     * |                                        | -> wait to acquire write lock          |
     * | [shouldFreezeMemTable]                 |                                        |
     * |                                        |                                        |
     * | [forceFreezeMemTable]                  |                                        |
     * |                                        |                                        |
     * | Release write lock                     | acquire write lock                     |
     * |                                        | [shouldFreezeMemTable]                 |
     * |                                        | -> memTable frozen by thread 1, skip   |
     */
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
                state.immutableMemTables.map { it.iterator(lower, upper) }
        val memTableMergedIter = MergeIterator(memTableIters)

        val tableIters = state.l0SsTables.mapNotNull { idx ->
            val table = state.ssTables[idx]!!
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
        val lock = stateLock.readLock()
        lock.lock()
        try {
            if (state.memTable.approximateSize() > options.targetSstSize) {
                return true
            }

        } finally {
            lock.unlock()
        }

        return false
    }
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
