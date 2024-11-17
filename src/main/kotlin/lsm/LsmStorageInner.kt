package org.example.lsm

import org.example.common.Bound
import org.example.common.ComparableByteArray
import org.example.lsm.memtable.*
import org.example.lsm.memtable.iterator.FusedIterator
import org.example.lsm.memtable.iterator.MergeIterator
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantReadWriteLock

class LsmStorageInner(
    val path: Path,
    val options: LsmStorageOptions,
    val state: LsmStorageState
) {

    private val memTableLock: ReentrantReadWriteLock = ReentrantReadWriteLock()

    companion object {
        fun open(path: Path, options: LsmStorageOptions): LsmStorageInner {
            return LsmStorageInner(
                path = path,
                options = options,
                state = LsmStorageState(MemTable.create(0))
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
            state.memTable.put(key, MemtableValue(value, MemTableValueFlag.NORMAL))
        }
    }

    fun delete(key: ComparableByteArray) {
        updateMemTable {
            state.memTable.put(key, MemtableValue(ComparableByteArray.EMPTY, MemTableValueFlag.DELETED))
        }
    }

    private fun updateMemTable(action: () -> Unit) {
        val readLock = memTableLock.readLock()
        readLock.lock()
        var readLockUnlocked = false
        try {
            if (shouldFreezeMemtable()) {
                readLock.unlock()
                readLockUnlocked = true
                val writeLock = memTableLock.writeLock()
                writeLock.lock()
                try {
                    if (shouldFreezeMemtable()) {
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
        val lock = memTableLock.writeLock()
        lock.lock()
        try {
            state.freezeMemtable()
        } finally {
            lock.unlock()
        }
    }

    fun scan(lower: Bound, upper: Bound): FusedIterator {
        val iter = FusedIterator(
            MergeIterator(
                listOf(state.memTable.iterator(lower, upper)) +
                        state.immutableMemtables.map { it.iterator(lower, upper) }
            )
        )
        return iter
    }

    private fun shouldFreezeMemtable(): Boolean {
        val lock = memTableLock.readLock()
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
