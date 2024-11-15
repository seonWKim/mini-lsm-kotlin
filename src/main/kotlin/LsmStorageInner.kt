package org.example

import java.nio.file.Path
import java.util.concurrent.locks.ReentrantReadWriteLock

class LsmStorageInner(
    val path: Path,
    val options: LsmStorageOptions,
    val state: LsmStorageState
) {

    val memtableStateLock: ReentrantReadWriteLock = ReentrantReadWriteLock()

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
        val lock = memtableStateLock.readLock()
        lock.lock()
        try {
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

            return null
            TODO("retrieve from immutable memtables")
            TODO("retrieve from sstables stored in disk")
        } finally {
            lock.unlock()
        }
    }

    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        val lock = memtableStateLock.writeLock()
        lock.lock()
        return try {
            if (shouldFreezeMemtable()) {
                forceFreezeMemtable()
            }
            state.memTable.put(key, MemtableValue(value, MemtableValueFlag.NORMAL))
        } finally {
            lock.unlock()
        }
    }

    fun delete(key: ComparableByteArray) {
        val lock = memtableStateLock.writeLock()
        lock.lock()
        return try {
            if (shouldFreezeMemtable()) {
                forceFreezeMemtable()
            }
            state.memTable.put(key, MemtableValue(key, MemtableValueFlag.DELETED))
        } finally {
            lock.unlock()
        }
    }

    fun forceFreezeMemtable() {
        val lock = memtableStateLock.writeLock()
        lock.lock()
        try {
            state.freezeMemtable()
        } finally {
            lock.unlock()
        }
    }

    private fun shouldFreezeMemtable(): Boolean {
        val lock = memtableStateLock.readLock()
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
