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
            val memtableResult = state.memtable.get(key)
            if (memtableResult != null) {
                return memtableResult
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
            state.memtable.put(key, value)
        } finally {
            lock.unlock()
        }
    }

    fun delete(key: ComparableByteArray) {
        val lock = memtableStateLock.writeLock()
        lock.lock()
        return try {
            state.memtable.delete(key)
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
}
