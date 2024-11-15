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
                state = LsmStorageState(
                    memtable = MemTable.create(0),
                    immutableMemtables = emptyList()
                )
            )
        }
    }

    fun get(key: ComparableByteArray): ComparableByteArray? {
        val lock = memtableStateLock.readLock()  // Get the read lock
        lock.lock()  // Acquire the lock
        return try {
            state.memtable.get(key)  // Access the state within the locked scope
        } finally {
            lock.unlock()  // Ensure the lock is released
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
        TODO()
    }
}
