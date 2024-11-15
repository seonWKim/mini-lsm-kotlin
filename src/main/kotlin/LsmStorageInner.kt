package org.example

import java.nio.file.Path
import java.util.concurrent.locks.ReentrantReadWriteLock

class LsmStorageInner(
    val state: LsmStorageState // TODO: we need to handle concurrent read/write on state
) {

    val stateLock: ReentrantReadWriteLock = ReentrantReadWriteLock()

    companion object {
        fun open(path: Path, options: LsmStorageOptions): LsmStorageInner {
            TODO()
        }
    }

    fun get(key: ComparableByteArray): ComparableByteArray? {
        TODO()
    }

    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        TODO()
    }

    fun delete(key: ComparableByteArray) {
        TODO()
    }

    fun forceFreezeMemtable() {
        TODO()
    }
}
