package org.seonwkim.common

import java.util.concurrent.locks.ReentrantReadWriteLock

class EncompassingRwLock<T>(
    private val value: T
) {
    private val lock = ReentrantReadWriteLock()

    fun read(): T {
        return value
    }

    fun <R> withReadLock(action: (T) -> R): R {
        val readLock = lock.readLock()
        readLock.lock()
        try {
            return action(value)
        } finally {
            readLock.unlock()
        }
    }

    fun <R> withWriteLock(action: (T) -> R): R {
        val writeLock = lock.writeLock()
        writeLock.lock()
        try {
            return action(value)
        } finally {
            writeLock.unlock()
        }
    }
}
