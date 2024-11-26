package org.github.seonwkim.common.lock

import java.util.concurrent.locks.ReentrantReadWriteLock

class DefaultRwLock<T>(
    private val value: T
) : RwLock<T> {
    private val lock = ReentrantReadWriteLock()

    override fun read(): T {
        return value
    }

    override fun <R> withReadLock(action: (T) -> R): R {
        val readLock = lock.readLock()
        readLock.lock()
        try {
            return action(value)
        } finally {
            readLock.unlock()
        }
    }

    override fun <R> withWriteLock(action: (T) -> R): R {
        val writeLock = lock.writeLock()
        writeLock.lock()
        try {
            return action(value)
        } finally {
            writeLock.unlock()
        }
    }
}