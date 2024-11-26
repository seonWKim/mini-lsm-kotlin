package org.github.seonwkim.common.lock

import java.util.concurrent.locks.ReentrantLock

class MutexLock<T>(
    val value: T
) : RwLock<T> {
    private val lock = ReentrantLock()

    override fun read(): T {
        return value
    }

    override fun <R> withWriteLock(action: (T) -> R): R {
        lock.lock()
        try {
            return action(value)
        } finally {
            lock.unlock()
        }
    }

    override fun <R> withReadLock(action: (T) -> R): R {
        lock.lock()
        try {
            return action(value)
        } finally {
            lock.unlock()
        }
    }
}
