package org.github.seonwkim.common.lock

import java.util.concurrent.locks.ReentrantLock

class MutexLock<T>(
    var value: T
) : RwLock<T> {
    private val lock = ReentrantLock()
    override fun replace(value: T): T {
        val prev = this.value
        this.value = value
        return prev
    }

    override fun readValue(): T {
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
