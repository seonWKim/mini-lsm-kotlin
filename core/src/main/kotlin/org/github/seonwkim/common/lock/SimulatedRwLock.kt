package org.github.seonwkim.common.lock

import java.util.concurrent.locks.ReentrantReadWriteLock

class SimulatedRwLock<T>(
    val value: T,
    val beforeReadLockAcquireBehavior: () -> Unit = {},
    val afterReadLockAcquireBehavior: () -> Unit = {},
    val beforeWriteLockAcquireBehavior: () -> Unit = {},
    val afterWriteLockAcquireBehavior: () -> Unit = {},
) : RwLock<T> {
    private val lock = ReentrantReadWriteLock()

    override fun read(): T {
        return value
    }

    override fun <R> withReadLock(action: (T) -> R): R {
        beforeReadLockAcquireBehavior()
        val readLock = lock.readLock()
        readLock.lock()
        try {
            afterReadLockAcquireBehavior()
            return action(value)
        } finally {
            readLock.unlock()
        }
    }

    override fun <R> withWriteLock(action: (T) -> R): R {
        beforeWriteLockAcquireBehavior()
        val writeLock = lock.writeLock()
        writeLock.lock()
        try {
            afterWriteLockAcquireBehavior()
            return action(value)
        } finally {
            writeLock.unlock()
        }
    }
}
