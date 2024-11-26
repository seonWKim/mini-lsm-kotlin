package org.github.seonwkim.common.lock

import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * A lock implementation that enforces priority-based access.
 *
 * @param T the type of the value being protected by the lock
 * @property value the value being protected by the lock
 * @property priority the priority level of the lock. Higher the value, higher the priority.
 */
class PriorityAwareLock<T>(
    val value: T,
    private val priority: Int
) : RwLock<T> {
    companion object {
        private val PRIORITY_SCOPED_VALUE: ScopedValue<Int> = ScopedValue.newInstance()
    }

    private val lock = ReentrantReadWriteLock()

    override fun read(): T {
        return value
    }

    override fun <R> withReadLock(action: (T) -> R): R {
        checkPriority()
        return ScopedValue.where(PRIORITY_SCOPED_VALUE, priority)
            .call {
                val readLock = lock.readLock()
                readLock.lock()
                try {
                    action(value)
                } finally {
                    readLock.unlock()
                }
            }
    }

    override fun <R> withWriteLock(action: (T) -> R): R {
        checkPriority()
        return ScopedValue.where(PRIORITY_SCOPED_VALUE, priority)
            .call {
                val writeLock = lock.writeLock()
                writeLock.lock()
                try {
                    action(value)
                } finally {
                    writeLock.unlock()
                }
            }
    }

    private fun checkPriority() {
        val prevLockPriority = runCatching { PRIORITY_SCOPED_VALUE.get() }.getOrNull()
        if (prevLockPriority != null && prevLockPriority <= priority) {
            throw Error("You have already acquired lock with higher priority.")
        }
    }
}
