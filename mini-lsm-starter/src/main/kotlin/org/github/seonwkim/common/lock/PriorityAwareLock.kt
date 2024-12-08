package org.github.seonwkim.common.lock

import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * A lock implementation that enforces priority-based access.
 *
 * @param T the type of the value being protected by the lock
 * @property value the value being protected by the lock
 * @property priority the priority level of the lock. lower the value, higher the priority.
 */
class PriorityAwareLock<T>(
    private var value: T,
    private val priority: Int
) : RwLock<T> {
    companion object {
        private val PRIORITY_SCOPED_VALUE: ScopedValue<PriorityAwareLock<*>> = ScopedValue.newInstance()
    }

    private val lock = ReentrantReadWriteLock()

    /**
     * Note that explicit write lock should be acquired using [withWriteLock].
     */
    override fun replace(value: T): T {
        val prev = this.value
        this.value = value
        return prev
    }

    override fun readValue(): T {
        return value
    }

    override fun <R> withReadLock(action: (T) -> R): R {
        checkPriority()
        return ScopedValue.where(PRIORITY_SCOPED_VALUE, this)
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
        return ScopedValue.where(PRIORITY_SCOPED_VALUE, this)
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
        if (prevLockPriority != null && prevLockPriority.priority > priority) {
            throw Error("You have already acquired lock with higher priority.")
        }
    }
}
