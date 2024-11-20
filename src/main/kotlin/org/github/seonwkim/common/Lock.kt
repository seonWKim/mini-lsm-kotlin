package org.github.seonwkim.common

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock

interface RwLock<T> {
    fun read(): T
    fun <R> withReadLock(action: (T) -> R): R
    fun <R> withWriteLock(action: (T) -> R): R
}

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

class SimulatedRwLock<T>(
    val value: T,
    val afterReadLockAcquireBehavior: () -> Unit = {},
    val afterWriteLockAcquireBehavior: () -> Unit = {}
) : RwLock<T> {
    private val lock = ReentrantReadWriteLock()

    override fun read(): T {
        return value
    }

    override fun <R> withReadLock(action: (T) -> R): R {
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

class MutexLock<T>(
    val value: T
): RwLock<T> {
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
