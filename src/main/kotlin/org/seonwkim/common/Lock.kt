package org.seonwkim.common

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantReadWriteLock

interface RwLock {
    fun readLock(): Lock
    fun writeLock(): Lock
}

class DefaultRwLock : RwLock {
    val lock = ReentrantReadWriteLock()

    override fun readLock(): Lock {
        return lock.readLock()
    }

    override fun writeLock(): Lock {
        return lock.writeLock()
    }
}

class SimulatedRwLock(
    val readBehavior: () -> Unit = {},
    val writeBehavior: () -> Unit = {}
) : RwLock {
    val lock = ReentrantReadWriteLock()
    override fun readLock(): Lock {
        readBehavior()
        return lock.readLock()
    }

    override fun writeLock(): Lock {
        writeBehavior()
        return lock.writeLock()
    }
}
