package org.github.seonwkim.common.lock

interface RwLock<T> {
    fun read(): T
    fun <R> withReadLock(action: (T) -> R): R
    fun <R> withWriteLock(action: (T) -> R): R
}
