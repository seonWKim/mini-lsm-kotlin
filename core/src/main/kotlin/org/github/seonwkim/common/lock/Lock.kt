package org.github.seonwkim.common.lock

/**
 * Interface representing a read-write lock for a value of type T.
 *
 * @param T the type of the value being protected by the lock
 */
interface RwLock<T> {

    /**
     * Switches the current value protected by the lock to a new value.
     *
     * @param value the new value to be set
     */
    fun switchValue(value: T)

    /**
     * Reads the current value protected by the lock.
     *
     * @return the current value
     */
    fun readValue(): T

    /**
     * Executes the given action with a read lock.
     *
     * @param R the return type of the action
     * @param action the action to be executed with the read lock
     * @return the result of the action
     */
    fun <R> withReadLock(action: (T) -> R): R

    /**
     * Executes the given action with a write lock.
     *
     * @param R the return type of the action
     * @param action the action to be executed with the write lock
     * @return the result of the action
     */
    fun <R> withWriteLock(action: (T) -> R): R
}
