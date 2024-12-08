package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedByteArray


/**
 * Interface for storage iterators.
 * Implementations of this interface provide mechanisms to iterate over key-value pairs in storage.
 */
interface StorageIterator {

    /**
     * Gets the current key.
     *
     * @return the current key as a [TimestampedByteArray]
     */
    fun key(): TimestampedByteArray

    /**
     * Gets the current value.
     *
     * @return the current value as a [ComparableByteArray]
     */
    fun value(): ComparableByteArray

    /**
     * Checks if the current iterator is valid.
     *
     * @return true if the iterator is valid, false otherwise
     */
    fun isValid(): Boolean

    /**
     * Moves to the next position in the iterator.
     * After calling this method, the iterator will point to the next key-value pair.
     */
    fun next()

    /**
     * Creates a copy of the current iterator.
     *
     * @return a new `StorageIterator` that is a copy of the current iterator
     */
    fun copy(): StorageIterator

    /**
     * Gets the number of underlying active iterators for this iterator.
     * This is useful for debugging purposes.
     *
     * @return the number of active iterators
     */
    fun numActiveIterators(): Int {
        return 1
    }

    /**
     * Retrieves all key-value pairs from the iterator.
     * This method is for debugging purposes and will consume the iterator.
     *
     * @return a list of strings representing all key-value pairs
     */
    fun allKeyValues(): List<String> {
        val result = mutableListOf<String>()
        while (isValid()) {
            result.add("key: ${key()}, value: ${value()}")
            next()
        }

        return result
    }
}
