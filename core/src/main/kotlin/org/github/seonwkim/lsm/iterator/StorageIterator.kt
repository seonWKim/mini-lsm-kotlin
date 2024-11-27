package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.ComparableByteArray

interface StorageIterator {
    // Get the current key
    fun key(): ComparableByteArray

    // Get the current value
    fun value(): ComparableByteArray

    // Check if the current iterator is valid
    fun isValid(): Boolean

    // Move to the next position
    fun next()

    fun copy(): StorageIterator

    // Number of underlying active iterators for this iterator
    fun numActiveIterators(): Int {
        return 1
    }

    // for debugging purposes, it will consume your iterator
    fun allKeyValues(): List<String> {
        val result = mutableListOf<String>()
        while (isValid()) {
            result.add("key: ${key()}, value: ${value()}")
            next()
        }

        return result
    }
}
