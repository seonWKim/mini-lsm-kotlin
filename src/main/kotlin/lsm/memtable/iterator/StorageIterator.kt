package org.example.lsm.memtable.iterator

import org.example.common.ComparableByteArray

interface StorageIterator {
    // Get the current key
    fun key(): ComparableByteArray

    // Get the current value
    fun value(): ComparableByteArray

    // Check if the current iterator is valid
    fun isValid(): Boolean

    // Move to the next position
    fun next()

    // Number of underlying active iterators for this iterator
    fun numActiveIterators(): Int {
        return 1
    }
}
