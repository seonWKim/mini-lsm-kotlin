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

    fun copy(): StorageIterator
}

/**
 * A wrapper around existing iterator.
 *
 * Prevents users from calling [next] when the iterator is invalid.
 * If an iterator is already invalid, [next] doesn't do anything.
 * If underlying [next] throws an exception, [isValid] should return false, and following calls on [next] should always throw an [Error].
 */
class FusedIterator(
    private val iter: StorageIterator,
) : StorageIterator {
    private var hasErrored = false

    override fun key(): ComparableByteArray {
        if (!isValid()) {
            throw Error("Use isValid() function before calling this function")
        }

        return iter.key()
    }

    override fun value(): ComparableByteArray {
        if (!isValid()) {
            throw Error("Use isValid() function before calling this function")
        }

        return iter.value()
    }

    override fun isValid(): Boolean {
        return !hasErrored && iter.isValid()
    }

    override fun next() {
        if (hasErrored) {
            throw Error("The iterator is tainted")
        }

        try {
            iter.next()
        } catch (e: Exception) {
            hasErrored = true
            throw IllegalStateException("There was an exception when calling next: $e")
        }
    }

    override fun copy(): StorageIterator {
        return FusedIterator(iter)
    }
}
