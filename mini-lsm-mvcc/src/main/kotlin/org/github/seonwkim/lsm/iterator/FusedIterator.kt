package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.TimestampedByteArray

/**
 * FusedIterator is a wrapper around an existing iterator.
 * It prevents users from calling [next] when the iterator is invalid.
 * If an iterator is already invalid, [next] doesn't do anything.
 * If the underlying [next] throws an exception, [isValid] should return false, and following calls on [next] should always throw an [Error].
 *
 * @property iter the underlying storage iterator
 * @property hasErrored a flag indicating whether an error has occurred
 */
class FusedIterator(
    private val iter: StorageIterator,
) : StorageIterator {
    private var hasErrored = false

    override fun key(): TimestampedByteArray {
        if (!isValid()) {
            throw Error("Use isValid() function before calling this function")
        }

        return iter.key()
    }

    override fun value(): TimestampedByteArray {
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
        return FusedIterator(iter.copy())
    }

    override fun numActiveIterators(): Int {
        return iter.numActiveIterators()
    }
}
