package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedByteArray

/**
 * MockIterator is a mock implementation of the StorageIterator interface.
 * It iterates over a predefined list of key-value pairs for testing purposes.
 *
 * @property data the list of key-value pairs to iterate over
 * @property throwErrorOnIdx the index at which to throw an error, for testing error handling
 */
class MockIterator(
    private val data: List<MockIteratorData>,
    private val throwErrorOnIdx: Int = -1
) : StorageIterator {

    private var currentIdx: Int = 0

    override fun key(): TimestampedByteArray {
        return data[currentIdx].key
    }

    override fun value(): ComparableByteArray {
        return data[currentIdx].value
    }

    override fun isValid(): Boolean {
        return currentIdx < data.size
    }

    override fun next() {
        currentIdx++
        if (currentIdx == throwErrorOnIdx) {
            throw IllegalStateException("Mock error!!")
        }
    }

    override fun copy(): StorageIterator {
        return MockIterator(data.map { it.copy() })
    }
}

data class MockIteratorData(
    val key: TimestampedByteArray,
    val value: ComparableByteArray
)
