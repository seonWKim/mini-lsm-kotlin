package org.seonwkim.lsm.iterator

import org.seonwkim.common.ComparableByteArray

class MockIterator(
    private val data: List<MockIteratorData>,
    private val throwErrorOnIdx: Int = -1
) : StorageIterator {

    private var currentIdx: Int = 0

    override fun key(): ComparableByteArray {
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
    val key: ComparableByteArray,
    val value: ComparableByteArray
)
