package org.seonwkim.lsm.iterator

import org.seonwkim.common.ComparableByteArray

class MockIterator(
    private val data: List<Pair<ComparableByteArray, ComparableByteArray>>,
    private val throwErrorOnIdx: Int = -1
) : StorageIterator {

    private var currentIdx: Int = 0

    override fun key(): ComparableByteArray {
        return data[currentIdx].first
    }

    override fun value(): ComparableByteArray {
        return data[currentIdx].second
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
        return MockIterator(data)
    }
}
