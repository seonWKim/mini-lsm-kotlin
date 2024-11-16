package org.example.lsm.memtable.iterator

import org.example.common.ComparableByteArray

class MockIterator(
    private val data: List<Pair<ComparableByteArray, ComparableByteArray>>
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
    }

    override fun copy(): StorageIterator {
        return MockIterator(data)
    }
}
