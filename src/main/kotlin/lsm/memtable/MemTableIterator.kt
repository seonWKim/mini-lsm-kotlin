package org.example.lsm.memtable

import org.example.common.Bound
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

class MemTableIterator(
    memTable: MemTable,
    lower: Bound,
    upper: Bound
) : StorageIterator {

    private var current: Map.Entry<MemTableKey, MemtableValue>? = null
    private val iter: Iterator<Map.Entry<MemTableKey, MemtableValue>>
    private val lower: Bound
    private val upper: Bound

    init {
        val iter = memTable.map.iterator()
        current = iter.asSequence()
            .firstOrNull { lower == Bound.UNBOUNDED || it.value.value >= lower.value }
        this.iter = iter
        this.lower = lower
        this.upper = upper
    }

    override fun key(): ComparableByteArray {
        return current?.key
            ?: throw IllegalCallerException("Use isValid() function before calling this function")
    }

    override fun value(): ComparableByteArray {
        return current?.value?.value
            ?: throw IllegalCallerException("Use isValid() function before calling this function")
    }

    override fun isValid(): Boolean {
        return current != null
    }

    override fun next() {
        if (iter.hasNext()) {
            current = iter.next()
        } else {
            current = null
        }
    }
}
