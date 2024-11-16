package org.example.lsm.memtable

import org.example.common.Bound
import org.example.common.BoundFlag
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
            .firstOrNull {
                if (lower.flag == BoundFlag.UNBOUNDED) {
                    return@firstOrNull true
                }

                if (lower.flag == BoundFlag.INCLUDED) {
                    return@firstOrNull it.key >= lower.value
                }

                it.key > lower.value
            }
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
        current = if (iter.hasNext()) iter.next() else null

        if (current != null && upper.flag != BoundFlag.UNBOUNDED) {
            when {
                current!!.key > upper.value -> current = null
                current!!.key == upper.value && upper.flag == BoundFlag.EXCLUDED -> current = null
            }
        }
    }
}
