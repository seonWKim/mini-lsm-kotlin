package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.memtable.isDeleted

class MemTableIterator(
    private val memTable: MemTable,
    lower: Bound,
    upper: Bound
) : StorageIterator {

    private var current: Map.Entry<TimestampedKey, MemtableValue>? = null
    private val iter: Iterator<Map.Entry<TimestampedKey, MemtableValue>>
    private val lower: Bound
    private val upper: Bound

    init {
        val iter = memTable.map.iterator()
        current = iter.asSequence()
            .firstOrNull {
                when (lower) {
                    is Bound.Unbounded -> true
                    is Bound.Included -> it.key.bytes >= lower.value
                    is Bound.Excluded -> it.key.bytes > lower.value
                }
            }
        this.iter = iter
        this.lower = lower
        this.upper = upper
    }

    override fun key(): ComparableByteArray {
        return current?.key?.bytes
            ?: throw Error("Use isValid() function before calling this function")
    }

    override fun value(): ComparableByteArray {
        return current?.value?.value
            ?: throw Error("Use isValid() function before calling this function")
    }

    override fun isValid(): Boolean {
        if (current == null) return false

        return when (upper) {
            is Bound.Unbounded -> true
            is Bound.Included -> current!!.key.bytes <= upper.value
            is Bound.Excluded -> current!!.key.bytes < upper.value
        }
    }

    override fun isDeleted(): Boolean {
        return current?.value.isDeleted()
    }

    override fun next() {
        current = if (iter.hasNext()) iter.next() else null

        if (current != null) {
            when (upper) {
                is Bound.Included -> {
                    if (current!!.key.bytes > upper.value) current = null
                }
                is Bound.Excluded -> {
                    if (current!!.key.bytes >= upper.value) current = null
                }
                Bound.Unbounded -> {}
            }
        }
    }

    override fun copy(): StorageIterator {
        return MemTableIterator(memTable, lower, upper)
    }
}
