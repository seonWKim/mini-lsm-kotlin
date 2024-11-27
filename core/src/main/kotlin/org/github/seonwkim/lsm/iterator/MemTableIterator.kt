package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.*
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
                val b = when (lower) {
                    is Unbounded -> true
                    is Included -> it.key.bytes >= lower.key
                    is Excluded -> it.key.bytes > lower.key
                }
                b
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
            is Unbounded -> true
            is Included -> current!!.key.bytes <= upper.key
            is Excluded -> current!!.key.bytes < upper.key
        }
    }

    override fun next() {
        current = if (iter.hasNext()) iter.next() else null

        if (current != null) {
            when (upper) {
                is Included -> {
                    if (current!!.key.bytes > upper.key) current = null
                }
                is Excluded -> {
                    if (current!!.key.bytes >= upper.key) current = null
                }
                Unbounded -> {}
            }
        }
    }

    override fun copy(): StorageIterator {
        return MemTableIterator(memTable, lower, upper)
    }
}
