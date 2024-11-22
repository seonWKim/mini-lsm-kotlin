package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.BoundV2
import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.memtable.isDeleted

class MemTableIterator(
    private val memTable: MemTable,
    lower: BoundV2,
    upper: BoundV2
) : StorageIterator {

    private var current: Map.Entry<TimestampedKey, MemtableValue>? = null
    private val iter: Iterator<Map.Entry<TimestampedKey, MemtableValue>>
    private val lower: BoundV2
    private val upper: BoundV2

    init {
        val iter = memTable.map.iterator()
        current = iter.asSequence()
            .firstOrNull {
                when (lower) {
                    is BoundV2.Unbounded -> true
                    is BoundV2.Included -> it.key.bytes >= lower.value
                    is BoundV2.Excluded -> it.key.bytes > lower.value
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
        return current != null
    }

    override fun isDeleted(): Boolean {
        return current?.value.isDeleted()
    }

    override fun next() {
        current = if (iter.hasNext()) iter.next() else null

        if (current != null) {
            when (upper) {
                is BoundV2.Included -> {
                    if (current!!.key.bytes > upper.value) current = null
                }
                is BoundV2.Excluded -> {
                    if (current!!.key.bytes >= upper.value) current = null
                }
                BoundV2.Unbounded -> {}
            }
        }
    }

    override fun copy(): StorageIterator {
        return MemTableIterator(memTable, lower, upper)
    }
}
