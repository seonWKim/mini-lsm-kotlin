package org.seonwkim.lsm.iterator

import org.seonwkim.common.Bound
import org.seonwkim.common.BoundFlag
import org.seonwkim.common.ComparableByteArray
import org.seonwkim.common.TimestampedKey
import org.seonwkim.lsm.memtable.MemTable
import org.seonwkim.lsm.memtable.MemtableValue
import org.seonwkim.lsm.memtable.isDeleted

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
                if (lower.flag == BoundFlag.UNBOUNDED) {
                    return@firstOrNull true
                }

                if (lower.flag == BoundFlag.INCLUDED) {
                    return@firstOrNull it.key.bytes >= lower.value
                }

                it.key.bytes > lower.value
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

        if (current != null && upper.flag != BoundFlag.UNBOUNDED) {
            when {
                current!!.key.bytes > upper.value -> current = null
                current!!.key.bytes == upper.value && upper.flag == BoundFlag.EXCLUDED -> current = null
            }
        }
    }

    override fun copy(): StorageIterator {
        return MemTableIterator(memTable, lower, upper)
    }
}
