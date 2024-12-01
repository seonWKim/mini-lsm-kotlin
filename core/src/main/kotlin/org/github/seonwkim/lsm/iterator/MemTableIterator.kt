package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue

/**
 * Iterator for traversing the entries in a MemTable.
 * This iterator supports bounded iteration with specified lower and upper bounds.
 *
 * @property memTable the MemTable to iterate over
 * @property lower the lower bound for iteration
 * @property upper the upper bound for iteration
 */
class MemTableIterator private constructor(
    private val memTable: MemTable,
    private val lower: Bound,
    private val upper: Bound,
) : StorageIterator {
    private var current: Map.Entry<TimestampedKey, MemtableValue>? = null
    private val iter: Iterator<Map.Entry<TimestampedKey, MemtableValue>>

    init {
        val iter = memTable.map.iterator()
        current = iter.asSequence()
            .firstOrNull {
                when (lower) {
                    is Unbounded -> true
                    is Included -> it.key.bytes >= lower.key
                    is Excluded -> it.key.bytes > lower.key
                }
            }
        this.iter = iter
    }

    companion object {
        /**
         * Creates a new MemTableIterator.
         *
         * @param memTable the MemTable to iterate over
         * @param lower the lower bound for iteration
         * @param upper the upper bound for iteration
         * @return a new MemTableIterator
         */
        fun create(memTable: MemTable, lower: Bound, upper: Bound): MemTableIterator {
            return MemTableIterator(memTable, lower, upper)
        }    }

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
