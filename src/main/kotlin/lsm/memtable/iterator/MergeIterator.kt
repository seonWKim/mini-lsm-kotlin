package org.example.lsm.memtable.iterator

import org.example.common.ComparableByteArray
import java.util.*

class MergeIterator(
    private val iterators: List<StorageIterator>
) : StorageIterator {
    data class PriorityQueueKey(
        val idx: Int,
        val iterator: StorageIterator,
    ) : Comparable<PriorityQueueKey> {
        override fun compareTo(other: PriorityQueueKey): Int {
            if (iterator.key() == other.iterator.key()) {
                return idx - other.idx
            }

            return iterator.key().compareTo(other.iterator.key())
        }
    }

    private val priorityQueue = PriorityQueue<PriorityQueueKey>()

    init {
        for ((idx, iter) in this.iterators.withIndex()) {
            if (iter.isValid()) {
                priorityQueue.add(PriorityQueueKey(idx, iter))
            }
        }
    }

    override fun key(): ComparableByteArray {
        return current()?.iterator?.key()
            ?: throw IllegalCallerException("Use isValid() function before calling this function")
    }

    override fun value(): ComparableByteArray {
        return current()?.iterator?.value()
            ?: throw IllegalCallerException("Use isValid() function before calling this function")
    }

    override fun isValid(): Boolean {
        return current() != null
    }

    override fun next() {
        val currKey = current()?.iterator?.key() ?: return
        while (priorityQueue.isNotEmpty()) {
            if (currKey == priorityQueue.first().iterator.key()) {
                val (nextIdx, nextIter) = priorityQueue.poll()
                nextIter.next()
                if (nextIter.isValid()) {
                    priorityQueue.add(PriorityQueueKey(nextIdx, nextIter))
                }
            } else {
                break
            }
        }
    }

    private fun current(): PriorityQueueKey? {
        return priorityQueue.firstOrNull()
    }

    override fun copy(): StorageIterator {
        return MergeIterator(iterators)
    }
}
