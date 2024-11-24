package org.github.seonwkim.lsm.iterator

import mu.KotlinLogging
import org.github.seonwkim.common.ComparableByteArray
import java.util.*

/**
 * Merge iterators of same type
 */
class MergeIterator<T : StorageIterator>(
    private val iterators: List<T>
) : StorageIterator {

    private val log = KotlinLogging.logger { }

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
            ?: throw Error("Use isValid() function before calling this function")
    }

    override fun value(): ComparableByteArray {
        return current()?.iterator?.value()
            ?: throw Error("Use isValid() function before calling this function")
    }

    override fun isValid(): Boolean {
        return current() != null
    }

    override fun next() {
        val currKey = current()?.iterator?.key() ?: return
        while (priorityQueue.isNotEmpty()) {
            if (currKey == priorityQueue.first().iterator.key()) {
                val (nextIdx, nextIter) = priorityQueue.poll()
                runCatching { nextIter.next() }
                    .onFailure { e ->
                        log.error { "error while calling next(): $e" }
                        throw e
                    }
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
        return MergeIterator(iterators = iterators.map { it.copy() })
    }

    override fun numActiveIterators(): Int {
        return iterators.sumOf { it.numActiveIterators() }
    }
}
