package org.github.seonwkim.lsm.iterator

import mu.KotlinLogging
import org.github.seonwkim.common.TimestampedByteArray
import java.util.*

/**
 * MergeIterator merges multiple iterators of the same type.
 *
 * @param T the type of StorageIterator being merged
 * @property iterators the list of iterators to be merged
 */
class MergeIterator<T : StorageIterator>(
    private val iterators: List<T>
) : StorageIterator {

    private val log = KotlinLogging.logger { }

    /**
     * Data class representing a key in the priority queue.
     *
     * @property idx the index of the iterator in the list
     * @property iterator the iterator instance
     */
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

    override fun key(): TimestampedByteArray {
        return current()?.iterator?.key()
            ?: throw Error("Use isValid() function before calling this function")
    }

    override fun value(): TimestampedByteArray {
        return current()?.iterator?.value()
            ?: throw Error("Use isValid() function before calling this function")
    }

    override fun isValid(): Boolean {
        return current()?.iterator?.isValid() == true
    }

    override fun next() {
        val currIter = current()?.iterator ?: return
        val currKey = currIter.key()
        removeSameKeys(currKey.copy())
    }

    /**
     * Removes entries with the same key from the priority queue.
     *
     * @param key the key to be removed
     */
    private fun removeSameKeys(key: TimestampedByteArray) {
        while (priorityQueue.isNotEmpty()) {
            if (priorityQueue.first().iterator.key() == key) {
                val (nextIdx, nextIter) = priorityQueue.poll()
                runCatching { nextIter.next() }.onFailure { e ->
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
