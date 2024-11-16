package org.example.lsm.memtable.iterator

import org.example.common.ComparableByteArray
import java.util.*

class MergeIterator(
    private val iterators: List<StorageIterator>
) : StorageIterator {

    private val priorityQueue = TreeMap<ComparableByteArray, MergeIteratorValue>()
    private var current: Map.Entry<ComparableByteArray, MergeIteratorValue>? = null

    data class MergeIteratorValue(
        val iteratorsIdx: Int,
        val value: ComparableByteArray,
    )

    init {
        for ((idx, iter) in this.iterators.withIndex()) {
            if (iter.isValid()) {
                priorityQueue.computeIfAbsent(iter.key()) { MergeIteratorValue(idx, iter.value()) }
                iter.next()
            }
        }

        next()
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
        val currentIdx = current?.value?.iteratorsIdx ?: return
        val currentIter = iterators[currentIdx]
        if (currentIter.isValid()) {
            priorityQueue[currentIter.key()]?.let { (prevIdx, _) ->
                if (currentIdx < prevIdx) {
                    priorityQueue[currentIter.key()] = MergeIteratorValue(currentIdx, currentIter.value())
                }
            }
        }

        current = priorityQueue.pollFirstEntry()
        if (current == null) return

        while (priorityQueue.isNotEmpty() && current!!.key == priorityQueue.firstEntry().key) {
            val nextEntry = priorityQueue.pollFirstEntry() ?: break
            val nextIdx = nextEntry.value.iteratorsIdx
            val nextIter = iterators[nextIdx]

            nextIter.takeIf { it.isValid() }?.let {
                priorityQueue[nextEntry.key]?.let { (prevIdx, _) ->
                    if (nextIdx < prevIdx) {
                        priorityQueue[nextEntry.key] = MergeIteratorValue(nextIdx, nextIter.value())
                    }
                }
            }

            if (nextIdx > current!!.value.iteratorsIdx) {
                current = nextEntry
            }
        }
    }

    override fun copy(): StorageIterator {
        return MergeIterator(iterators)
    }
}
