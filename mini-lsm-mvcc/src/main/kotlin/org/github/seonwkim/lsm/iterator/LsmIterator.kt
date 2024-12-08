package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.*


/**
 * LsmIterator is an iterator for traversing entries in an LSM tree.
 * It supports iteration over key-value pairs with bounds checking.
 *
 * @property inner the inner iterator that merges multiple iterators
 * @property endBound the end bound for the iterator
 * @property isValid a flag indicating whether the iterator is valid
 */
class LsmIterator(
    private val inner: LsmIteratorInner,
    private val endBound: Bound,
    private var isValid: Boolean
) : StorageIterator {
    companion object {
        fun new(iter: LsmIteratorInner, endBound: Bound): LsmIterator {
            val lsmIterator = LsmIterator(
                inner = iter,
                endBound = endBound,
                isValid = iter.isValid()
            )
            lsmIterator.moveToNonDelete()
            return lsmIterator
        }
    }

    private fun nextInner() {
        inner.next()
        if (!inner.isValid()) {
            isValid = false
            return
        }

        when (endBound) {
            is Unbounded -> {}
            is Included -> isValid = inner.key() <= endBound.key
            is Excluded -> isValid = inner.key() < endBound.key
        }
    }

    fun moveToNonDelete() {
        while (isValid && inner.value().isEmpty()) {
            nextInner()
        }
    }

    override fun key(): TimestampedByteArray {
        return inner.key()
    }

    override fun value(): TimestampedByteArray {
        return inner.value()
    }

    override fun isValid(): Boolean {
        return isValid
    }

    override fun next() {
        nextInner()
        moveToNonDelete()
    }

    override fun copy(): StorageIterator {
        TODO("Not implemented")
    }

    override fun numActiveIterators(): Int {
        return inner.numActiveIterators()
    }

    override fun allKeyValues(): List<String> {
        val result = mutableListOf<String>()
        while (isValid()) {
            result.add("key: ${key()}, value: ${value()}")
            next()
        }
        return result
    }
}

typealias LsmIteratorInner = TwoMergeIterator<
        TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
        MergeIterator<SstConcatIterator>>
