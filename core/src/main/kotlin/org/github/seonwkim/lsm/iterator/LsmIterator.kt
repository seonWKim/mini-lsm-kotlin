package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.ComparableByteArray

typealias LsmIteratorInner = TwoMergeIterator<
        TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
        MergeIterator<SstConcatIterator>>

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

    fun nextInner() {
        inner.next()
        if (!inner.isValid()) {
            isValid = false
            return
        }

        when (endBound) {
            is Bound.Unbounded -> {}
            is Bound.Included -> isValid = inner.key() <= endBound.key
            is Bound.Excluded -> isValid = inner.key() < endBound.key
        }
    }

    fun moveToNonDelete() {
        while (isValid && inner.value().isEmpty()) {
            nextInner()
        }
    }

    override fun key(): ComparableByteArray {
        return inner.key()
    }

    override fun value(): ComparableByteArray {
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
