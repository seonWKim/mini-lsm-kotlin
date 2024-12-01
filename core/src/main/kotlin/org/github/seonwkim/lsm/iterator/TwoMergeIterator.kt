package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.ComparableByteArray

/**
 * TwoMergeIterator is used to merge two iterators of different types.
 * It supports iteration over key-value pairs from both iterators in sorted order.
 *
 * @param A the type of the first iterator
 * @param B the type of the second iterator
 * @property first the first iterator
 * @property second the second iterator
 * @property chooseFirst a flag indicating whether to choose the first iterator
 */
class TwoMergeIterator<A : StorageIterator, B : StorageIterator>(
    private val first: A,
    private val second: B,
    private var chooseFirst: Boolean
) : StorageIterator {
    companion object {
        fun <A : StorageIterator, B : StorageIterator> create(first: A, second: B): TwoMergeIterator<A, B> {
            val iter = TwoMergeIterator(
                first = first,
                second = second,
                chooseFirst = false
            )
            iter.skipSecond()
            iter.chooseFirst = chooseFirst(iter.first, iter.second)
            return iter
        }

        fun chooseFirst(first: StorageIterator, second: StorageIterator): Boolean {
            if (!first.isValid()) {
                return false
            }

            if (!second.isValid()) {
                return true
            }

            return first.key() < second.key()
        }
    }

    fun skipSecond() {
        if (first.isValid() && second.isValid() && first.key() == second.key()) {
            second.next()
        }
    }

    override fun key(): ComparableByteArray {
        return if (chooseFirst) {
            require(first.isValid()) { "first iterator is not valid" }
            first.key()
        } else {
            require(second.isValid()) { "second iterator is not valid" }
            second.key()
        }
    }

    override fun value(): ComparableByteArray {
        return if (chooseFirst) {
            first.value()
        } else {
            second.value()
        }
    }

    override fun isValid(): Boolean {
        return if (chooseFirst) {
            first.isValid()
        } else {
            second.isValid()
        }
    }

    override fun next() {
        if (chooseFirst) {
            first.next()
        } else {
            second.next()
        }
        skipSecond()
        chooseFirst = chooseFirst(first, second)
    }

    override fun copy(): StorageIterator {
        throw Error("not implemented")
    }

    override fun numActiveIterators(): Int {
        return first.numActiveIterators() + second.numActiveIterators()
    }
}
