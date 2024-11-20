package org.seonwkim.lsm.iterator

import org.seonwkim.common.ComparableByteArray

/**
 * Used to merge two iterators of different type.
 */
class TwoMergeIterator(
    private val first: StorageIterator,
    private val second: StorageIterator,
    private var chooseFirst: Boolean
) : StorageIterator {
    companion object {
        fun create(first: StorageIterator, second: StorageIterator): TwoMergeIterator {
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
}
