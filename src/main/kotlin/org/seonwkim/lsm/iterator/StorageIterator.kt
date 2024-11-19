package org.seonwkim.lsm.iterator

import org.seonwkim.common.ComparableByteArray
import org.seonwkim.lsm.block.BlockIterator
import org.seonwkim.lsm.block.BlockKey
import org.seonwkim.lsm.sstable.SsTable

interface StorageIterator {
    // Get the current key
    fun key(): ComparableByteArray

    // Get the current value
    fun value(): ComparableByteArray

    // Check if the current iterator is valid
    fun isValid(): Boolean

    // Move to the next position
    fun next()

    fun copy(): StorageIterator

    // Number of underlying active iterators for this iterator
    fun numActiveIterators(): Int {
        return 1
    }

    // Check additional information
    fun meta(): IteratorMeta? = null
}

class SsTableIterator(
    private val table: SsTable,
    private var blockIter: BlockIterator,
    private var blockIdx: Int
) : StorageIterator {
    companion object {
        fun seekToFirstInner(table: SsTable): Pair<Int, BlockIterator> {
            return Pair(0, BlockIterator.createAndSeekToFirst(table.readBlockCached(0)))
        }

        fun createAndSeekToFirst(table: SsTable): SsTableIterator {
            val (blockIdx, blockIter) = seekToFirstInner(table)
            return SsTableIterator(
                table = table,
                blockIter = blockIter,
                blockIdx = blockIdx
            )
        }

        fun seekToKeyInner(table: SsTable, key: BlockKey): Pair<Int, BlockIterator> {
            var blockIdx = table.findBlockIdx(key)
            var blockIter = BlockIterator.createAndSeekToKey(table.readBlockCached(blockIdx), key)
            if (!blockIter.isValid()) {
                blockIdx += 1
                if (blockIdx < table.numberOfBlocks()) {
                    blockIter = BlockIterator.createAndSeekToFirst(table.readBlockCached(blockIdx))
                }
            }
            return Pair(blockIdx, blockIter)
        }

        fun createAndSeekToKey(table: SsTable, key: BlockKey): SsTableIterator {
            val (blockIdx, blockIter) = seekToKeyInner(table, key)
            return SsTableIterator(
                table = table,
                blockIter = blockIter,
                blockIdx = blockIdx
            )
        }
    }

    fun seekToFirst() {
        val (blockIdx, blockIter) = seekToFirstInner(table)
        this.blockIdx = blockIdx
        this.blockIter = blockIter
    }

    fun seekToKey(key: BlockKey) {
        val (blockIdx, blockIter) = seekToKeyInner(table, key)
        this.blockIdx = blockIdx
        this.blockIter = blockIter
    }

    override fun key(): ComparableByteArray {
        return blockIter.key().bytes
    }

    override fun value(): ComparableByteArray {
        return blockIter.value()
    }

    override fun isValid(): Boolean {
        return blockIter.isValid()
    }

    override fun next() {
        blockIter.next()
        if (!blockIter.isValid()) {
            blockIdx += 1
            if (blockIdx < table.numberOfBlocks()) {
                blockIter = BlockIterator.createAndSeekToFirst(
                    table.readBlockCached(blockIdx)
                )
            }
        }
    }

    override fun copy(): StorageIterator {
        throw Error("Not implemented")
    }
}

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
