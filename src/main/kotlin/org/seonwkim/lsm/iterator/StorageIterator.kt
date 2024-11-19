package org.seonwkim.lsm.iterator

import org.seonwkim.common.ComparableByteArray
import org.seonwkim.lsm.block.BlockIterator
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
    }

    fun seekToFirst() {
        val (blockIdx, blockIter) = seekToFirstInner(table)
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
