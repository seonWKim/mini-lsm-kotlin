package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.lsm.block.BlockIterator
import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.lsm.sstable.Sstable

class SsTableIterator(
    private val table: Sstable,
    private var blockIter: BlockIterator,
    private var blockIdx: Int
) : StorageIterator {
    companion object {
        fun seekToFirstInner(table: Sstable): Pair<Int, BlockIterator> {
            return Pair(0, BlockIterator.createAndSeekToFirst(table.readBlockCached(0)))
        }

        fun createAndSeekToFirst(table: Sstable): SsTableIterator {
            val (blockIdx, blockIter) = seekToFirstInner(table)
            return SsTableIterator(
                table = table,
                blockIter = blockIter,
                blockIdx = blockIdx
            )
        }

        fun seekToKeyInner(table: Sstable, key: TimestampedKey): Pair<Int, BlockIterator> {
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

        fun createAndSeekToKey(table: Sstable, key: TimestampedKey): SsTableIterator {
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

    fun seekToKey(key: TimestampedKey) {
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
