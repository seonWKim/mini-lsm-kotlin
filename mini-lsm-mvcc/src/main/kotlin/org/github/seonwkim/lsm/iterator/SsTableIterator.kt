package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.TimestampedByteArray
import org.github.seonwkim.lsm.block.BlockIterator
import org.github.seonwkim.lsm.sstable.Sstable

/**
 * SsTableIterator is an iterator for traversing entries in an SSTable.
 * It supports iteration over key-value pairs stored in the SSTable.
 *
 * @property table the SSTable to iterate over
 * @property blockIter the current block iterator
 * @property blockIdx the current block index
 */
class SsTableIterator(
    private val table: Sstable,
    private var blockIter: BlockIterator,
    private var blockIdx: Int
) : StorageIterator {

    companion object {
        /**
         * Seeks to the first entry in the SSTable.
         *
         * @param table the SSTable to iterate over
         * @return a pair containing the block index and block iterator
         */
        fun seekToFirstInner(table: Sstable): Pair<Int, BlockIterator> {
            return Pair(0, BlockIterator.createAndSeekToFirst(table.readBlockCached(0)))
        }

        /**
         * Creates an SsTableIterator and seeks to the first entry.
         *
         * @param table the SSTable to iterate over
         * @return a new [SsTableIterator]
         */
        fun createAndSeekToFirst(table: Sstable): SsTableIterator {
            val (blockIdx, blockIter) = seekToFirstInner(table)
            return SsTableIterator(
                table = table,
                blockIter = blockIter,
                blockIdx = blockIdx
            )
        }

        /**
         * Seeks to the specified key in the SSTable.
         *
         * @param table the SSTable to iterate over
         * @param key the key to seek to
         * @return a pair containing the block index and block iterator
         */
        fun seekToKeyInner(table: Sstable, key: TimestampedByteArray): Pair<Int, BlockIterator> {
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

        /**
         * Creates an SsTableIterator and seeks to the specified key.
         *
         * @param table the SSTable to iterate over
         * @param key the key to seek to
         * @return a new [SsTableIterator]
         */
        fun createAndSeekToKey(table: Sstable, key: TimestampedByteArray): SsTableIterator {
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

    fun seekToKey(key: TimestampedByteArray) {
        val (blockIdx, blockIter) = seekToKeyInner(table, key)
        this.blockIdx = blockIdx
        this.blockIter = blockIter
    }

    override fun key(): TimestampedByteArray {
        return blockIter.key()
    }

    override fun value(): TimestampedByteArray {
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
