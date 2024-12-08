package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.*

/**
 * An iterator for traversing key-value pairs in a block.
 *
 * @property block the block to iterate over
 * @property key the key of the current iterator position
 * @property valueRange the range of the value in the block data corresponding to the current key
 * @property idx the current index at the iterator position
 * @property firstKey the first key in the block
 */
class BlockIterator(
    private val block: Block,
    private val key: TimestampedByteArray,
    private var valueRange: IntRange,
    private var idx: Int,
    private val firstKey: TimestampedByteArray
) {
    companion object {
        /**
         * Creates a new `BlockIterator` for the given block.
         *
         * @param block the block to iterate over
         * @return a new `BlockIterator`
         */
        private fun new(block: Block): BlockIterator {
            return BlockIterator(
                firstKey = BlockUtil.getFirstKey(block),
                block = block,
                key = TimestampedByteArray.new(),
                valueRange = IntRange.EMPTY,
                idx = 0
            )
        }

        /**
         * Creates a new `BlockIterator` and seeks to the first key.
         *
         * @param block the block to iterate over
         * @return a new `BlockIterator` positioned at the first key
         */
        fun createAndSeekToFirst(block: Block): BlockIterator {
            val iter = new(block)
            iter.seekToFirst()
            return iter
        }

        /**
         * Creates a new `BlockIterator` and seeks to the specified key.
         *
         * @param block the block to iterate over
         * @param key the key to seek to
         * @return a new `BlockIterator` positioned at the specified key
         */
        fun createAndSeekToKey(block: Block, key: TimestampedByteArray): BlockIterator {
            val iter = new(block)
            iter.seekToKey(key)
            return iter
        }
    }

    /**
     * Returns the key at the current iterator position.
     *
     * @return the key at the current iterator position
     * @throws IllegalStateException if the iterator is invalid
     */
    fun key(): TimestampedByteArray {
        if (key.isEmpty()) {
            throw IllegalStateException("Invalid iterator")
        }
        return key
    }

    /**
     * Returns the value at the current iterator position.
     *
     * @return the value at the current iterator position
     * @throws IllegalStateException if the iterator is invalid
     */
    fun value(): ComparableByteArray {
        if (key.isEmpty()) {
            throw IllegalStateException("Invalid iterator")
        }
        return block.data.slice(valueRange)
    }

    /**
     * Checks if the iterator is valid.
     *
     * @return true if the iterator is valid, false otherwise
     */
    fun isValid(): Boolean {
        return !key.isEmpty()
    }

    /**
     * Advances the iterator to the next key-value pair.
     */
    fun next() {
        idx += SIZE_OF_U16_IN_BYTE
        seekTo(idx)
    }

    /**
     * Seeks the iterator to the first key-value pair.
     */
    fun seekToFirst() {
        seekTo(0)
    }

    /**
     * Seeks the iterator to the specified index.
     *
     * @param idx the index to seek to
     */
    private fun seekTo(idx: Int) {
        if (idx % 2 != 0) {
            throw IllegalStateException("Should be even number idx($idx)")
        }

        if (idx >= block.offsets.size()) {
            key.clear()
            valueRange = IntRange.EMPTY
            return
        }
        val offset = (block.offsets.slice(idx, idx + SIZE_OF_U16_IN_BYTE)).toU16Int()
        seekToOffset(offset)
        this.idx = idx
    }

    /**
     * Seeks to the specified offset and updates the current key and value.
     *
     * @param offset the offset to seek to
     */
    private fun seekToOffset(offset: Int) {
        var currentOffset = offset
        val overlapLength = block.data.slice(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
        currentOffset += SIZE_OF_U16_IN_BYTE
        val keyLength = block.data.slice(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
        currentOffset += SIZE_OF_U16_IN_BYTE
        val key = block.data.slice(currentOffset, currentOffset + keyLength)
        this.key.clear()
        this.key += this.firstKey.slice(0..<overlapLength)// overlapping key
        this.key += key // non-overlapping key
        currentOffset += keyLength

        // TODO(TIMESTAMP: retrieve u64 timestamp from block and set the timestamp to key)
        block.data.slice(currentOffset, currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long().let { timestamp ->
            this.key().setTimestamp(timestamp)
        }
        currentOffset += SIZE_OF_U64_IN_BYTE
        val valueLength = block.data.slice(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
        currentOffset += SIZE_OF_U16_IN_BYTE
        val valueOffsetBegin = currentOffset
        val valueOffsetEnd = valueOffsetBegin + valueLength
        this.valueRange = valueOffsetBegin..<valueOffsetEnd
    }

    /**
     * Seeks the iterator to the specified key.
     *
     * @param key the key to seek to
     */
    fun seekToKey(key: TimestampedByteArray) {
        var low = 0
        var high = block.offsets.size()
        while (low < high) {
            val mid = low + (high - low) / 2  // Ensure mid is even
            seekTo(if (mid % 2 == 0) mid else mid - 1)
            if (!isValid()) {
                throw IllegalStateException("invalid")
            }

            val comparedResult = key().compareTo(key)
            when {
                comparedResult < 0 -> {
                    low = if (mid % 2 == 0) mid + 2 else mid + 1
                } // Move to the next even number
                comparedResult > 0 -> {
                    high = if (mid % 2 == 0) mid else mid - 1
                }

                else -> return
            }
        }

        seekTo(low)
    }
}
