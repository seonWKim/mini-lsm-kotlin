package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.*

class BlockIterator(
    private val block: Block,
    // key of current iterator position
    private val key: TimestampedKey,
    // value range in the block.data, which corresponds to the current key
    private var valueRange: IntRange,
    // current index at the iterator position
    private var idx: Int,
    // first key in the block
    private val firstKey: TimestampedKey
) {
    companion object {
        fun new(block: Block): BlockIterator {
            return BlockIterator(
                firstKey = BlockUtil.getFirstKey(block),
                block = block,
                key = TimestampedKey(ComparableByteArray.new()),
                valueRange = IntRange.EMPTY,
                idx = 0
            )
        }

        fun createAndSeekToFirst(block: Block): BlockIterator {
            val iter = new(block)
            iter.seekToFirst()
            return iter
        }

        fun createAndSeekToKey(block: Block, key: TimestampedKey): BlockIterator {
            val iter = new(block)
            iter.seekToKey(key)
            return iter
        }
    }

    fun key(): TimestampedKey {
        if (key.isEmpty()) {
            throw IllegalStateException("Invalid iterator")
        }

        return key
    }

    fun value(): ComparableByteArray {
        if (key.isEmpty()) {
            throw IllegalStateException("Invalid iterator")
        }

        return block.data.slice(valueRange)
    }

    fun isValid(): Boolean {
        return !key.isEmpty()
    }

    fun seekToFirst() {
        seekTo(0)
    }

    fun seekTo(idx: Int) {
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
     * Seek to the specified position and update the current [key] and [value]
     * Index update will be handled by the caller
     */
    private fun seekToOffset(offset: Int) {
        var currentOffset = offset
        val overlapLength = block.data.slice(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
        currentOffset += SIZE_OF_U16_IN_BYTE
        val keyLength = block.data.slice(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
        currentOffset += SIZE_OF_U16_IN_BYTE
        val key = block.data.slice(currentOffset, currentOffset + keyLength)
        this.key.clear()
        this.key.append(this.firstKey.slice(0..<overlapLength)) // overlapping key
        this.key.append(key) // non-overlapping key
        currentOffset += keyLength
        val timestamp = block.data.slice(currentOffset, currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
        this.key.setTimestamp(timestamp)
        currentOffset += SIZE_OF_U64_IN_BYTE
        val valueLength = block.data.slice(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
        val valueOffsetBegin = offset +
                SIZE_OF_U16_IN_BYTE + // (overlap length)'s length
                SIZE_OF_U16_IN_BYTE + // (key length)'s length
                SIZE_OF_U64_IN_BYTE + // timestamp length
                keyLength + // key length
                SIZE_OF_U16_IN_BYTE // (value length)'s length
        val valueOffsetEnd = valueOffsetBegin + valueLength
        this.valueRange = valueOffsetBegin..<valueOffsetEnd
    }

    fun seekToKey(key: TimestampedKey) {
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

    fun next() {
        idx += SIZE_OF_U16_IN_BYTE
        seekTo(idx)
    }
}
