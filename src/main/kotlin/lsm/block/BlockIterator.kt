package org.seonWKim.lsm.block

import org.seonWKim.common.ComparableByteArray
import org.seonWKim.common.SIZEOF_U16_IN_BYTE
import org.seonWKim.common.toUInt

class BlockIterator(
    private val block: Block,
    // key of current iterator position
    private val key: BlockKey,
    // value range in the block.data, which corresponds to the current key
    private var valueRange: IntRange,
    // current index at the iterator position
    private var idx: Int,
    // first key in the block
    private val firstKey: BlockKey
) {
    companion object {
        fun new(block: Block): BlockIterator {
            return BlockIterator(
                firstKey = BlockUtil.getFirstKey(block),
                block = block,
                key = BlockKey(ComparableByteArray.EMPTY),
                valueRange = IntRange.EMPTY,
                idx = 0
            )
        }

        fun createAndSeekToFirst(block: Block): BlockIterator {
            val iter = new(block)
            iter.seekToFirst()
            return iter
        }

        fun createAndSeekToKey(block: Block, key: BlockKey): BlockIterator {
            val iter = new(block)
            iter.seekToKey(key)
            return iter
        }
    }

    fun key(): BlockKey {
        if (key.isEmpty()) {
            throw IllegalStateException("Invalid iterator")
        }

        return key
    }

    fun value(): List<Byte> {
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
        if (idx >= block.offsets.size) {
            key.clear()
            valueRange = IntRange.EMPTY
            return
        }
        val offset = (block.offsets.subList(idx, idx + SIZEOF_U16_IN_BYTE)).toUInt()
        seekToOffset(offset)
        this.idx = idx
    }

    /**
     * Seek to the specified position and update the current [key] and [value]
     * Index update will be handled by the caller
     */
    fun seekToOffset(offset: Int) {
        var currentOffset = offset
        val overlapLength = block.data.subList(currentOffset, currentOffset + SIZEOF_U16_IN_BYTE).toUInt()
        currentOffset += SIZEOF_U16_IN_BYTE
        val keyLength = block.data.subList(currentOffset, currentOffset + SIZEOF_U16_IN_BYTE).toUInt()
        currentOffset += SIZEOF_U16_IN_BYTE
        val key = block.data.subList(currentOffset, currentOffset + keyLength)
        this.key.clear()
        this.key.append(this.key.slice(0..<overlapLength))
        this.key.append(key)
        currentOffset += keyLength
        val valueLength = block.data.subList(currentOffset, currentOffset + SIZEOF_U16_IN_BYTE).toUInt()
        val valueOffsetBegin = offset + SIZEOF_U16_IN_BYTE + SIZEOF_U16_IN_BYTE + keyLength + SIZEOF_U16_IN_BYTE
        val valueOffsetEnd = valueOffsetBegin + valueLength
        this.valueRange = valueOffsetBegin..<valueOffsetEnd
    }

    fun seekToKey(key: BlockKey) {
        var low = 0
        var high = block.offsets.size
        while (low < high) {
            val mid = low + (high - low) / 2
            seekTo(mid)
            if (!isValid()) {
                throw IllegalStateException("invalid")
            }

            val comparedResult = key().compareTo(key)
            when {
                comparedResult < 0 -> low = mid + 1
                comparedResult > 0 -> high = mid
                else -> return
            }
        }

        seekTo(low)
    }

    fun next() {
        idx += SIZEOF_U16_IN_BYTE
        seekTo(idx)
    }
}