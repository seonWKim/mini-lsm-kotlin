package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.*

/**
 * Utility object for encoding, decoding, and retrieving keys from blocks.
 */
object BlockUtil {

    /**
     * Retrieves the first key from a block.
     *
     * @param block the block to retrieve the first key from
     * @return the first key in the block
     */
    fun getFirstKey(block: Block): TimestampedByteArray {
        var currentOffset = 0
        currentOffset += SIZE_OF_U16_IN_BYTE
        val keyLength = block.data.slice(currentOffset..<currentOffset + 2).toU16Int()
        currentOffset += SIZE_OF_U16_IN_BYTE
        val keyBytes = block.data.slice(currentOffset..<currentOffset + keyLength)
        currentOffset += keyLength
        return TimestampedByteArray(
            bytes = keyBytes.copyBytes(),
            timestamp = block.data.slice(currentOffset..<currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
        )
    }

    /**
     * Encodes a block into a [TimestampedByteArray].
     *
     * @param block the block to encode
     * @return the encoded block as a [TimestampedByteArray]
     */
    fun encode(block: Block): TimestampedByteArray {
        val offsetLength = block.offsets.size() / SIZE_OF_U16_IN_BYTE
        return block.data + block.offsets + offsetLength.toU16ByteArray()
    }

    /**
     * Decodes a [TimestampedByteArray] into a block.
     *
     * @param data the [TimestampedByteArray] to decode
     * @return the decoded block
     */
    fun decode(data: TimestampedByteArray): Block {
        val entryOffsetLength = data.slice(data.size() - SIZE_OF_U16_IN_BYTE, data.size()).toU16Int()

        val dataEnd = data.size() - SIZE_OF_U16_IN_BYTE - entryOffsetLength * SIZE_OF_U16_IN_BYTE
        return Block(
            data = data.slice(0, dataEnd),
            offsets = data.slice(dataEnd, data.size() - SIZE_OF_U16_IN_BYTE)
        )
    }
}
