package org.seonwkim.lsm.block

import org.seonwkim.common.*

object BlockUtil {
    fun getFirstKey(block: Block): TimestampedKey {
        var currentOffset = 0
        currentOffset += SIZE_OF_U16_IN_BYTE
        val keyLength = block.data.slice(currentOffset..<currentOffset + 2).toU16Int()
        currentOffset += SIZE_OF_U16_IN_BYTE
        val keyBytes = block.data.slice(currentOffset..<currentOffset + keyLength)
        currentOffset += keyLength
        return TimestampedKey(
            bytes = keyBytes,
            timestamp = block.data.slice(currentOffset..<currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
        )
    }

    fun encode(block: Block): ComparableByteArray {
        val offsetLength = block.offsets.size() / SIZE_OF_U16_IN_BYTE
        return block.data + block.offsets + offsetLength.toU16ByteArray()
    }

    fun decode(data: ComparableByteArray): Block {
        val entryOffsetLength = data.slice(data.size() - SIZE_OF_U16_IN_BYTE, data.size()).toU16Int()

        val dataEnd = data.size() - SIZE_OF_U16_IN_BYTE - entryOffsetLength * SIZE_OF_U16_IN_BYTE
        return Block(
            data = data.slice(0, dataEnd),
            offsets = data.slice(dataEnd, data.size() - SIZE_OF_U16_IN_BYTE)
        )
    }
}
