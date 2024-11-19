package org.seonWKim.lsm.block

import org.seonWKim.common.*

object BlockUtil {
    fun getFirstKey(block: Block): BlockKey {
        var currentOffset = 0
        currentOffset += SIZE_OF_U16_IN_BYTE
        val keyLength = block.data.slice(currentOffset..<currentOffset + 2).toU16Int()
        currentOffset += SIZE_OF_U16_IN_BYTE
        val keyBytes = block.data.slice(currentOffset..<currentOffset + keyLength)
        currentOffset += keyLength
        return BlockKey(
            bytes = keyBytes,
            timestamp = block.data.slice(currentOffset..<currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
        )
    }
}
