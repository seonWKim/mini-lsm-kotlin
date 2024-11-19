package org.seonWKim.lsm.block

import org.seonWKim.common.*

object BlockMetaUtil {
    fun encodeBlockMeta(blockMeta: List<BlockMeta>, maxTimestamp: Long, buf: MutableList<Byte>) {
        val originalLength = buf.size

        // number of blocks
        buf.addAll(blockMeta.size.toU32ByteArray())

        // add each block
        for (meta in blockMeta) {
            buf.addAll(meta.offset.toU32ByteArray())
            buf.addAll(meta.firstKey.size().toU16ByteArray())
            buf.addAll(meta.firstKey.key.array)
            buf.addAll(meta.firstKey.timestamp().toU64ByteArray())
            buf.addAll(meta.lastKey.size().toU16ByteArray())
            buf.addAll(meta.lastKey.key.array)
            buf.addAll(meta.lastKey.timestamp().toU64ByteArray())
        }

        // add timestamp
        buf.addAll(maxTimestamp.toU64ByteArray())

        // add hash
        val checksumCalculationStartOffset = originalLength + SIZE_OF_U32_IN_BYTE
        buf.addAll(crcHash(buf.slice(checksumCalculationStartOffset..<buf.size)).toU32ByteArray())
    }

    fun decodeBlockMeta(buf: List<Byte>): BlockMetaDecodedResult {
        val blockMeta = mutableListOf<BlockMeta>()

        var currentOffset = 0
        // retrieve number of blocks
        val numberOfBlocks = buf.subList(currentOffset, currentOffset + SIZE_OF_U32_IN_BYTE).toU32Int()

        // to check hash value
        currentOffset += SIZE_OF_U32_IN_BYTE
        val checksum = crcHash(buf.slice(currentOffset..<buf.size))

        // decode blockMetas
        for (i in 0 until numberOfBlocks) {
            val offset = buf.subList(currentOffset, currentOffset + SIZE_OF_U32_IN_BYTE).toU32Int()
            currentOffset += SIZE_OF_U32_IN_BYTE

            val firstKeyLength = buf.subList(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
            currentOffset += SIZE_OF_U16_IN_BYTE
            val firstKey = ComparableByteArray(buf.subList(currentOffset, currentOffset + firstKeyLength))
            currentOffset += firstKeyLength
            val firstKeyTimestamp = buf.subList(currentOffset, currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
            currentOffset += SIZE_OF_U64_IN_BYTE

            val lastKeyLength = buf.subList(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
            currentOffset += SIZE_OF_U16_IN_BYTE
            val lastKey = ComparableByteArray(buf.subList(currentOffset, currentOffset + lastKeyLength))
            currentOffset += lastKeyLength
            val lastKeyTimestamp = buf.subList(currentOffset, currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
            currentOffset += SIZE_OF_U64_IN_BYTE

            blockMeta.add(
                BlockMeta(
                    offset = offset,
                    firstKey = BlockKey(key = firstKey, timestamp = firstKeyTimestamp),
                    lastKey = BlockKey(key = lastKey, timestamp = lastKeyTimestamp)
                )
            )
        }

        val maxTimestamp = buf.subList(currentOffset, currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
        currentOffset += SIZE_OF_U64_IN_BYTE

        val savedChecksum = buf.subList(currentOffset, currentOffset + SIZE_OF_U32_IN_BYTE).toU32Int()
        if (checksum != savedChecksum) {
            throw IllegalStateException("Calculated checksum($checksum) != saved checksum($savedChecksum)")
        }

        return BlockMetaDecodedResult(
            blockMeta = blockMeta,
            maxTimestamp = maxTimestamp
        )
    }
}
