package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.*

/**
 * Utility object for encoding and decoding block metadata.
 */
object BlockMetaUtil {

    /**
     * Encodes a list of block metadata and a maximum timestamp into a buffer.
     *
     * @param blockMeta the list of block metadata to encode
     * @param maxTimestamp the maximum timestamp to encode
     * @param buf the buffer to encode the metadata into
     */
    fun encodeBlockMeta(blockMeta: List<BlockMeta>, maxTimestamp: Long, buf: ComparableByteArray) {
        val originalLength = buf.size()

        // number of blocks
        buf += blockMeta.size.toU32ByteArray()

        // add each block
        for (meta in blockMeta) {
            buf += meta.offset.toU32ByteArray()
            buf += meta.firstKey.size().toU16ByteArray()
            buf += meta.firstKey.bytes
            buf += meta.firstKey.timestamp().toU64ByteArray()
            buf += meta.lastKey.size().toU16ByteArray()
            buf += meta.lastKey.bytes
            buf += meta.lastKey.timestamp().toU64ByteArray()
        }

        // add timestamp
        buf += maxTimestamp.toU64ByteArray()

        // add hash
        val checksumCalculationStartOffset = originalLength + SIZE_OF_U32_IN_BYTE
        buf += crcHash(buf.slice(checksumCalculationStartOffset..<buf.size())).toU32ByteArray()
    }

    /**
     * Decodes block metadata from a buffer.
     *
     * @param buf the buffer containing the encoded metadata
     * @return the decoded block metadata and maximum timestamp
     * @throws IllegalStateException if the checksum does not match
     */
    fun decodeBlockMeta(buf: ComparableByteArray): BlockMetaDecodedResult {
        val blockMeta = mutableListOf<BlockMeta>()

        var currentOffset = 0
        // retrieve number of blocks
        val numberOfBlocks = buf.slice(currentOffset, currentOffset + SIZE_OF_U32_IN_BYTE).toU32Int()

        currentOffset += SIZE_OF_U32_IN_BYTE
        // to check hash value, we minus SIZE_OF_U32_IN_BYTES to exclude checksum
        val checksum = crcHash(buf.slice(currentOffset..<buf.size() - SIZE_OF_U32_IN_BYTE))

        // decode blockMetas
        for (i in 0 until numberOfBlocks) {
            val offset = buf.slice(currentOffset, currentOffset + SIZE_OF_U32_IN_BYTE).toU32Int()
            currentOffset += SIZE_OF_U32_IN_BYTE

            val firstKeyLength = buf.slice(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
            currentOffset += SIZE_OF_U16_IN_BYTE
            val firstKey = buf.slice(currentOffset, currentOffset + firstKeyLength)
            currentOffset += firstKeyLength
            val firstKeyTimestamp = buf.slice(currentOffset, currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
            currentOffset += SIZE_OF_U64_IN_BYTE

            val lastKeyLength = buf.slice(currentOffset, currentOffset + SIZE_OF_U16_IN_BYTE).toU16Int()
            currentOffset += SIZE_OF_U16_IN_BYTE
            val lastKey = buf.slice(currentOffset, currentOffset + lastKeyLength)
            currentOffset += lastKeyLength
            val lastKeyTimestamp = buf.slice(currentOffset, currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
            currentOffset += SIZE_OF_U64_IN_BYTE

            blockMeta.add(
                BlockMeta(
                    offset = offset,
                    firstKey = TimestampedKey(bytes = firstKey, timestamp = firstKeyTimestamp),
                    lastKey = TimestampedKey(bytes = lastKey, timestamp = lastKeyTimestamp)
                )
            )
        }

        val maxTimestamp = buf.slice(currentOffset, currentOffset + SIZE_OF_U64_IN_BYTE).toU64Long()
        currentOffset += SIZE_OF_U64_IN_BYTE

        val savedChecksum = buf.slice(currentOffset, currentOffset + SIZE_OF_U32_IN_BYTE).toU32Int()
        if (checksum != savedChecksum) {
            throw IllegalStateException("Calculated checksum($checksum) != saved checksum($savedChecksum)")
        }

        return BlockMetaDecodedResult(
            blockMeta = blockMeta,
            maxTimestamp = maxTimestamp
        )
    }
}
