package org.github.seonwkim.lsm.sstable

import org.github.seonwkim.common.TimestampedByteArray
import org.github.seonwkim.common.SIZE_OF_U32_IN_BYTE
import org.github.seonwkim.common.crcHash
import org.github.seonwkim.common.toU32Int
import org.github.seonwkim.lsm.block.Block
import org.github.seonwkim.lsm.block.BlockMeta
import org.github.seonwkim.lsm.block.BlockMetaUtil
import org.github.seonwkim.lsm.block.BlockUtil
import org.github.seonwkim.lsm.bloom.Bloom
import org.github.seonwkim.lsm.bloom.BloomUtil

class Sstable(
    val file: SsTableFile,
    val blockMeta: List<BlockMeta>,
    val blockMetaOffset: Int,
    val id: Int,
    val blockCache: BlockCache?,
    val firstKey: TimestampedByteArray,
    val lastKey: TimestampedByteArray,
    val bloom: Bloom?,
    // val maxTs: Long
) {

    companion object {
        fun openForTest(file: SsTableFile): Sstable {
            return open(0, null, file)
        }

        fun open(id: Int, blockCache: BlockCache?, file: SsTableFile): Sstable {
            val len = file.size
            val rawBloomOffset = file.read(len - SIZE_OF_U32_IN_BYTE, SIZE_OF_U32_IN_BYTE)
            val bloomOffset = rawBloomOffset.toU32Int().toLong()
            val rawBloom = file.read(bloomOffset, (len - 4 - bloomOffset).toInt())
            val bloom = BloomUtil.decode(rawBloom)
            val rawMetaOffset = file.read(bloomOffset - 4, SIZE_OF_U32_IN_BYTE)
            val blockMetaOffset = rawMetaOffset.toU32Int().toLong()
            val rawMeta = file.read(blockMetaOffset, (bloomOffset - 4 - blockMetaOffset).toInt())
            val (blockMeta) = BlockMetaUtil.decodeBlockMeta(rawMeta)
            // val (blockMeta, maxTs) = BlockMetaUtil.decodeBlockMeta(rawMeta)
            return Sstable(
                file = file,
                blockMeta = blockMeta,
                blockMetaOffset = blockMetaOffset.toInt(),
                id = id,
                blockCache = blockCache,
                firstKey = blockMeta.first().firstKey,
                lastKey = blockMeta.last().lastKey,
                bloom = bloom,
                // maxTs = maxTs
            )
        }
    }

    /**
     * Read a block from disk, with block cache.
     */
    fun readBlockCached(blockIdx: Int): Block {
        return blockCache?.let {
            it.getOrDefault(BlockCacheKey(id, blockIdx)) { readBlock(blockIdx) }
        } ?: readBlock(blockIdx)
    }

    private fun readBlock(blockIdx: Int): Block {
        val offset = blockMeta[blockIdx].offset
        val offsetEnd = blockMeta.getOrNull(blockIdx + 1)?.offset ?: blockMetaOffset
        val blockLen = offsetEnd - offset - SIZE_OF_U32_IN_BYTE
        val blockDataWithChecksum = file.read(offset.toLong(), (offsetEnd - offset))
        val blockData = blockDataWithChecksum.slice(0, blockLen)
        val checksum = blockDataWithChecksum.slice(blockLen, blockDataWithChecksum.size()).toU32Int()
        if (checksum != crcHash(blockData)) {
            throw Error("Block checksum mismatch")
        }

        return BlockUtil.decode(blockData)
    }

    /**
     * Find the block that may contain [key].
     */
    fun findBlockIdx(key: TimestampedByteArray): Int {
        return blockMeta.binarySearch { it.firstKey.compareTo(key) }
            .let { idx ->
                if (idx < 0) maxOf(-idx - 2, 0)
                else idx
            }
    }

    /**
     * Get the number of blocks.
     */
    fun numberOfBlocks(): Int {
        return blockMeta.size
    }
}
