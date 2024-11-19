package org.seonwkim.lsm.sstable

import org.seonwkim.common.SIZE_OF_U32_IN_BYTE
import org.seonwkim.common.crcHash
import org.seonwkim.common.toU32Int
import org.seonwkim.lsm.block.*

class SsTable(
    val file: SsTableFile,
    val blockMeta: List<BlockMeta>,
    val blockMetaOffset: Int,
    val id: Int,
    val blockCache: BlockCache?,
    val firstKey: BlockKey,
    val lastKey: BlockKey,
    // private val bloom: Bloom?,
    val maxTs: Long
) {

    companion object {
        fun openForTest(file: SsTableFile): SsTable {
            return open(0, null, file)
        }

        fun open(id: Int, blockCache: BlockCache?, file: SsTableFile): SsTable {
            val len = file.size
            // TODO: we will have to decode bloom filter when bloom filter is supported
            val rawMetaOffset = file.read(len - 4, 4)
            val blockMetaOffset = rawMetaOffset.toU32Int().toLong()
            val rawMeta = file.read(blockMetaOffset, (len - 4 - blockMetaOffset).toInt())
            val (blockMeta, maxTs) = BlockMetaUtil.decodeBlockMeta(rawMeta)
            return SsTable(
                file = file,
                blockMeta = blockMeta,
                blockMetaOffset = blockMetaOffset.toInt(),
                id = id,
                blockCache = blockCache,
                firstKey = blockMeta.first().firstKey,
                lastKey = blockMeta.last().lastKey,
                maxTs = maxTs
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
    fun findBlockIdx(key: BlockKey): Int {
        return blockMeta.indexOfFirst { it.firstKey > key }
            .let { if (it == -1) blockMeta.size else it  }
            .minus(1)
            .coerceAtLeast(0)
    }

    /**
     * Get the number of blocks.
     */
    fun numberOfBlocks(): Int {
        return blockMeta.size
    }
}