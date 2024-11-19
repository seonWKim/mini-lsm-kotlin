package org.seonWKim.lsm.sstable

import org.seonWKim.common.toU32Int
import org.seonWKim.lsm.block.BlockKey
import org.seonWKim.lsm.block.BlockMeta
import org.seonWKim.lsm.block.BlockMetaUtil

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
}
