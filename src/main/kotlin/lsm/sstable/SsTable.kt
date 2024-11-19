package org.seonWKim.lsm.sstable

import org.seonWKim.lsm.block.BlockKey
import org.seonWKim.lsm.block.BlockMeta

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

}
