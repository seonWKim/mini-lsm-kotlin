package org.seonWKim.lsm.sstable

import org.seonWKim.lsm.block.BlockKey
import org.seonWKim.lsm.block.BlockMeta

class SsTable(
    private val file: SsTableFile,
    private val blockMeta: List<BlockMeta>,
    private val blockMetaOffset: Int,
    private val id: Int,
    private val blockCache: BlockCache?,
    private val firstKey: BlockKey,
    private val lastKey: BlockKey,
    // private val bloom: Bloom?,
    private val maxTs: Long
) {

}
