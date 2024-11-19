package org.seonWKim.lsm.block

class BlockMeta(
    // offset of this data block
    val offset: Int,
    // first key of the data block
    val firstKey: BlockKey,
    // last key of the data block
    val lastKey: BlockKey
)
