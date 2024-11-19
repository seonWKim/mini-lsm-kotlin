package org.seonwkim.lsm.block

data class BlockMeta(
    // offset of this data block
    val offset: Int,
    // first key of the data block
    val firstKey: BlockKey,
    // last key of the data block
    val lastKey: BlockKey
)

data class BlockMetaDecodedResult(
    val blockMeta: List<BlockMeta>,
    val maxTimestamp: Long
)
