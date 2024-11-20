package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.TimestampedKey

data class BlockMeta(
    // offset of this data block
    val offset: Int,
    // first key of the data block
    val firstKey: TimestampedKey,
    // last key of the data block
    val lastKey: TimestampedKey
)

data class BlockMetaDecodedResult(
    val blockMeta: List<BlockMeta>,
    val maxTimestamp: Long
)
