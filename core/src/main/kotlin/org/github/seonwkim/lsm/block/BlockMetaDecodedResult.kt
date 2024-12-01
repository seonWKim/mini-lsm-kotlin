package org.github.seonwkim.lsm.block

/**
 * A data class representing the result of decoding block metadata.
 *
 * @property blockMeta the list of block metadata
 * @property maxTimestamp the maximum timestamp found in the block metadata
 */
data class BlockMetaDecodedResult(
    val blockMeta: List<BlockMeta>,
    val maxTimestamp: Long
)
