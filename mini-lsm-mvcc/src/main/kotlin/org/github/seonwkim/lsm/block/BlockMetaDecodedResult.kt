package org.github.seonwkim.lsm.block

/**
 * A data class representing the result of decoding block metadata.
 *
 * @property blockMeta the list of block metadata
 * TODO(add support for maxTimestamp in block meta)
 */
data class BlockMetaDecodedResult(
    val blockMeta: List<BlockMeta>,
    val maxTimestamp: Long
)
